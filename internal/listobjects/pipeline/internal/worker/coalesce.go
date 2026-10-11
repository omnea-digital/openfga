package worker

import "context"

// trySender is implemented by mediums that support a non-blocking receive.
type trySender interface {
	TryRecv() (*Message, bool)
}

// TryRecv returns a queued message without blocking. It returns false when
// no message is queued or the channel is closed.
func (m *ChannelMedium) TryRecv() (*Message, bool) {
	select {
	case value, ok := <-m.ch:
		return value, ok && value != nil
	default:
		return nil, false
	}
}

// TryRecv returns a queued message without blocking.
func (m *QueueMedium) TryRecv() (*Message, bool) {
	if m.closed {
		return nil, false
	}
	return m.queue.TryRecv()
}

// TryRecv returns a queued message without blocking.
func (m *AccumulatorMedium) TryRecv() (*Message, bool) {
	if m.closed {
		return nil, false
	}
	return m.acc.TryRecv()
}

// coalescer receives messages from one sender and, when more messages are
// already queued behind the first, merges them into a single message of at
// most max values. It never waits for a message that has not arrived, so it
// adds no latency: a quiet edge behaves exactly as without coalescing, and
// only a backlog is merged.
//
// A merged message carries the union of its parts' values. Its Done releases
// every part, so per-message callbacks (pool return, cycle in-flight
// accounting) still run exactly once and only after the merged message has
// been processed. Interpreting a union of ids in one read returns the union
// of the per-part reads, so results are unchanged.
type coalescer struct {
	sender  Sender
	try     trySender
	max     int
	pending *Message
}

func newCoalescer(sender Sender, maxBatch int) *coalescer {
	c := coalescer{sender: sender, max: maxBatch}
	if maxBatch > 0 {
		c.try, _ = sender.(trySender)
	}
	return &c
}

// Recv returns the next (possibly merged) message.
func (c *coalescer) Recv(ctx context.Context) (*Message, bool) {
	msg := c.pending
	c.pending = nil
	if msg == nil {
		var ok bool
		msg, ok = c.sender.Recv(ctx)
		if !ok {
			return nil, false
		}
	}

	if c.try == nil || len(msg.Value) >= c.max {
		return msg, true
	}

	size := len(msg.Value)
	var parts []*Message
	for size < c.max {
		next, ok := c.try.TryRecv()
		if !ok {
			break
		}
		if size+len(next.Value) > c.max {
			// Keep the message whole; it leads the next batch.
			c.pending = next
			break
		}
		parts = append(parts, next)
		size += len(next.Value)
	}

	if len(parts) == 0 {
		return msg, true
	}

	parts = append(parts, msg)
	values := make([]string, 0, size)
	values = append(values, msg.Value...)
	for _, part := range parts[:len(parts)-1] {
		values = append(values, part.Value...)
	}

	return &Message{
		Value: values,
		Callback: func() {
			for _, part := range parts {
				part.Done()
			}
		},
	}, true
}

// Release returns a message held for the next batch. It must be called once
// the coalescer is no longer used.
func (c *coalescer) Release() {
	if c.pending != nil {
		c.pending.Done()
		c.pending = nil
	}
}
