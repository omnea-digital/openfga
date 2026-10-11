package mpmc

// TryRecv dequeues the next item if one is immediately available. It never
// blocks: it returns the zero value and false when the buffer is empty,
// including after [Queue.Close] once the buffer has been drained.
func (p *Queue[T]) TryRecv() (T, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	pos := p.tail.Load()

	for {
		cell := &p.data[p.mask(pos)]
		seq := cell.Sequence.Load()
		diff := seq - (pos + 1)

		switch {
		case diff == 0:
			if !p.tail.CompareAndSwap(pos, pos+1) {
				pos = p.tail.Load()
				continue
			}
			value := cell.Data
			var zero T
			cell.Data = zero
			cell.Sequence.Store(pos + int64(p.capacity))

			// Wake one parked sender, if any. Guarded as in Recv: the full
			// channel is closed once the queue is done.
			if !p.done.Load() {
				select {
				case p.full <- struct{}{}:
				default:
				}
			}
			return value, true
		case diff < 0:
			var zero T
			return zero, false
		default:
			pos = p.tail.Load()
		}
	}
}
