package worker

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func queuedMessages(t *testing.T, medium Medium, sizes []int, done *int) {
	t.Helper()
	n := 0
	for _, size := range sizes {
		values := make([]string, size)
		for i := range values {
			values[i] = strconv.Itoa(n)
			n++
		}
		msg := &Message{Value: values, Callback: func() { *done++ }}
		require.True(t, medium.Send(context.Background(), msg))
	}
}

func TestCoalescer(t *testing.T) {
	mediums := map[string]func() Medium{
		"channel":     func() Medium { return NewChannelMedium(nil, 16) },
		"queue":       func() Medium { return NewQueueMedium(nil, 16) },
		"accumulator": func() Medium { return NewAccumulatorMedium(nil) },
	}

	for name, newMedium := range mediums {
		t.Run(name+"/merges_queued_messages_up_to_max_and_releases_every_part", func(t *testing.T) {
			medium := newMedium()
			var done int
			queuedMessages(t, medium, []int{3, 3, 3, 3, 3}, &done)
			medium.Close()

			c := newCoalescer(medium, 7)
			var batches [][]string
			for {
				msg, ok := c.Recv(context.Background())
				if !ok {
					break
				}
				batches = append(batches, append([]string(nil), msg.Value...))
				msg.Done()
			}
			c.Release()

			// 3+3 fits in 7, the third message would overflow and leads the next batch.
			require.Equal(t, [][]string{
				{"0", "1", "2", "3", "4", "5"},
				{"6", "7", "8", "9", "10", "11"},
				{"12", "13", "14"},
			}, batches)
			require.Equal(t, 5, done)
		})

		t.Run(name+"/disabled_passes_messages_through", func(t *testing.T) {
			medium := newMedium()
			var done int
			queuedMessages(t, medium, []int{2, 2}, &done)
			medium.Close()

			c := newCoalescer(medium, 0)
			msg, ok := c.Recv(context.Background())
			require.True(t, ok)
			require.Equal(t, []string{"0", "1"}, msg.Value)
			msg.Done()
			c.Release()
			DrainSender(context.Background(), medium)
			require.Equal(t, 2, done)
		})

		t.Run(name+"/release_returns_a_held_message", func(t *testing.T) {
			medium := newMedium()
			var done int
			queuedMessages(t, medium, []int{4, 4}, &done)

			c := newCoalescer(medium, 5)
			msg, ok := c.Recv(context.Background())
			require.True(t, ok)
			require.Len(t, msg.Value, 4)
			msg.Done()

			// The second message was taken off the medium and held; abandoning the
			// coalescer must still release it.
			c.Release()
			require.Equal(t, 2, done)
			medium.Close()
		})
	}
}
