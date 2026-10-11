package mpmc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueue_TryRecv(t *testing.T) {
	q := MustQueue[int](2, -1)

	_, ok := q.TryRecv()
	require.False(t, ok, "empty queue")

	for i := range 5 {
		require.True(t, q.Send(context.Background(), i))
	}

	for i := range 3 {
		v, ok := q.TryRecv()
		require.True(t, ok)
		require.Equal(t, i, v)
	}

	q.Close()

	// Items queued before Close are still delivered, then the queue reports empty.
	for i := 3; i < 5; i++ {
		v, ok := q.TryRecv()
		require.True(t, ok)
		require.Equal(t, i, v)
	}
	_, ok = q.TryRecv()
	require.False(t, ok)
}
