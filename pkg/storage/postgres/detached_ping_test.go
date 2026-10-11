package postgres

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
)

// startDelayProxy forwards TCP to target and holds every chunk for delay in each direction,
// so a ping takes long enough for a cancel to land in the middle of it.
func startDelayProxy(t *testing.T, target string, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	pump := func(dst, src net.Conn) {
		defer dst.Close()
		defer src.Close()
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				time.Sleep(delay)
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}

	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			go pump(server, client)
			go pump(client, server)
		}
	}()
	return ln.Addr().String()
}

func newSingleConnPool(t *testing.T, uri string, detached bool) *pgxpool.Pool {
	t.Helper()
	cfg := sqlcommon.NewConfig(sqlcommon.WithMaxOpenConns(1), sqlcommon.WithDetachedPing(detached))
	poolCfg, err := parseConfig(uri, false, cfg)
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	conn.Release()
	return pool
}

func TestDetachedPing(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")
	directURI := testDatastore.GetConnectionURI(true)

	const hop = 5 * time.Millisecond
	parsed, err := url.Parse(directURI)
	require.NoError(t, err)
	parsed.Host = startDelayProxy(t, parsed.Host, hop)
	delayedURI := parsed.String()

	// Each round leaves the connection idle long enough to be pinged, then cancels the caller
	// while the ping is on the wire.
	cancelDuringPing := func(t *testing.T, pool *pgxpool.Pool) (newConns int64, slowestCancelledAcquire time.Duration) {
		before := pool.Stat().NewConnsCount()
		for range 3 {
			time.Sleep(pgxDefaultPingIdleThreshold + 100*time.Millisecond)
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(hop/2, cancel)
			start := time.Now()
			_, err := pool.Acquire(ctx)
			slowestCancelledAcquire = max(slowestCancelledAcquire, time.Since(start))
			require.ErrorIs(t, err, context.Canceled)
			cancel()

			conn, err := pool.Acquire(context.Background())
			require.NoError(t, err)
			_, err = conn.Exec(context.Background(), "select 1")
			require.NoError(t, err)
			conn.Release()
		}
		return pool.Stat().NewConnsCount() - before, slowestCancelledAcquire
	}

	t.Run("upstream_behaviour_destroys_the_connection_of_a_caller_cancelled_during_the_ping", func(t *testing.T) {
		newConns, _ := cancelDuringPing(t, newSingleConnPool(t, delayedURI, false))
		require.Equal(t, int64(3), newConns)
	})

	t.Run("detached_ping_keeps_the_connection_and_still_returns_the_cancellation_promptly", func(t *testing.T) {
		newConns, slowest := cancelDuringPing(t, newSingleConnPool(t, delayedURI, true))
		require.Zero(t, newConns)
		// The caller waits for at most the ping round trip (two hops each way here), never PingTimeout.
		require.Less(t, slowest, 500*time.Millisecond)
	})

	t.Run("detached_ping_still_replaces_a_connection_the_server_closed", func(t *testing.T) {
		pool := newSingleConnPool(t, delayedURI, true)

		var pid uint32
		conn, err := pool.Acquire(context.Background())
		require.NoError(t, err)
		require.NoError(t, conn.QueryRow(context.Background(), "select pg_backend_pid()").Scan(&pid))
		conn.Release()

		admin, err := pgx.Connect(context.Background(), directURI)
		require.NoError(t, err)
		defer admin.Close(context.Background())
		var terminated bool
		require.NoError(t, admin.QueryRow(context.Background(), "select pg_terminate_backend($1)", pid).Scan(&terminated))
		require.True(t, terminated)

		before := pool.Stat().NewConnsCount()
		time.Sleep(pgxDefaultPingIdleThreshold + 100*time.Millisecond)

		conn, err = pool.Acquire(context.Background())
		require.NoError(t, err)
		defer conn.Release()
		var newPid uint32
		require.NoError(t, conn.QueryRow(context.Background(), "select pg_backend_pid()").Scan(&newPid))
		require.NotEqual(t, pid, newPid)
		require.Equal(t, int64(1), pool.Stat().NewConnsCount()-before)
	})
}
