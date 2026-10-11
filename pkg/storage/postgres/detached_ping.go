package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfga/openfga/pkg/server/config"
)

// pgxDefaultPingIdleThreshold is pgxpool's default ShouldPing rule: ping a connection that has been
// idle for more than one second.
const pgxDefaultPingIdleThreshold = time.Second

// useDetachedPing stops a cancelled caller from destroying a healthy pooled connection.
//
// pgxpool.Pool.Acquire pings a connection idle for more than a second under the caller's context
// and destroys the connection on any ping error. A caller cancelled before or during that ping
// (a union sibling after a short-circuit, a cache hit, a client timeout) therefore makes the pool
// close a healthy connection and dial a new one; a cancel during the ping also breaks the socket,
// because pgx's context watcher sets the socket deadline to now.
//
// With this option the ping runs under a context the caller cannot cancel, bounded by the pool's
// PingTimeout, so only a connection that fails a ping on its own is destroyed (pgx pings it once
// more under the caller's context and destroys it, as upstream does). If the caller has gone by
// the time the connection is ready, PrepareConn hands the connection back to the pool and Acquire
// returns the caller's context error, the same error the caller would get without this option.
func useDetachedPing(c *pgxpool.Config) {
	pingTimeout := c.PingTimeout
	if pingTimeout <= 0 {
		pingTimeout = config.DefaultDatastorePingTimeout
	}

	c.ShouldPing = func(ctx context.Context, params pgxpool.ShouldPingParams) bool {
		if params.IdleDuration <= pgxDefaultPingIdleThreshold {
			return false
		}
		pingCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pingTimeout)
		defer cancel()
		return params.Conn.Ping(pingCtx) != nil
	}

	c.PrepareConn = func(ctx context.Context, _ *pgx.Conn) (bool, error) {
		return true, ctx.Err()
	}
}
