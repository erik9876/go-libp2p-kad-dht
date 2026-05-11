package dht

import (
	"context"
	"sync/atomic"
)

type lookupOriginKey struct{}

// WithLookupOrigin returns a derived context that carries the given origin tag.
// DHT lookups performed with this context (or a descendant) will be tagged with
// the origin string in their LookupEvent.Origin field.
func WithLookupOrigin(ctx context.Context, origin string) context.Context {
	return context.WithValue(ctx, lookupOriginKey{}, origin)
}

// lookupOriginFrom returns the origin tag previously set via WithLookupOrigin,
// or "" if none was set.
func lookupOriginFrom(ctx context.Context) string {
	if v, ok := ctx.Value(lookupOriginKey{}).(string); ok {
		return v
	}
	return ""
}

// GlobalLookupHook receives every LookupEvent published via PublishLookupEvent,
// regardless of whether the calling context was registered via
// RegisterForLookupEvents. The hook runs on the DHT goroutine and MUST NOT
// block — implementors should fan out onto a buffered channel.
type GlobalLookupHook func(*LookupEvent)

var globalLookupHook atomic.Pointer[GlobalLookupHook]

// SetGlobalLookupHook installs a process-wide hook called for every published
// LookupEvent. Passing nil clears the hook. Safe for concurrent use.
func SetGlobalLookupHook(h GlobalLookupHook) {
	if h == nil {
		globalLookupHook.Store(nil)
		return
	}
	globalLookupHook.Store(&h)
}

// ClearGlobalLookupHook removes any previously installed global hook.
func ClearGlobalLookupHook() {
	globalLookupHook.Store(nil)
}
