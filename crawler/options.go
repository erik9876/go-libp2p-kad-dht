package crawler

import (
	"time"

	"github.com/erik9876/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// Option DHT Crawler option type.
type Option func(*options) error

type options struct {
	protocols            []protocol.ID
	parallelism          int
	connectTimeout       time.Duration
	perMsgTimeout        time.Duration
	dialAddressExtendDur time.Duration
}

// defaults are the default crawler options. This option will be automatically
// prepended to any options you pass to the crawler constructor.
var defaults = func(o *options) error {
	o.protocols = amino.Protocols
	o.parallelism = 1000
	o.connectTimeout = time.Second * 5
	o.perMsgTimeout = time.Second * 5
	o.dialAddressExtendDur = time.Minute * 30

	return nil
}

// WithProtocols defines the ordered set of protocols the crawler will use to talk to other nodes
func WithProtocols(protocols []protocol.ID) Option {
	return func(o *options) error {
		o.protocols = append([]protocol.ID{}, protocols...)
		return nil
	}
}

// WithParallelism defines the number of queries that can be issued in parallel
func WithParallelism(parallelism int) Option {
	return func(o *options) error {
		o.parallelism = parallelism
		return nil
	}
}

// WithMsgTimeout defines the amount of time a single DHT message is allowed to take before it's deemed failed
func WithMsgTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		o.perMsgTimeout = timeout
		return nil
	}
}

// WithConnectTimeout defines the time for peer connection before timing out
func WithConnectTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		o.connectTimeout = timeout
		return nil
	}
}

// WithDialAddrExtendDuration sets the duration by which the TTL of dialed address in peer store are
// extended.
// Defaults to 30 minutes if unset.
func WithDialAddrExtendDuration(ext time.Duration) Option {
	return func(o *options) error {
		o.dialAddressExtendDur = ext
		return nil
	}
}
