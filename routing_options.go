package dht

import (
	internalConfig "github.com/erik9876/go-libp2p-kad-dht/internal/config"
	"github.com/libp2p/go-libp2p/core/routing"
)

// Quorum is a DHT option that tells the DHT how many peers it needs to get
// values from before returning the best one. Zero means the DHT query
// should complete instead of returning early.
//
// Default: 0
func Quorum(n int) routing.Option {
	return func(opts *routing.Options) error {
		if opts.Other == nil {
			opts.Other = make(map[interface{}]interface{}, 1)
		}
		opts.Other[internalConfig.QuorumOptionKey{}] = n
		return nil
	}
}
