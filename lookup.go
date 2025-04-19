package dht

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/erik9876/go-libp2p-kad-dht/internal"
	"github.com/erik9876/go-libp2p-kad-dht/metrics"
	"github.com/erik9876/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/trace"
)

// GetClosestPeers is a Kademlia 'node lookup' operation. Returns a slice of
// the K closest peers to the given key.
//
// If the context is canceled, this function will return the context error
// along with the closest K peers it has found so far.
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	if key == "" {
		return nil, fmt.Errorf("can't lookup empty key")
	}

	// TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key, dht.pmGetClosestPeers(key), func(*qpeerset.QueryPeerset) bool { return false })
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil || !lookupRes.completed {
		return lookupRes.peers, err
	}

	// tracking lookup results for network size estimator
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		logger.Warnf("network size estimator track peers: %s", err)
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.NetworkSize.M(int64(ns))
	}

	// Reset the refresh timer for this key's bucket since we've just
	// successfully interacted with the closest peers to key
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	return lookupRes.peers, nil
}

// pmGetClosestPeers is the protocol messenger version of the GetClosestPeer queryFn.
func (dht *IpfsDHT) pmGetClosestPeers(key string) queryFn {
	return func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, peer.ID(key))
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:  routing.QueryError,
				ID:    p,
				Extra: err.Error(),
			})
			return nil, err
		}

		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return peers, err
	}
}

// source routed -> old idea
// RandomWalk performs a random walk in the DHT and returns the final node
func (dht *IpfsDHT) RandomWalk(ctx context.Context, length int) ([]peer.ID, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.RandomWalk")
	defer span.End()

    // Start with a random peer from the routing table
    peers := dht.routingTable.ListPeers()
    if len(peers) == 0 {
        return nil, fmt.Errorf("no peers in routing table")
    }

    randIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(peers))))
    if err != nil {
        return nil, err
    }

    currentPeer := peers[randIndex.Int64()]
	
    path := make([]peer.ID, length)
	path = append(path, currentPeer)

	// Perform the walk for the specified number of hops
	for i := 0; i < length; i++ {
		nextPeer, err := dht.randomHop(ctx, currentPeer, path)
		for err != nil {
			randIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(peers))))
			if err != nil {
				continue
			}

			altPeer := peers[randIndex.Int64()]
			nextPeer, err = dht.randomHop(ctx, altPeer, path)
			if err == nil {
				break
			}
		}
        currentPeer = nextPeer
		path = append(path, currentPeer)
    }

    return path, nil
}

// randomHop asks the given peer for its routing table entries and randomly selects one of them
func (dht *IpfsDHT) randomHop(ctx context.Context, targetPeer peer.ID, path []peer.ID) (peer.ID, error) {
	// Create a random key to query the peer's routing table
	randomKey := make([]byte, 20)
	if _, err := rand.Read(randomKey); err != nil {
		return "", fmt.Errorf("failed to generate random key: %w", err)
	}

	// Ask the target peer for peers close to the random key
	closestPeers, err := dht.protoMessenger.GetClosestPeers(ctx, targetPeer, peer.ID(randomKey))
	if err != nil {
		return "", fmt.Errorf("failed to get closest peers from target: %w", err)
	}

	// Filter out self and target peer from the results
	filteredPeers := make([]peer.ID, 0, len(closestPeers))
	for _, p := range closestPeers {
		if p.ID != dht.self && p.ID != targetPeer {
			filteredPeers = append(filteredPeers, p.ID)
		}
	}

	if len(filteredPeers) == 0 {
		return "", fmt.Errorf("no valid peers found in target's routing table")
	}

	// Randomly select a peer from the filtered list
	randIndex, err := rand.Int(rand.Reader, big.NewInt(int64(len(filteredPeers))))
	if err != nil {
		return "", fmt.Errorf("failed to generate random index: %w", err)
	}
	return filteredPeers[randIndex.Int64()], nil
}
