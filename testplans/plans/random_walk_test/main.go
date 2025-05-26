package main

import (
	"context"
	"fmt"
	"time"

	// Core DHT and libp2p imports
	dht "github.com/erik9876/go-libp2p-kad-dht"
	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	// Logging and testground imports
	logging "github.com/ipfs/go-log"
	"github.com/multiformats/go-multibase"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
)

// Constants for test configuration
const (
	peerCollectionTimeout = 5 * time.Second
	connectionTimeout     = 5 * time.Second
	bootstrapTimeout      = 20 * time.Second
	barrierWaitTime       = 2 * time.Second
)

func main() {
	run.InvokeMap(map[string]interface{}{
		"random_walk_test": run.InitializedTestCaseFn(runRandomWalkTest),
	})
}

// setupLogging configures the logging levels for different components
func setupLogging() error {
	logging.SetAllLoggers(logging.LevelInfo)
	components := map[string]string{
		"dht":              "info",
		"dht/RtRefreshManager": "error",
		"basichost":        "error",
	}

	for component, level := range components {
		if err := logging.SetLogLevel(component, level); err != nil {
			return fmt.Errorf("failed to set log level for %s: %w", component, err)
		}
	}
	return nil
}

// setupHost creates and configures a new libp2p host
func setupHost() (host.Host, error) {
	return libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
}

// setupDHT initializes and configures the DHT with the given forwarding probability
func setupDHT(ctx context.Context, h host.Host, wantForwardingProb float64) (*dht.IpfsDHT, error) {
	return dht.New(ctx, h, 
		dht.Mode(dht.ModeServer), 
		dht.WantForwardingProbability(wantForwardingProb))
}

// collectPeers gathers peer information from the test network
func collectPeers(ctx context.Context, client sync.Client, runenv *runtime.RunEnv, h host.Host) ([]peer.AddrInfo, error) {
	topicPeers := sync.NewTopic("peers", &peer.AddrInfo{})
	me := peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}
	
	if _, err := client.Publish(ctx, topicPeers, &me); err != nil {
		return nil, fmt.Errorf("failed to publish peer info: %w", err)
	}

	peerCh := make(chan *peer.AddrInfo, runenv.TestInstanceCount)
	if _, err := client.Subscribe(ctx, topicPeers, peerCh); err != nil {
		return nil, fmt.Errorf("failed to subscribe to peers: %w", err)
	}

	var peers []peer.AddrInfo
	seen := map[peer.ID]bool{}
	
peerLoop:
	for len(peers) < runenv.TestInstanceCount-1 {
		select {
		case p := <-peerCh:
			if p.ID != h.ID() && !seen[p.ID] {
				peers = append(peers, *p)
				seen[p.ID] = true
				runenv.RecordMessage("Discovered peer %s", p.ID)
			}
		case <-time.After(peerCollectionTimeout):
			runenv.RecordMessage("Timeout collecting peers")
			break peerLoop
		}
	}
	return peers, nil
}

// connectToPeers establishes connections with all discovered peers
func connectToPeers(ctx context.Context, h host.Host, peers []peer.AddrInfo, runenv *runtime.RunEnv) {
	for _, p := range peers {
		ctxc, cancelc := context.WithTimeout(ctx, connectionTimeout)
		if err := h.Connect(ctxc, p); err != nil {
			runenv.RecordMessage("Connect to %s failed: %s", p.ID, err)
		}
		cancelc()
	}
}

// waitForRoutingTable waits for the routing table to be populated
func waitForRoutingTable(kadDHT *dht.IpfsDHT, runenv *runtime.RunEnv) {
	deadline := time.Now().Add(bootstrapTimeout)
	for time.Now().Before(deadline) {
		sz := kadDHT.RoutingTable().Size()
		runenv.RecordMessage("Routing table size: %d", sz)
		if sz >= runenv.TestInstanceCount-1 {
			break
		}
		time.Sleep(time.Second)
	}
	runenv.RecordMessage("Final routing table size: %d", kadDHT.RoutingTable().Size())
}

// handlePutOperation handles the DHT put operation for sequence 1
func handlePutOperation(ctx context.Context, client sync.Client, kadDHT *dht.IpfsDHT, h host.Host, runenv *runtime.RunEnv) error {
	peerTopic := sync.NewTopic("peer-info", &struct{ ID peer.ID }{})

	pubKey := h.Peerstore().PubKey(h.ID())
	if pubKey == nil {
		return fmt.Errorf("no public key in peerstore")
	}

	id, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return fmt.Errorf("peer ID from public key failed: %w", err)
	}

	if _, err := client.Publish(ctx, peerTopic, &struct{ ID peer.ID }{ID: id}); err != nil {
		return fmt.Errorf("publish peer-info failed: %w", err)
	}

	dhtKey := "/pk/" + string(id)
	rawPub, err := crypto.MarshalPublicKey(pubKey)
	if err != nil {
		return fmt.Errorf("marshal public key failed: %w", err)
	}

	if err := kadDHT.PutValue(ctx, dhtKey, rawPub); err != nil {
		runenv.RecordMessage("PutValue failed: %s", err)
	} else {
		rawKey, err := pubKey.Raw()
		if err != nil {
			runenv.RecordMessage("failed to get raw public key: %s", err)
		} else {
            keyBase32, err := multibase.Encode(multibase.Base32, rawKey)
            if err != nil {
                runenv.RecordMessage("failed to encode public key: %s", err)
            } else {
			    runenv.RecordMessage("PutValue successful - Stored public key for peer %s, %s", id, keyBase32)
            }
		}
	}
	return nil
}

// handleGetOperation handles the DHT get operation for sequence 2
func handleGetOperation(ctx context.Context, client sync.Client, kadDHT *dht.IpfsDHT, runenv *runtime.RunEnv) error {
	peerTopic := sync.NewTopic("peer-info", &struct{ ID peer.ID }{})
	
	peerCh := make(chan *struct{ ID peer.ID }, 1)
	if _, err := client.Subscribe(ctx, peerTopic, peerCh); err != nil {
		return fmt.Errorf("subscribe peer-info failed: %w", err)
	}

	peerInfo := <-peerCh
	targetKey := "/pk/" + string(peerInfo.ID)

	// First round: Direct GetValue
	getCtx, getCancel := context.WithCancel(ctx)
	startGet := time.Now()
	val, err := kadDHT.GetValue(getCtx, targetKey)
	getDuration := time.Since(startGet)
	getCancel()
	
	if err != nil {
		runenv.RecordMessage("GetValue failed: %s", err)
	} else {
		runenv.RecordMessage("GetValue successful in %v, raw value length: %d", getDuration, len(val))
	}

	// Second round: WantValueFromPeers
	wantCtx, wantCancel := context.WithCancel(ctx)
	defer wantCancel()
	
	startWant := time.Now()
	val, err = kadDHT.WantValueFromPeers(wantCtx, targetKey, 1)
	wantDuration := time.Since(startWant)
	if err != nil {
		runenv.RecordFailure(fmt.Errorf("lookup failed: %s", err))
		return nil
	}
	runenv.RecordMessage("WantValueFromPeers completed in %v", wantDuration)

	pubKey, err := crypto.UnmarshalPublicKey(val)
	if err != nil {
		runenv.RecordMessage("unmarshal public key failed: %s", err)
	} else {
		// Convert public key to a more readable format
		pubKeyBytes, err := pubKey.Raw()
		if err != nil {
			runenv.RecordMessage("failed to get raw public key: %s", err)
		} else {
            encodedKey, err := multibase.Encode(multibase.Base32, pubKeyBytes)
            if err != nil {
                runenv.RecordMessage("failed to encode public key: %s", err)
            } else {
                runenv.RecordMessage("got pubkey (base32): %s", encodedKey)
            }
		}
	}
	return nil
}

func runRandomWalkTest(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
	// Setup logging
	if err := setupLogging(); err != nil {
		return err
	}

	// Initialize context and sync client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := sync.MustBoundClient(ctx, runenv)
	defer client.Close()

	// Setup barrier synchronization
	seq := client.MustSignalEntry(ctx, sync.State("enrolled"))
	initCtx.MustWaitAllInstancesInitialized(ctx)
	barrier := func(state string) {
		if _, err := client.SignalAndWait(ctx, sync.State(state), runenv.TestInstanceCount); err != nil {
			runenv.RecordMessage("error waiting for %s: %s", state, err)
		}
	}

	// Initialize host and DHT
	h, err := setupHost()
	if err != nil {
		return fmt.Errorf("failed to setup host: %w", err)
	}
	defer h.Close()

	barrier("initialized")

	wantForwardingProb := runenv.FloatParam("want_forwarding_probability")
	kadDHT, err := setupDHT(ctx, h, wantForwardingProb)
	if err != nil {
		return fmt.Errorf("failed to setup DHT: %w", err)
	}
	defer kadDHT.Close()

	// Collect peers and establish connections
	peers, err := collectPeers(ctx, client, runenv, h)
	if err != nil {
		return err
	}

	connectToPeers(ctx, h, peers, runenv)
	if err := kadDHT.Bootstrap(ctx); err != nil {
		runenv.RecordMessage("Bootstrap failed: %s", err)
	}

	waitForRoutingTable(kadDHT, runenv)
	barrier("routing_table_ready")

	// Execute test operations based on sequence
	time.Sleep(barrierWaitTime)
	if seq == 1 {
		if err := handlePutOperation(ctx, client, kadDHT, h, runenv); err != nil {
			return err
		}
	}
	barrier("put_value")

	time.Sleep(barrierWaitTime)
	if seq == 2 {
		if err := handleGetOperation(ctx, client, kadDHT, runenv); err != nil {
			return err
		}
	}
	barrier("done")

	runenv.RecordSuccess()
	return nil
}