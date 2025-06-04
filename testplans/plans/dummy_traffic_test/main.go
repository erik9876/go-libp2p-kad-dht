package main

import (
	"context"
	"fmt"
	"time"

	dht "github.com/erik9876/go-libp2p-kad-dht"
	logging "github.com/ipfs/go-log"
	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
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
        "dummy_traffic_test": run.InitializedTestCaseFn(runDummyTrafficTest),
    })
}

// setupLogging configures the logging levels for different components
func setupLogging() error {
	logging.SetAllLoggers(logging.LevelInfo)
	components := map[string]string{
		"dht":              "debug",
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
func setupDHT(ctx context.Context, h host.Host, lowerBound int, upperBound int) (*dht.IpfsDHT, error) {
	return dht.New(ctx, h, 
		dht.Mode(dht.ModeServer), 
        dht.DummyOperationsBounds(time.Second * time.Duration(lowerBound), time.Second * time.Duration(upperBound)))
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

func runDummyTrafficTest(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
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
	_ = client.MustSignalEntry(ctx, sync.State("enrolled"))
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

	dummyOperationsLowerBound := runenv.IntParam("dummy_operations_lower_bound")
	dummyOperationsUpperBound := runenv.IntParam("dummy_operations_upper_bound")
	kadDHT, err := setupDHT(ctx, h, dummyOperationsLowerBound, dummyOperationsUpperBound)
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

	time.Sleep(60 * time.Second)

    barrier("done")
    runenv.RecordSuccess()

    return nil
}