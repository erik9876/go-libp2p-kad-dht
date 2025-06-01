package main

import (
	"context"
	"fmt"
	"time"

	dht "github.com/erik9876/go-libp2p-kad-dht"
	logging "github.com/ipfs/go-log"
	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
)

func main() {
    run.InvokeMap(map[string]interface{}{
        "dummy_traffic_test": run.InitializedTestCaseFn(runDummyTrafficTest),
    })
}

func runDummyTrafficTest(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
    logging.SetAllLoggers(logging.LevelDebug)
    if err := logging.SetLogLevel("dht", "debug"); err != nil {
        fmt.Println("Logging error:", err)
    }
    if err := logging.SetLogLevel("dht/RtRefreshManager", "error"); err != nil {
        fmt.Println("Logging error:", err)
    }
    if err := logging.SetLogLevel("rtrefresh", "error"); err != nil {
        fmt.Println("Logging error:", err)
    }

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    client := sync.MustBoundClient(ctx, runenv)
    defer client.Close()

    _ = client.MustSignalEntry(ctx, sync.State("enrolled"))
    initCtx.MustWaitAllInstancesInitialized(ctx)
    barrier := func(state string) {
        if _, err := client.SignalAndWait(ctx, sync.State(state), runenv.TestInstanceCount); err != nil {
            runenv.RecordMessage("error waiting for %s: %s", state, err)
        }
    }

    h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
    if err != nil {
        return fmt.Errorf("libp2p.New failed: %w", err)
    }
    defer h.Close()

    barrier("initialized")

    kadDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
    if err != nil {
        return fmt.Errorf("creating DHT failed: %w", err)
    }
    defer kadDHT.Close()

    topicPeers := sync.NewTopic("peers", &peer.AddrInfo{})
    me := peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}
    if _, err := client.Publish(ctx, topicPeers, &me); err != nil {
        return fmt.Errorf("publish peers failed: %w", err)
    }
    runenv.RecordMessage("I am peer %s", h.ID())
    barrier("published")

    peerCh := make(chan *peer.AddrInfo, runenv.TestInstanceCount)
    if _, err := client.Subscribe(ctx, topicPeers, peerCh); err != nil {
        return fmt.Errorf("subscribe peers failed: %w", err)
    }
    var peers []peer.AddrInfo
    seen := map[peer.ID]bool{}
    for len(peers) < runenv.TestInstanceCount-1 {
        select {
        case p := <-peerCh:
            if p.ID != h.ID() && !seen[p.ID] {
                peers = append(peers, *p)
                seen[p.ID] = true
                runenv.RecordMessage("Discovered peer %s", p.ID)
            }
        case <-time.After(5 * time.Second):
            runenv.RecordMessage("Timeout collecting peers")
            break
        }
    }

    for _, p := range peers {
        ctxc, cancelc := context.WithTimeout(ctx, 5*time.Second)
        if err := h.Connect(ctxc, p); err != nil {
            runenv.RecordMessage("Connect to %s failed: %s", p.ID, err)
        }
        cancelc()
    }
    if err := kadDHT.Bootstrap(ctx); err != nil {
        runenv.RecordMessage("Bootstrap failed: %s", err)
    } else {
        runenv.RecordMessage("DHT bootstrap successful")
    }

    deadline := time.Now().Add(20 * time.Second)
    for time.Now().Before(deadline) {
        sz := kadDHT.RoutingTable().Size()
        runenv.RecordMessage("Routing table size: %d", sz)
        if sz >= runenv.TestInstanceCount-1 {
            break
        }
        time.Sleep(1 * time.Second)
    }
    runenv.RecordMessage("Final routing table size: %d", kadDHT.RoutingTable().Size())

	barrier("routing_table_ready")

	time.Sleep(30 * time.Second)

    barrier("done")
    runenv.RecordSuccess()

    return nil
}