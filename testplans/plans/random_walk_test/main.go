package main

import (
	"context"
	"fmt"
	"time"

	dht "github.com/erik9876/go-libp2p-kad-dht"
	"github.com/erik9876/go-libp2p-kad-dht/internal"
	logging "github.com/ipfs/go-log"
	libp2p "github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/testground/sdk-go/run"
	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
)

func main() {
    run.InvokeMap(map[string]interface{}{
        "random_walk_test": run.InitializedTestCaseFn(runRandomWalkTest),
    })
}

func runRandomWalkTest(runenv *runtime.RunEnv, initCtx *run.InitContext) error {
    logging.SetAllLoggers(logging.LevelInfo)
    if err := logging.SetLogLevel("dht", "info"); err != nil {
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

    seq := client.MustSignalEntry(ctx, sync.State("enrolled"))
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

    // Add a topic to share the peer ID as well for verification
    type peerInfo struct{ ID peer.ID }
    peerTopic := sync.NewTopic("peer-info", &peerInfo{})

    if seq == 1 {
        runenv.RecordMessage("PUTTER - Starting put operation")

        pubKey := h.Peerstore().PubKey(h.ID())
        if pubKey == nil {
            return fmt.Errorf("no public key in peerstore")
        }

        id, err := peer.IDFromPublicKey(pubKey)
        if err != nil {
            return fmt.Errorf("peer ID from public key failed: %w", err)
        }

        // Publish our peer ID first so others can verify
        if _, err := client.Publish(ctx, peerTopic, &peerInfo{ID: id}); err != nil {
            return fmt.Errorf("publish peer-info failed: %w", err)
        }
        runenv.RecordMessage("PUTTER - Published our peer ID: %s", id)

        dhtKey := "/pk/" + string(id)
        runenv.RecordMessage("PUTTER - Using DHT key: %s", internal.LoggableRecordKeyString(dhtKey))
        runenv.RecordMessage("PUTTER - Peer ID: %s", id)

        rawPub, err := crypto.MarshalPublicKey(pubKey)
        if err != nil {
            return fmt.Errorf("marshal public key failed: %w", err)
        }
        runenv.RecordMessage("PUTTER - Marshaled public key length: %d bytes", len(rawPub))

        if err := kadDHT.PutValue(ctx, dhtKey, rawPub); err != nil {
            runenv.RecordMessage("PutValue failed: %s", err)
        } else {
            runenv.RecordMessage("PutValue successful - Stored public key for peer %s", id)
        }

        // Verify we can read back what we just put
        testVal, err := kadDHT.GetValue(ctx, dhtKey)
        if err != nil {
            runenv.RecordMessage("WARNING: Could not verify put by reading back value: %s", err)
        } else {
            runenv.RecordMessage("PUTTER - Successfully verified we can read back the value we just put")
            if len(testVal) != len(rawPub) {
                runenv.RecordMessage("WARNING: Retrieved value length (%d) differs from what we put (%d)", len(testVal), len(rawPub))
            }
        }
    }
    time.Sleep(2 * time.Second)
    barrier("put_value")

    if seq == 3 {
        runenv.RecordMessage("GETTER - Starting get operation")

        // First, get the peer ID that we should be looking for
        peerCh := make(chan *peerInfo, 1)
        if _, err := client.Subscribe(ctx, peerTopic, peerCh); err != nil {
            return fmt.Errorf("subscribe peer-info failed: %w", err)
        }
        peerInfo := <-peerCh
        runenv.RecordMessage("GETTER - Received peer ID to look for: %s", peerInfo.ID)

        targetKey := "/pk/" + string(peerInfo.ID)
        runenv.RecordMessage("GETTER - Target key: %s", internal.LoggableRecordKeyString(targetKey))

		getCtx, getCancel := context.WithCancel(context.Background())
        val, err := kadDHT.GetValue(getCtx, targetKey)
        if err != nil {
            runenv.RecordMessage("GetValue failed: %s", err)
        } else {
            runenv.RecordMessage("GetValue successful, raw value length: %d", len(val))
        }
        getCancel()
    }
    barrier("get_value")

    if seq == 2 {
        runenv.RecordMessage("starting lookup")

         // First, get the peer ID that we should be looking for
        peerCh := make(chan *peerInfo, 1)
        if _, err := client.Subscribe(ctx, peerTopic, peerCh); err != nil {
            return fmt.Errorf("subscribe peer-info failed: %w", err)
        }
        peerInfo := <-peerCh
        runenv.RecordMessage("GETTER - Received peer ID to look for: %s", peerInfo.ID)

        targetKey := "/pk/" + string(peerInfo.ID)

		wantCtx, wantCancel := context.WithCancel(context.Background())
		defer wantCancel()
        val, err := kadDHT.WantValueFromPeers(wantCtx, targetKey, 1)
        if err != nil {
            runenv.RecordMessage("lookup failed: %s", err)
        } else {
            pk2, err := crypto.UnmarshalPublicKey(val)
            if err != nil {
                runenv.RecordMessage("unmarshal public key failed: %s", err)
            } else {
                runenv.RecordMessage("got pubkey: %s", pk2)
            }
        }
    }

    barrier("done")
    runenv.RecordSuccess()
    return nil
}