package dual

import (
	"context"
	"testing"
	"time"

	dht "github.com/erik9876/go-libp2p-kad-dht"
	"github.com/erik9876/go-libp2p-kad-dht/internal"
	test "github.com/erik9876/go-libp2p-kad-dht/internal/testing"
	"github.com/ipfs/go-cid"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	peerstore "github.com/libp2p/go-libp2p/core/peerstore"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

var wancid, lancid cid.Cid

func init() {
	wancid = cid.NewCidV1(cid.DagCBOR, internal.Hash([]byte("wan cid -- value")))
	lancid = cid.NewCidV1(cid.DagCBOR, internal.Hash([]byte("lan cid -- value")))
}

type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

type customRtHelper struct {
	allow peer.ID
}

func MkFilterForPeer() (func(_ interface{}, p peer.ID) bool, *customRtHelper) {
	helper := customRtHelper{}

	type hasHost interface {
		Host() host.Host
	}

	f := func(dht interface{}, p peer.ID) bool {
		d := dht.(hasHost)
		conns := d.Host().Network().ConnsToPeer(p)

		for _, c := range conns {
			if c.RemotePeer() == helper.allow {
				return true
			}
		}
		return false
	}
	return f, &helper
}

func setupDHTWithFilters(ctx context.Context, t *testing.T, options ...dht.Option) (*DHT, []*customRtHelper) {
	h, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	h.Start()
	t.Cleanup(func() { h.Close() })

	wanFilter, wanRef := MkFilterForPeer()
	wanOpts := []dht.Option{
		dht.NamespacedValidator("v", blankValidator{}),
		dht.ProtocolPrefix("/test"),
		dht.DisableAutoRefresh(),
		dht.RoutingTableFilter(wanFilter),
	}
	wan, err := dht.New(ctx, h, wanOpts...)
	require.NoError(t, err)

	lanFilter, lanRef := MkFilterForPeer()
	lanOpts := []dht.Option{
		dht.NamespacedValidator("v", blankValidator{}),
		dht.ProtocolPrefix("/test"),
		dht.ProtocolExtension(LanExtension),
		dht.DisableAutoRefresh(),
		dht.RoutingTableFilter(lanFilter),
		dht.Mode(dht.ModeServer),
	}
	lan, err := dht.New(ctx, h, lanOpts...)
	require.NoError(t, err)

	impl := DHT{wan, lan}
	return &impl, []*customRtHelper{wanRef, lanRef}
}

func setupDHT(ctx context.Context, t *testing.T, options ...dht.Option) *DHT {
	t.Helper()

	host, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	host.Start()
	t.Cleanup(func() { host.Close() })

	baseOpts := []dht.Option{
		dht.NamespacedValidator("v", blankValidator{}),
		dht.ProtocolPrefix("/test"),
		dht.DisableAutoRefresh(),
	}

	d, err := New(
		ctx,
		host,
		append([]Option{DHTOption(baseOpts...)}, DHTOption(options...))...,
	)
	require.NoError(t, err)

	return d
}

func connect(ctx context.Context, t *testing.T, a, b *dht.IpfsDHT) {
	t.Helper()
	bid := b.PeerID()
	baddr := b.Host().Peerstore().Addrs(bid)
	if len(baddr) == 0 {
		t.Fatal("no addresses for connection.")
	}
	a.Host().Peerstore().AddAddrs(bid, baddr, peerstore.TempAddrTTL)
	if err := a.Host().Connect(ctx, peer.AddrInfo{ID: bid}); err != nil {
		t.Fatal(err)
	}
	wait(ctx, t, a, b)
}

func wait(ctx context.Context, t *testing.T, a, b *dht.IpfsDHT) {
	t.Helper()
	for a.RoutingTable().Find(b.PeerID()) == "" {
		// fmt.Fprintf(os.Stderr, "%v\n", a.RoutingTable().GetPeerInfos())
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond * 5):
		}
	}
}

func setupTier(ctx context.Context, t *testing.T) (*DHT, *dht.IpfsDHT, *dht.IpfsDHT) {
	t.Helper()
	baseOpts := []dht.Option{
		dht.NamespacedValidator("v", blankValidator{}),
		dht.ProtocolPrefix("/test"),
		dht.DisableAutoRefresh(),
	}

	d, hlprs := setupDHTWithFilters(ctx, t)

	whost, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	whost.Start()
	t.Cleanup(func() { whost.Close() })

	wan, err := dht.New(
		ctx,
		whost,
		append(baseOpts, dht.Mode(dht.ModeServer))...,
	)
	if err != nil {
		t.Fatal(err)
	}
	hlprs[0].allow = wan.PeerID()
	connect(ctx, t, d.WAN, wan)

	lhost, err := bhost.NewHost(swarmt.GenSwarm(t, swarmt.OptDisableReuseport), new(bhost.HostOpts))
	require.NoError(t, err)
	lhost.Start()
	t.Cleanup(func() { lhost.Close() })

	lan, err := dht.New(
		ctx,
		lhost,
		append(baseOpts, dht.Mode(dht.ModeServer), dht.ProtocolExtension("/lan"))...,
	)
	if err != nil {
		t.Fatal(err)
	}
	hlprs[1].allow = lan.PeerID()
	connect(ctx, t, d.LAN, lan)

	return d, wan, lan
}

func TestDualModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d := setupDHT(ctx, t)
	defer d.Close()

	if d.WAN.Mode() != dht.ModeAuto {
		t.Fatal("wrong default mode for wan")
	} else if d.LAN.Mode() != dht.ModeServer {
		t.Fatal("wrong default mode for lan")
	}

	d2 := setupDHT(ctx, t, dht.Mode(dht.ModeClient))
	defer d2.Close()
	if d2.WAN.Mode() != dht.ModeClient ||
		d2.LAN.Mode() != dht.ModeClient {
		t.Fatal("wrong client mode operation")
	}
}

func TestFindProviderAsync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, wan, lan := setupTier(ctx, t)
	defer d.Close()
	defer wan.Close()
	defer lan.Close()

	time.Sleep(5 * time.Millisecond)

	if err := wan.Provide(ctx, wancid, false); err != nil {
		t.Fatal(err)
	}

	if err := lan.Provide(ctx, lancid, true); err != nil {
		t.Fatal(err)
	}

	wpc := d.FindProvidersAsync(ctx, wancid, 1)
	select {
	case p := <-wpc:
		if p.ID != wan.PeerID() {
			t.Fatal("wrong wan provider")
		}
	case <-ctx.Done():
		t.Fatal("find provider timeout.")
	}

	lpc := d.FindProvidersAsync(ctx, lancid, 1)
	select {
	case p := <-lpc:
		if p.ID != lan.PeerID() {
			t.Fatal("wrong lan provider")
		}
	case <-ctx.Done():
		t.Fatal("find provider timeout.")
	}
}

func TestValueGetSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, wan, lan := setupTier(ctx, t)
	defer d.Close()
	defer wan.Close()
	defer lan.Close()

	time.Sleep(5 * time.Millisecond)

	err := d.PutValue(ctx, "/v/hello", []byte("valid"))
	if err != nil {
		t.Fatal(err)
	}
	val, err := wan.GetValue(ctx, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "valid" {
		t.Fatal("failed to get expected string.")
	}

	_, err = lan.GetValue(ctx, "/v/hello")
	if err == nil {
		t.Fatal(err)
	}
}

func TestSearchValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, wan, lan := setupTier(ctx, t)
	defer d.Close()
	defer wan.Close()
	defer lan.Close()

	d.WAN.Validator.(record.NamespacedValidator)["v"] = test.TestValidator{}
	d.LAN.Validator.(record.NamespacedValidator)["v"] = test.TestValidator{}

	_ = wan.PutValue(ctx, "/v/hello", []byte("valid"))

	valCh, err := d.SearchValue(ctx, "/v/hello", dht.Quorum(0))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case v := <-valCh:
		if string(v) != "valid" {
			t.Errorf("expected 'valid', got '%s'", string(v))
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case _, ok := <-valCh:
		if ok {
			t.Errorf("chan should close")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	err = lan.PutValue(ctx, "/v/hello", []byte("newer"))
	if err != nil {
		t.Error(err)
	}

	valCh, err = d.SearchValue(ctx, "/v/hello", dht.Quorum(0))
	if err != nil {
		t.Fatal(err)
	}

	var lastVal []byte
	for c := range valCh {
		lastVal = c
	}
	if string(lastVal) != "newer" {
		t.Fatal("incorrect best search value")
	}
}

func TestGetPublicKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, wan, lan := setupTier(ctx, t)
	defer d.Close()
	defer wan.Close()
	defer lan.Close()

	time.Sleep(5 * time.Millisecond)

	pk, err := d.GetPublicKey(ctx, wan.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}
	if id != wan.PeerID() {
		t.Fatal("incorrect PK")
	}

	pk, err = d.GetPublicKey(ctx, lan.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	id, err = peer.IDFromPublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}
	if id != lan.PeerID() {
		t.Fatal("incorrect PK")
	}
}

func TestFindPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, wan, lan := setupTier(ctx, t)
	defer d.Close()
	defer wan.Close()
	defer lan.Close()

	time.Sleep(5 * time.Millisecond)

	p, err := d.FindPeer(ctx, lan.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	assertUniqueMultiaddrs(t, p.Addrs)
	p, err = d.FindPeer(ctx, wan.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	assertUniqueMultiaddrs(t, p.Addrs)
}

func assertUniqueMultiaddrs(t *testing.T, addrs []multiaddr.Multiaddr) {
	set := make(map[string]bool)
	for _, addr := range addrs {
		if set[string(addr.Bytes())] {
			t.Errorf("duplicate address %s", addr)
		}
		set[string(addr.Bytes())] = true
	}
}
