package dht

import (
	"sync/atomic"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

// Process-wide monotonic counters of incoming DHT queries by message type.
// Counts every request that reaches handlerForMsgType, regardless of whether
// the handler returns successfully. Read non-destructively via SnapshotIncomingQueries.
var incomingCounters [6]uint64

// IncomingQueryCounts is a snapshot of the per-type counters at one point in time.
type IncomingQueryCounts struct {
	PutValue     uint64 `json:"put_value"`
	GetValue     uint64 `json:"get_value"`
	AddProvider  uint64 `json:"add_provider"`
	GetProviders uint64 `json:"get_providers"`
	FindNode     uint64 `json:"find_node"`
	Ping         uint64 `json:"ping"`
}

func recordIncomingQuery(t pb.Message_MessageType) {
	if int(t) < len(incomingCounters) {
		atomic.AddUint64(&incomingCounters[t], 1)
	}
}

// SnapshotIncomingQueries returns the current counter values. Safe for concurrent use.
func SnapshotIncomingQueries() IncomingQueryCounts {
	return IncomingQueryCounts{
		PutValue:     atomic.LoadUint64(&incomingCounters[pb.Message_PUT_VALUE]),
		GetValue:     atomic.LoadUint64(&incomingCounters[pb.Message_GET_VALUE]),
		AddProvider:  atomic.LoadUint64(&incomingCounters[pb.Message_ADD_PROVIDER]),
		GetProviders: atomic.LoadUint64(&incomingCounters[pb.Message_GET_PROVIDERS]),
		FindNode:     atomic.LoadUint64(&incomingCounters[pb.Message_FIND_NODE]),
		Ping:         atomic.LoadUint64(&incomingCounters[pb.Message_PING]),
	}
}
