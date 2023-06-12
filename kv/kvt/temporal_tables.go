package kvt

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
)

//go:generate enumer -type Domain,History,InvertedIdx --output temporal_enum_gen.go -gqlgen -json -text -yaml

type (
	Domain      remote.Domain
	History     remote.History
	InvertedIdx remote.InvertedIdx
)

const (
	AccountsDomain   Domain = Domain(remote.Domain_DOMAIN_ACCOUNT)
	StorageDomain    Domain = Domain(remote.Domain_DOMAIN_STORAGE)
	CodeDomain       Domain = Domain(remote.Domain_DOMAIN_CODE)
	CommitmentDomain Domain = Domain(remote.Domain_DOMAIN_COMMITMENT)

	DomainsAmount = 4
)

const (
	AccountsHistory   History = History(remote.History_HISTORY_ACCOUNT)
	StorageHistory    History = History(remote.History_HISTORY_STORAGE)
	CodeHistory       History = History(remote.History_HISTORY_CODE)
	CommitmentHistory History = History(remote.History_HISTORY_COMMITMENT)

	HistoriesAmount = 4
)

const (
	AccountsIdx   InvertedIdx = InvertedIdx(remote.InvertedIdx_INVERTED_IDX_ACCOUNT)
	StorageIdx    InvertedIdx = InvertedIdx(remote.InvertedIdx_INVERTED_IDX_STORAGE)
	CodeIdx       InvertedIdx = InvertedIdx(remote.InvertedIdx_INVERTED_IDX_CODE)
	CommitmentIdx InvertedIdx = InvertedIdx(remote.InvertedIdx_INVERTED_IDX_COMMITMENT)

	LogTopicIdx   InvertedIdx = InvertedIdx(remote.InvertedIdx_INVERTED_IDX_LOG_TOPIC)
	LogAddrIdx    InvertedIdx = InvertedIdx(remote.InvertedIdx_INVERTED_IDX_LOG_ADDR)
	TracesFromIdx InvertedIdx = InvertedIdx(remote.InvertedIdx_INVERTED_IDX_TRACE_FROM)
	TracesToIdx   InvertedIdx = InvertedIdx(remote.InvertedIdx_INVERTED_IDX_TRACE_TO)

	InvertedIndicesAmount = 8
)

type TemporalTx interface {
	kv.Tx
	DomainGet(name Domain, k, k2 []byte) (v []byte, ok bool, err error)
	DomainGetAsOf(name Domain, k, k2 []byte, ts uint64) (v []byte, ok bool, err error)
	HistoryGet(name History, k []byte, ts uint64) (v []byte, ok bool, err error)

	// IndexRange - return iterator over range of inverted index for given key `k`
	// Asc semantic:  [from, to) AND from > to
	// Desc semantic: [from, to) AND from < to
	// Limit -1 means Unlimited
	// from -1, to -1 means unbounded (StartOfTable, EndOfTable)
	// Example: IndexRange("IndexName", 10, 5, order.Desc, -1)
	// Example: IndexRange("IndexName", -1, -1, order.Asc, 10)
	IndexRange(name InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (timestamps iter.U64, err error)
	HistoryRange(name History, fromTs, toTs int, asc order.By, limit int) (it iter.KV, err error)
	DomainRange(name Domain, fromKey, toKey []byte, ts uint64, asc order.By, limit int) (it iter.KV, err error)
}
