package kvt

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
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
