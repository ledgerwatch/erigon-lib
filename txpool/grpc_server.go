package txpool

import (
	"context"
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_txpool "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TxPoolAPIVersion
var TxPoolAPIVersion = &types2.VersionReply{Major: 1, Minor: 0, Patch: 0}

type txPool interface {
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)
	AddLocals(txRlp [][]byte) []error
	DeprecatedForEach(_ context.Context, f func(rlp, sender []byte, t SubPoolType), tx kv.Tx) error

	//Get(hash common.Hash) types.Transaction
	//AddLocals(txs []types.Transaction) []error
	//Content() (map[common.Address]types.Transactions, map[common.Address]types.Transactions)
	//CountContent() (uint, uint)
	//SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription
}

type GrpcServer struct {
	proto_txpool.UnimplementedTxpoolServer
	ctx               context.Context
	txPool            txPool
	db                kv.RoDB
	newTxSlotsStreams *NewTxSlotsStreams
}

func NewTxPoolServer(ctx context.Context, txPool txPool, db kv.RoDB) *GrpcServer {
	return &GrpcServer{ctx: ctx, txPool: txPool, db: db, newTxSlotsStreams: &NewTxSlotsStreams{}}
}

func (s *GrpcServer) Version(context.Context, *emptypb.Empty) (*types2.VersionReply, error) {
	return TxPoolAPIVersion, nil
}
func convertSubPoolType(t SubPoolType) proto_txpool.AllReply_Type {
	switch t {
	case PendingSubPool:
		return proto_txpool.AllReply_PENDING
	case BaseFeeSubPool:
		return proto_txpool.AllReply_PENDING
	case QueuedSubPool:
		return proto_txpool.AllReply_QUEUED
	default:
		panic("unknown")
	}
}
func (s *GrpcServer) All(ctx context.Context, _ *proto_txpool.AllRequest) (*proto_txpool.AllReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	reply := &proto_txpool.AllReply{}
	reply.Txs = make([]*proto_txpool.AllReply_Tx, 0, 32)
	if err := s.txPool.DeprecatedForEach(ctx, func(rlp, sender []byte, t SubPoolType) {
		reply.Txs = append(reply.Txs, &proto_txpool.AllReply_Tx{
			Sender: sender,
			Type:   convertSubPoolType(t),
			RlpTx:  rlp,
		})
	}, tx); err != nil {
		return nil, err
	}
	return reply, nil
}

func (s *GrpcServer) FindUnknown(ctx context.Context, in *proto_txpool.TxHashes) (*proto_txpool.TxHashes, error) {
	return nil, fmt.Errorf("unimplemented")
	/*
		var underpriced int
		for i := range in.Hashes {
			h := gointerfaces.ConvertH256ToHash(in.Hashes[i])
			if s.txPool.Has(h) {
				continue
			}
			if s.underpriced.Contains(h) {
				underpriced++
				continue
			}
			reply.Hashes = append(reply.Hashes, in.Hashes[i])
		}
		txAnnounceInMeter.Mark(int64(len(in.Hashes)))
		txAnnounceKnownMeter.Mark(int64(len(in.Hashes) - len(reply.Hashes)))
		txAnnounceUnderpricedMeter.Mark(int64(underpriced))
	*/
}

func (s *GrpcServer) Add(ctx context.Context, in *proto_txpool.AddRequest) (*proto_txpool.AddReply, error) {
	reply := &proto_txpool.AddReply{Imported: make([]proto_txpool.ImportResult, len(in.RlpTxs)), Errors: make([]string, len(in.RlpTxs))}
	errs := s.txPool.AddLocals(in.RlpTxs)
	for i, err := range errs {
		if err == nil {
			continue
		}

		reply.Errors[i] = err.Error()

		// Track a few interesting failure types
		switch err {
		case nil: // Noop, but need to handle to not count these

		//case core.ErrAlreadyKnown:
		//	reply.Imported[i] = proto_txpool.ImportResult_ALREADY_EXISTS
		//case core.ErrUnderpriced, core.ErrReplaceUnderpriced:
		//	reply.Imported[i] = proto_txpool.ImportResult_FEE_TOO_LOW
		//case core.ErrInvalidSender, core.ErrGasLimit, core.ErrNegativeValue, core.ErrOversizedData:
		//	reply.Imported[i] = proto_txpool.ImportResult_INVALID
		default:
			reply.Imported[i] = proto_txpool.ImportResult_INTERNAL_ERROR
		}
	}
	return reply, nil
}

func (s *GrpcServer) OnAdd(req *proto_txpool.OnAddRequest, stream proto_txpool.Txpool_OnAddServer) error {
	//txpool.Loop does send messages to this streams
	remove := s.newTxSlotsStreams.Add(stream)
	defer remove()
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *GrpcServer) Transactions(ctx context.Context, in *proto_txpool.TransactionsRequest) (*proto_txpool.TransactionsReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	reply := &proto_txpool.TransactionsReply{RlpTxs: make([][]byte, len(in.Hashes))}
	for i := range in.Hashes {
		h := gointerfaces.ConvertH256ToHash(in.Hashes[i])
		txnRlp, err := s.txPool.GetRlp(tx, h[:])
		if err != nil {
			return nil, err
		}
		reply.RlpTxs[i] = txnRlp
	}

	return reply, nil
}

func (s *GrpcServer) Status(_ context.Context, _ *proto_txpool.StatusRequest) (*proto_txpool.StatusReply, error) {
	/*	pending, baseFee, queued := s.txPool.CountContent()
		return &proto_txpool.StatusReply{
			PendingCount: uint32(pending),
			QueuedCount:  uint32(queued),
			BaseFeeCount:  uint32(baseFee),
		}, nil
	*/return nil, nil
}

// NewTxSlotsStreams - it's safe to use this class as non-pointer
type NewTxSlotsStreams struct {
	chans map[uint]proto_txpool.Txpool_OnAddServer
	mu    sync.Mutex
	id    uint
}

func (s *NewTxSlotsStreams) Add(stream proto_txpool.Txpool_OnAddServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chans == nil {
		s.chans = make(map[uint]proto_txpool.Txpool_OnAddServer)
	}
	s.id++
	id := s.id
	s.chans[id] = stream
	return func() { s.remove(id) }
}

func (s *NewTxSlotsStreams) Broadcast(reply *proto_txpool.OnAddReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, stream := range s.chans {
		err := stream.Send(reply)
		if err != nil {
			log.Debug("failed send to mined block stream", "err", err)
			select {
			case <-stream.Context().Done():
				delete(s.chans, id)
			default:
			}
		}
	}
}

func (s *NewTxSlotsStreams) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chans[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.chans, id)
}
