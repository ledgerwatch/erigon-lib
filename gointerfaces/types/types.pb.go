// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.18.0
// source: types/types.proto

package types

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type H128 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Hi uint64 `protobuf:"varint,1,opt,name=hi,proto3" json:"hi,omitempty"`
	Lo uint64 `protobuf:"varint,2,opt,name=lo,proto3" json:"lo,omitempty"`
}

func (x *H128) Reset() {
	*x = H128{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *H128) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*H128) ProtoMessage() {}

func (x *H128) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use H128.ProtoReflect.Descriptor instead.
func (*H128) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{0}
}

func (x *H128) GetHi() uint64 {
	if x != nil {
		return x.Hi
	}
	return 0
}

func (x *H128) GetLo() uint64 {
	if x != nil {
		return x.Lo
	}
	return 0
}

type H160 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Hi *H128  `protobuf:"bytes,1,opt,name=hi,proto3" json:"hi,omitempty"`
	Lo uint32 `protobuf:"varint,2,opt,name=lo,proto3" json:"lo,omitempty"`
}

func (x *H160) Reset() {
	*x = H160{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *H160) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*H160) ProtoMessage() {}

func (x *H160) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use H160.ProtoReflect.Descriptor instead.
func (*H160) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{1}
}

func (x *H160) GetHi() *H128 {
	if x != nil {
		return x.Hi
	}
	return nil
}

func (x *H160) GetLo() uint32 {
	if x != nil {
		return x.Lo
	}
	return 0
}

type H256 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Hi *H128 `protobuf:"bytes,1,opt,name=hi,proto3" json:"hi,omitempty"`
	Lo *H128 `protobuf:"bytes,2,opt,name=lo,proto3" json:"lo,omitempty"`
}

func (x *H256) Reset() {
	*x = H256{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *H256) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*H256) ProtoMessage() {}

func (x *H256) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use H256.ProtoReflect.Descriptor instead.
func (*H256) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{2}
}

func (x *H256) GetHi() *H128 {
	if x != nil {
		return x.Hi
	}
	return nil
}

func (x *H256) GetLo() *H128 {
	if x != nil {
		return x.Lo
	}
	return nil
}

type H512 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Hi *H256 `protobuf:"bytes,1,opt,name=hi,proto3" json:"hi,omitempty"`
	Lo *H256 `protobuf:"bytes,2,opt,name=lo,proto3" json:"lo,omitempty"`
}

func (x *H512) Reset() {
	*x = H512{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *H512) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*H512) ProtoMessage() {}

func (x *H512) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use H512.ProtoReflect.Descriptor instead.
func (*H512) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{3}
}

func (x *H512) GetHi() *H256 {
	if x != nil {
		return x.Hi
	}
	return nil
}

func (x *H512) GetLo() *H256 {
	if x != nil {
		return x.Lo
	}
	return nil
}

type H1024 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Hi *H512 `protobuf:"bytes,1,opt,name=hi,proto3" json:"hi,omitempty"`
	Lo *H512 `protobuf:"bytes,2,opt,name=lo,proto3" json:"lo,omitempty"`
}

func (x *H1024) Reset() {
	*x = H1024{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *H1024) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*H1024) ProtoMessage() {}

func (x *H1024) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use H1024.ProtoReflect.Descriptor instead.
func (*H1024) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{4}
}

func (x *H1024) GetHi() *H512 {
	if x != nil {
		return x.Hi
	}
	return nil
}

func (x *H1024) GetLo() *H512 {
	if x != nil {
		return x.Lo
	}
	return nil
}

type H2048 struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Hi *H1024 `protobuf:"bytes,1,opt,name=hi,proto3" json:"hi,omitempty"`
	Lo *H1024 `protobuf:"bytes,2,opt,name=lo,proto3" json:"lo,omitempty"`
}

func (x *H2048) Reset() {
	*x = H2048{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *H2048) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*H2048) ProtoMessage() {}

func (x *H2048) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use H2048.ProtoReflect.Descriptor instead.
func (*H2048) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{5}
}

func (x *H2048) GetHi() *H1024 {
	if x != nil {
		return x.Hi
	}
	return nil
}

func (x *H2048) GetLo() *H1024 {
	if x != nil {
		return x.Lo
	}
	return nil
}

// Reply message containing the current service version on the service side
type VersionReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Major uint32 `protobuf:"varint,1,opt,name=major,proto3" json:"major,omitempty"`
	Minor uint32 `protobuf:"varint,2,opt,name=minor,proto3" json:"minor,omitempty"`
	Patch uint32 `protobuf:"varint,3,opt,name=patch,proto3" json:"patch,omitempty"`
}

func (x *VersionReply) Reset() {
	*x = VersionReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VersionReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VersionReply) ProtoMessage() {}

func (x *VersionReply) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VersionReply.ProtoReflect.Descriptor instead.
func (*VersionReply) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{6}
}

func (x *VersionReply) GetMajor() uint32 {
	if x != nil {
		return x.Major
	}
	return 0
}

func (x *VersionReply) GetMinor() uint32 {
	if x != nil {
		return x.Minor
	}
	return 0
}

func (x *VersionReply) GetPatch() uint32 {
	if x != nil {
		return x.Patch
	}
	return 0
}

type ExecutionPayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ParentHash    *H256  `protobuf:"bytes,1,opt,name=parentHash,proto3" json:"parentHash,omitempty"`
	Coinbase      *H160  `protobuf:"bytes,2,opt,name=coinbase,proto3" json:"coinbase,omitempty"`
	StateRoot     *H256  `protobuf:"bytes,3,opt,name=stateRoot,proto3" json:"stateRoot,omitempty"`
	ReceiptRoot   *H256  `protobuf:"bytes,4,opt,name=receiptRoot,proto3" json:"receiptRoot,omitempty"`
	LogsBloom     *H2048 `protobuf:"bytes,5,opt,name=logsBloom,proto3" json:"logsBloom,omitempty"`
	PrevRandao    *H256  `protobuf:"bytes,6,opt,name=prevRandao,proto3" json:"prevRandao,omitempty"`
	BlockNumber   uint64 `protobuf:"varint,7,opt,name=blockNumber,proto3" json:"blockNumber,omitempty"`
	GasLimit      uint64 `protobuf:"varint,8,opt,name=gasLimit,proto3" json:"gasLimit,omitempty"`
	GasUsed       uint64 `protobuf:"varint,9,opt,name=gasUsed,proto3" json:"gasUsed,omitempty"`
	Timestamp     uint64 `protobuf:"varint,10,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	ExtraData     []byte `protobuf:"bytes,11,opt,name=extraData,proto3" json:"extraData,omitempty"`
	BaseFeePerGas *H256  `protobuf:"bytes,12,opt,name=baseFeePerGas,proto3" json:"baseFeePerGas,omitempty"`
	BlockHash     *H256  `protobuf:"bytes,13,opt,name=blockHash,proto3" json:"blockHash,omitempty"`
	//
	//Array of transaction objects, each object is a byte list.
	//See https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.2/src/engine/interop/specification.md
	Transactions [][]byte `protobuf:"bytes,14,rep,name=transactions,proto3" json:"transactions,omitempty"`
}

func (x *ExecutionPayload) Reset() {
	*x = ExecutionPayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExecutionPayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecutionPayload) ProtoMessage() {}

func (x *ExecutionPayload) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecutionPayload.ProtoReflect.Descriptor instead.
func (*ExecutionPayload) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{7}
}

func (x *ExecutionPayload) GetParentHash() *H256 {
	if x != nil {
		return x.ParentHash
	}
	return nil
}

func (x *ExecutionPayload) GetCoinbase() *H160 {
	if x != nil {
		return x.Coinbase
	}
	return nil
}

func (x *ExecutionPayload) GetStateRoot() *H256 {
	if x != nil {
		return x.StateRoot
	}
	return nil
}

func (x *ExecutionPayload) GetReceiptRoot() *H256 {
	if x != nil {
		return x.ReceiptRoot
	}
	return nil
}

func (x *ExecutionPayload) GetLogsBloom() *H2048 {
	if x != nil {
		return x.LogsBloom
	}
	return nil
}

func (x *ExecutionPayload) GetPrevRandao() *H256 {
	if x != nil {
		return x.PrevRandao
	}
	return nil
}

func (x *ExecutionPayload) GetBlockNumber() uint64 {
	if x != nil {
		return x.BlockNumber
	}
	return 0
}

func (x *ExecutionPayload) GetGasLimit() uint64 {
	if x != nil {
		return x.GasLimit
	}
	return 0
}

func (x *ExecutionPayload) GetGasUsed() uint64 {
	if x != nil {
		return x.GasUsed
	}
	return 0
}

func (x *ExecutionPayload) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *ExecutionPayload) GetExtraData() []byte {
	if x != nil {
		return x.ExtraData
	}
	return nil
}

func (x *ExecutionPayload) GetBaseFeePerGas() *H256 {
	if x != nil {
		return x.BaseFeePerGas
	}
	return nil
}

func (x *ExecutionPayload) GetBlockHash() *H256 {
	if x != nil {
		return x.BlockHash
	}
	return nil
}

func (x *ExecutionPayload) GetTransactions() [][]byte {
	if x != nil {
		return x.Transactions
	}
	return nil
}

type NodeInfoPorts struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Discovery uint32 `protobuf:"varint,1,opt,name=discovery,proto3" json:"discovery,omitempty"`
	Listener  uint32 `protobuf:"varint,2,opt,name=listener,proto3" json:"listener,omitempty"`
}

func (x *NodeInfoPorts) Reset() {
	*x = NodeInfoPorts{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NodeInfoPorts) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NodeInfoPorts) ProtoMessage() {}

func (x *NodeInfoPorts) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NodeInfoPorts.ProtoReflect.Descriptor instead.
func (*NodeInfoPorts) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{8}
}

func (x *NodeInfoPorts) GetDiscovery() uint32 {
	if x != nil {
		return x.Discovery
	}
	return 0
}

func (x *NodeInfoPorts) GetListener() uint32 {
	if x != nil {
		return x.Listener
	}
	return 0
}

type NodeInfoReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id           string         `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name         string         `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Enode        string         `protobuf:"bytes,3,opt,name=enode,proto3" json:"enode,omitempty"`
	Enr          string         `protobuf:"bytes,4,opt,name=enr,proto3" json:"enr,omitempty"`
	Ports        *NodeInfoPorts `protobuf:"bytes,5,opt,name=ports,proto3" json:"ports,omitempty"`
	ListenerAddr string         `protobuf:"bytes,6,opt,name=listenerAddr,proto3" json:"listenerAddr,omitempty"`
	Protocols    []byte         `protobuf:"bytes,7,opt,name=protocols,proto3" json:"protocols,omitempty"`
}

func (x *NodeInfoReply) Reset() {
	*x = NodeInfoReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NodeInfoReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NodeInfoReply) ProtoMessage() {}

func (x *NodeInfoReply) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NodeInfoReply.ProtoReflect.Descriptor instead.
func (*NodeInfoReply) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{9}
}

func (x *NodeInfoReply) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *NodeInfoReply) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *NodeInfoReply) GetEnode() string {
	if x != nil {
		return x.Enode
	}
	return ""
}

func (x *NodeInfoReply) GetEnr() string {
	if x != nil {
		return x.Enr
	}
	return ""
}

func (x *NodeInfoReply) GetPorts() *NodeInfoPorts {
	if x != nil {
		return x.Ports
	}
	return nil
}

func (x *NodeInfoReply) GetListenerAddr() string {
	if x != nil {
		return x.ListenerAddr
	}
	return ""
}

func (x *NodeInfoReply) GetProtocols() []byte {
	if x != nil {
		return x.Protocols
	}
	return nil
}

type PeerInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id             string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name           string   `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Enode          string   `protobuf:"bytes,3,opt,name=enode,proto3" json:"enode,omitempty"`
	Enr            string   `protobuf:"bytes,4,opt,name=enr,proto3" json:"enr,omitempty"`
	Caps           []string `protobuf:"bytes,5,rep,name=caps,proto3" json:"caps,omitempty"`
	ConnLocalAddr  string   `protobuf:"bytes,6,opt,name=connLocalAddr,proto3" json:"connLocalAddr,omitempty"`
	ConnRemoteAddr string   `protobuf:"bytes,7,opt,name=connRemoteAddr,proto3" json:"connRemoteAddr,omitempty"`
	ConnIsInbound  bool     `protobuf:"varint,8,opt,name=connIsInbound,proto3" json:"connIsInbound,omitempty"`
	ConnIsTrusted  bool     `protobuf:"varint,9,opt,name=connIsTrusted,proto3" json:"connIsTrusted,omitempty"`
	ConnIsStatic   bool     `protobuf:"varint,10,opt,name=connIsStatic,proto3" json:"connIsStatic,omitempty"`
}

func (x *PeerInfo) Reset() {
	*x = PeerInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_types_types_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PeerInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PeerInfo) ProtoMessage() {}

func (x *PeerInfo) ProtoReflect() protoreflect.Message {
	mi := &file_types_types_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PeerInfo.ProtoReflect.Descriptor instead.
func (*PeerInfo) Descriptor() ([]byte, []int) {
	return file_types_types_proto_rawDescGZIP(), []int{10}
}

func (x *PeerInfo) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *PeerInfo) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *PeerInfo) GetEnode() string {
	if x != nil {
		return x.Enode
	}
	return ""
}

func (x *PeerInfo) GetEnr() string {
	if x != nil {
		return x.Enr
	}
	return ""
}

func (x *PeerInfo) GetCaps() []string {
	if x != nil {
		return x.Caps
	}
	return nil
}

func (x *PeerInfo) GetConnLocalAddr() string {
	if x != nil {
		return x.ConnLocalAddr
	}
	return ""
}

func (x *PeerInfo) GetConnRemoteAddr() string {
	if x != nil {
		return x.ConnRemoteAddr
	}
	return ""
}

func (x *PeerInfo) GetConnIsInbound() bool {
	if x != nil {
		return x.ConnIsInbound
	}
	return false
}

func (x *PeerInfo) GetConnIsTrusted() bool {
	if x != nil {
		return x.ConnIsTrusted
	}
	return false
}

func (x *PeerInfo) GetConnIsStatic() bool {
	if x != nil {
		return x.ConnIsStatic
	}
	return false
}

var file_types_types_proto_extTypes = []protoimpl.ExtensionInfo{
	{
		ExtendedType:  (*descriptorpb.FileOptions)(nil),
		ExtensionType: (*uint32)(nil),
		Field:         50001,
		Name:          "types.service_major_version",
		Tag:           "varint,50001,opt,name=service_major_version",
		Filename:      "types/types.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FileOptions)(nil),
		ExtensionType: (*uint32)(nil),
		Field:         50002,
		Name:          "types.service_minor_version",
		Tag:           "varint,50002,opt,name=service_minor_version",
		Filename:      "types/types.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FileOptions)(nil),
		ExtensionType: (*uint32)(nil),
		Field:         50003,
		Name:          "types.service_patch_version",
		Tag:           "varint,50003,opt,name=service_patch_version",
		Filename:      "types/types.proto",
	},
}

// Extension fields to descriptorpb.FileOptions.
var (
	// optional uint32 service_major_version = 50001;
	E_ServiceMajorVersion = &file_types_types_proto_extTypes[0]
	// optional uint32 service_minor_version = 50002;
	E_ServiceMinorVersion = &file_types_types_proto_extTypes[1]
	// optional uint32 service_patch_version = 50003;
	E_ServicePatchVersion = &file_types_types_proto_extTypes[2]
)

var File_types_types_proto protoreflect.FileDescriptor

var file_types_types_proto_rawDesc = []byte{
	0x0a, 0x11, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2f, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x05, 0x74, 0x79, 0x70, 0x65, 0x73, 0x1a, 0x20, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x65, 0x73, 0x63,
	0x72, 0x69, 0x70, 0x74, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x26, 0x0a, 0x04,
	0x48, 0x31, 0x32, 0x38, 0x12, 0x0e, 0x0a, 0x02, 0x68, 0x69, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x02, 0x68, 0x69, 0x12, 0x0e, 0x0a, 0x02, 0x6c, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x02, 0x6c, 0x6f, 0x22, 0x33, 0x0a, 0x04, 0x48, 0x31, 0x36, 0x30, 0x12, 0x1b, 0x0a, 0x02,
	0x68, 0x69, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73,
	0x2e, 0x48, 0x31, 0x32, 0x38, 0x52, 0x02, 0x68, 0x69, 0x12, 0x0e, 0x0a, 0x02, 0x6c, 0x6f, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x02, 0x6c, 0x6f, 0x22, 0x40, 0x0a, 0x04, 0x48, 0x32, 0x35,
	0x36, 0x12, 0x1b, 0x0a, 0x02, 0x68, 0x69, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e,
	0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x31, 0x32, 0x38, 0x52, 0x02, 0x68, 0x69, 0x12, 0x1b,
	0x0a, 0x02, 0x6c, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70,
	0x65, 0x73, 0x2e, 0x48, 0x31, 0x32, 0x38, 0x52, 0x02, 0x6c, 0x6f, 0x22, 0x40, 0x0a, 0x04, 0x48,
	0x35, 0x31, 0x32, 0x12, 0x1b, 0x0a, 0x02, 0x68, 0x69, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52, 0x02, 0x68, 0x69,
	0x12, 0x1b, 0x0a, 0x02, 0x6c, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74,
	0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52, 0x02, 0x6c, 0x6f, 0x22, 0x41, 0x0a,
	0x05, 0x48, 0x31, 0x30, 0x32, 0x34, 0x12, 0x1b, 0x0a, 0x02, 0x68, 0x69, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x35, 0x31, 0x32, 0x52,
	0x02, 0x68, 0x69, 0x12, 0x1b, 0x0a, 0x02, 0x6c, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x35, 0x31, 0x32, 0x52, 0x02, 0x6c, 0x6f,
	0x22, 0x43, 0x0a, 0x05, 0x48, 0x32, 0x30, 0x34, 0x38, 0x12, 0x1c, 0x0a, 0x02, 0x68, 0x69, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x31,
	0x30, 0x32, 0x34, 0x52, 0x02, 0x68, 0x69, 0x12, 0x1c, 0x0a, 0x02, 0x6c, 0x6f, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x31, 0x30, 0x32,
	0x34, 0x52, 0x02, 0x6c, 0x6f, 0x22, 0x50, 0x0a, 0x0c, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x6d, 0x61, 0x6a, 0x6f, 0x72, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x6d, 0x61, 0x6a, 0x6f, 0x72, 0x12, 0x14, 0x0a, 0x05, 0x6d,
	0x69, 0x6e, 0x6f, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x6d, 0x69, 0x6e, 0x6f,
	0x72, 0x12, 0x14, 0x0a, 0x05, 0x70, 0x61, 0x74, 0x63, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x05, 0x70, 0x61, 0x74, 0x63, 0x68, 0x22, 0xb1, 0x04, 0x0a, 0x10, 0x45, 0x78, 0x65, 0x63,
	0x75, 0x74, 0x69, 0x6f, 0x6e, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x2b, 0x0a, 0x0a,
	0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x48, 0x61, 0x73, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52, 0x0a, 0x70,
	0x61, 0x72, 0x65, 0x6e, 0x74, 0x48, 0x61, 0x73, 0x68, 0x12, 0x27, 0x0a, 0x08, 0x63, 0x6f, 0x69,
	0x6e, 0x62, 0x61, 0x73, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x79,
	0x70, 0x65, 0x73, 0x2e, 0x48, 0x31, 0x36, 0x30, 0x52, 0x08, 0x63, 0x6f, 0x69, 0x6e, 0x62, 0x61,
	0x73, 0x65, 0x12, 0x29, 0x0a, 0x09, 0x73, 0x74, 0x61, 0x74, 0x65, 0x52, 0x6f, 0x6f, 0x74, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32,
	0x35, 0x36, 0x52, 0x09, 0x73, 0x74, 0x61, 0x74, 0x65, 0x52, 0x6f, 0x6f, 0x74, 0x12, 0x2d, 0x0a,
	0x0b, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x52, 0x6f, 0x6f, 0x74, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52,
	0x0b, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x52, 0x6f, 0x6f, 0x74, 0x12, 0x2a, 0x0a, 0x09,
	0x6c, 0x6f, 0x67, 0x73, 0x42, 0x6c, 0x6f, 0x6f, 0x6d, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x0c, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x30, 0x34, 0x38, 0x52, 0x09, 0x6c,
	0x6f, 0x67, 0x73, 0x42, 0x6c, 0x6f, 0x6f, 0x6d, 0x12, 0x2b, 0x0a, 0x0a, 0x70, 0x72, 0x65, 0x76,
	0x52, 0x61, 0x6e, 0x64, 0x61, 0x6f, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74,
	0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52, 0x0a, 0x70, 0x72, 0x65, 0x76, 0x52,
	0x61, 0x6e, 0x64, 0x61, 0x6f, 0x12, 0x20, 0x0a, 0x0b, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x4e, 0x75,
	0x6d, 0x62, 0x65, 0x72, 0x18, 0x07, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0b, 0x62, 0x6c, 0x6f, 0x63,
	0x6b, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x12, 0x1a, 0x0a, 0x08, 0x67, 0x61, 0x73, 0x4c, 0x69,
	0x6d, 0x69, 0x74, 0x18, 0x08, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x67, 0x61, 0x73, 0x4c, 0x69,
	0x6d, 0x69, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x67, 0x61, 0x73, 0x55, 0x73, 0x65, 0x64, 0x18, 0x09,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x67, 0x61, 0x73, 0x55, 0x73, 0x65, 0x64, 0x12, 0x1c, 0x0a,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1c, 0x0a, 0x09, 0x65,
	0x78, 0x74, 0x72, 0x61, 0x44, 0x61, 0x74, 0x61, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09,
	0x65, 0x78, 0x74, 0x72, 0x61, 0x44, 0x61, 0x74, 0x61, 0x12, 0x31, 0x0a, 0x0d, 0x62, 0x61, 0x73,
	0x65, 0x46, 0x65, 0x65, 0x50, 0x65, 0x72, 0x47, 0x61, 0x73, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52, 0x0d, 0x62,
	0x61, 0x73, 0x65, 0x46, 0x65, 0x65, 0x50, 0x65, 0x72, 0x47, 0x61, 0x73, 0x12, 0x29, 0x0a, 0x09,
	0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x48, 0x61, 0x73, 0x68, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52, 0x09, 0x62, 0x6c,
	0x6f, 0x63, 0x6b, 0x48, 0x61, 0x73, 0x68, 0x12, 0x22, 0x0a, 0x0c, 0x74, 0x72, 0x61, 0x6e, 0x73,
	0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x0e, 0x20, 0x03, 0x28, 0x0c, 0x52, 0x0c, 0x74,
	0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x22, 0x49, 0x0a, 0x0d, 0x4e,
	0x6f, 0x64, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x50, 0x6f, 0x72, 0x74, 0x73, 0x12, 0x1c, 0x0a, 0x09,
	0x64, 0x69, 0x73, 0x63, 0x6f, 0x76, 0x65, 0x72, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52,
	0x09, 0x64, 0x69, 0x73, 0x63, 0x6f, 0x76, 0x65, 0x72, 0x79, 0x12, 0x1a, 0x0a, 0x08, 0x6c, 0x69,
	0x73, 0x74, 0x65, 0x6e, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x6c, 0x69,
	0x73, 0x74, 0x65, 0x6e, 0x65, 0x72, 0x22, 0xc9, 0x01, 0x0a, 0x0d, 0x4e, 0x6f, 0x64, 0x65, 0x49,
	0x6e, 0x66, 0x6f, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05,
	0x65, 0x6e, 0x6f, 0x64, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x65, 0x6e, 0x6f,
	0x64, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x03, 0x65, 0x6e, 0x72, 0x12, 0x2a, 0x0a, 0x05, 0x70, 0x6f, 0x72, 0x74, 0x73, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x4e, 0x6f, 0x64, 0x65,
	0x49, 0x6e, 0x66, 0x6f, 0x50, 0x6f, 0x72, 0x74, 0x73, 0x52, 0x05, 0x70, 0x6f, 0x72, 0x74, 0x73,
	0x12, 0x22, 0x0a, 0x0c, 0x6c, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x65, 0x72, 0x41, 0x64, 0x64, 0x72,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x6c, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x65, 0x72,
	0x41, 0x64, 0x64, 0x72, 0x12, 0x1c, 0x0a, 0x09, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c,
	0x73, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x09, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f,
	0x6c, 0x73, 0x22, 0xa8, 0x02, 0x0a, 0x08, 0x50, 0x65, 0x65, 0x72, 0x49, 0x6e, 0x66, 0x6f, 0x12,
	0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12,
	0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e,
	0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x65, 0x6e, 0x6f, 0x64, 0x65, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x05, 0x65, 0x6e, 0x6f, 0x64, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x65, 0x6e, 0x72,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x65, 0x6e, 0x72, 0x12, 0x12, 0x0a, 0x04, 0x63,
	0x61, 0x70, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x63, 0x61, 0x70, 0x73, 0x12,
	0x24, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x6e, 0x4c, 0x6f, 0x63, 0x61, 0x6c, 0x41, 0x64, 0x64, 0x72,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x63, 0x6f, 0x6e, 0x6e, 0x4c, 0x6f, 0x63, 0x61,
	0x6c, 0x41, 0x64, 0x64, 0x72, 0x12, 0x26, 0x0a, 0x0e, 0x63, 0x6f, 0x6e, 0x6e, 0x52, 0x65, 0x6d,
	0x6f, 0x74, 0x65, 0x41, 0x64, 0x64, 0x72, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x63,
	0x6f, 0x6e, 0x6e, 0x52, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x41, 0x64, 0x64, 0x72, 0x12, 0x24, 0x0a,
	0x0d, 0x63, 0x6f, 0x6e, 0x6e, 0x49, 0x73, 0x49, 0x6e, 0x62, 0x6f, 0x75, 0x6e, 0x64, 0x18, 0x08,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x0d, 0x63, 0x6f, 0x6e, 0x6e, 0x49, 0x73, 0x49, 0x6e, 0x62, 0x6f,
	0x75, 0x6e, 0x64, 0x12, 0x24, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x6e, 0x49, 0x73, 0x54, 0x72, 0x75,
	0x73, 0x74, 0x65, 0x64, 0x18, 0x09, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0d, 0x63, 0x6f, 0x6e, 0x6e,
	0x49, 0x73, 0x54, 0x72, 0x75, 0x73, 0x74, 0x65, 0x64, 0x12, 0x22, 0x0a, 0x0c, 0x63, 0x6f, 0x6e,
	0x6e, 0x49, 0x73, 0x53, 0x74, 0x61, 0x74, 0x69, 0x63, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x0c, 0x63, 0x6f, 0x6e, 0x6e, 0x49, 0x73, 0x53, 0x74, 0x61, 0x74, 0x69, 0x63, 0x3a, 0x52, 0x0a,
	0x15, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x5f, 0x6d, 0x61, 0x6a, 0x6f, 0x72, 0x5f, 0x76,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1c, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x6c, 0x65, 0x4f, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x73, 0x18, 0xd1, 0x86, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x13, 0x73, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x4d, 0x61, 0x6a, 0x6f, 0x72, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x3a, 0x52, 0x0a, 0x15, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x5f, 0x6d, 0x69, 0x6e,
	0x6f, 0x72, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1c, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x6c,
	0x65, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xd2, 0x86, 0x03, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x13, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4d, 0x69, 0x6e, 0x6f, 0x72, 0x56, 0x65,
	0x72, 0x73, 0x69, 0x6f, 0x6e, 0x3a, 0x52, 0x0a, 0x15, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x5f, 0x70, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1c,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2e, 0x46, 0x69, 0x6c, 0x65, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xd3, 0x86, 0x03,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x13, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x50, 0x61, 0x74,
	0x63, 0x68, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x42, 0x0f, 0x5a, 0x0d, 0x2e, 0x2f, 0x74,
	0x79, 0x70, 0x65, 0x73, 0x3b, 0x74, 0x79, 0x70, 0x65, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_types_types_proto_rawDescOnce sync.Once
	file_types_types_proto_rawDescData = file_types_types_proto_rawDesc
)

func file_types_types_proto_rawDescGZIP() []byte {
	file_types_types_proto_rawDescOnce.Do(func() {
		file_types_types_proto_rawDescData = protoimpl.X.CompressGZIP(file_types_types_proto_rawDescData)
	})
	return file_types_types_proto_rawDescData
}

var file_types_types_proto_msgTypes = make([]protoimpl.MessageInfo, 11)
var file_types_types_proto_goTypes = []interface{}{
	(*H128)(nil),                     // 0: types.H128
	(*H160)(nil),                     // 1: types.H160
	(*H256)(nil),                     // 2: types.H256
	(*H512)(nil),                     // 3: types.H512
	(*H1024)(nil),                    // 4: types.H1024
	(*H2048)(nil),                    // 5: types.H2048
	(*VersionReply)(nil),             // 6: types.VersionReply
	(*ExecutionPayload)(nil),         // 7: types.ExecutionPayload
	(*NodeInfoPorts)(nil),            // 8: types.NodeInfoPorts
	(*NodeInfoReply)(nil),            // 9: types.NodeInfoReply
	(*PeerInfo)(nil),                 // 10: types.PeerInfo
	(*descriptorpb.FileOptions)(nil), // 11: google.protobuf.FileOptions
}
var file_types_types_proto_depIdxs = []int32{
	0,  // 0: types.H160.hi:type_name -> types.H128
	0,  // 1: types.H256.hi:type_name -> types.H128
	0,  // 2: types.H256.lo:type_name -> types.H128
	2,  // 3: types.H512.hi:type_name -> types.H256
	2,  // 4: types.H512.lo:type_name -> types.H256
	3,  // 5: types.H1024.hi:type_name -> types.H512
	3,  // 6: types.H1024.lo:type_name -> types.H512
	4,  // 7: types.H2048.hi:type_name -> types.H1024
	4,  // 8: types.H2048.lo:type_name -> types.H1024
	2,  // 9: types.ExecutionPayload.parentHash:type_name -> types.H256
	1,  // 10: types.ExecutionPayload.coinbase:type_name -> types.H160
	2,  // 11: types.ExecutionPayload.stateRoot:type_name -> types.H256
	2,  // 12: types.ExecutionPayload.receiptRoot:type_name -> types.H256
	5,  // 13: types.ExecutionPayload.logsBloom:type_name -> types.H2048
	2,  // 14: types.ExecutionPayload.prevRandao:type_name -> types.H256
	2,  // 15: types.ExecutionPayload.baseFeePerGas:type_name -> types.H256
	2,  // 16: types.ExecutionPayload.blockHash:type_name -> types.H256
	8,  // 17: types.NodeInfoReply.ports:type_name -> types.NodeInfoPorts
	11, // 18: types.service_major_version:extendee -> google.protobuf.FileOptions
	11, // 19: types.service_minor_version:extendee -> google.protobuf.FileOptions
	11, // 20: types.service_patch_version:extendee -> google.protobuf.FileOptions
	21, // [21:21] is the sub-list for method output_type
	21, // [21:21] is the sub-list for method input_type
	21, // [21:21] is the sub-list for extension type_name
	18, // [18:21] is the sub-list for extension extendee
	0,  // [0:18] is the sub-list for field type_name
}

func init() { file_types_types_proto_init() }
func file_types_types_proto_init() {
	if File_types_types_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_types_types_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*H128); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*H160); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*H256); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*H512); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*H1024); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*H2048); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VersionReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExecutionPayload); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NodeInfoPorts); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NodeInfoReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_types_types_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PeerInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_types_types_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   11,
			NumExtensions: 3,
			NumServices:   0,
		},
		GoTypes:           file_types_types_proto_goTypes,
		DependencyIndexes: file_types_types_proto_depIdxs,
		MessageInfos:      file_types_types_proto_msgTypes,
		ExtensionInfos:    file_types_types_proto_extTypes,
	}.Build()
	File_types_types_proto = out.File
	file_types_types_proto_rawDesc = nil
	file_types_types_proto_goTypes = nil
	file_types_types_proto_depIdxs = nil
}
