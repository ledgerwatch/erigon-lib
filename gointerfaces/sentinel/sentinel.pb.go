// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v4.22.1
// source: p2psentinel/sentinel.proto

package sentinel

import (
	types "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type GossipType int32

const (
	// Lightclient gossip
	GossipType_LightClientFinalityUpdateGossipType   GossipType = 0
	GossipType_LightClientOptimisticUpdateGossipType GossipType = 1
	// Legacy gossip
	GossipType_BeaconBlockGossipType GossipType = 2
	// Global gossip topics.
	GossipType_AggregateAndProofGossipType GossipType = 3
	GossipType_VoluntaryExitGossipType     GossipType = 4
	GossipType_ProposerSlashingGossipType  GossipType = 5
	GossipType_AttesterSlashingGossipType  GossipType = 6
)

// Enum value maps for GossipType.
var (
	GossipType_name = map[int32]string{
		0: "LightClientFinalityUpdateGossipType",
		1: "LightClientOptimisticUpdateGossipType",
		2: "BeaconBlockGossipType",
		3: "AggregateAndProofGossipType",
		4: "VoluntaryExitGossipType",
		5: "ProposerSlashingGossipType",
		6: "AttesterSlashingGossipType",
	}
	GossipType_value = map[string]int32{
		"LightClientFinalityUpdateGossipType":   0,
		"LightClientOptimisticUpdateGossipType": 1,
		"BeaconBlockGossipType":                 2,
		"AggregateAndProofGossipType":           3,
		"VoluntaryExitGossipType":               4,
		"ProposerSlashingGossipType":            5,
		"AttesterSlashingGossipType":            6,
	}
)

func (x GossipType) Enum() *GossipType {
	p := new(GossipType)
	*p = x
	return p
}

func (x GossipType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (GossipType) Descriptor() protoreflect.EnumDescriptor {
	return file_p2psentinel_sentinel_proto_enumTypes[0].Descriptor()
}

func (GossipType) Type() protoreflect.EnumType {
	return &file_p2psentinel_sentinel_proto_enumTypes[0]
}

func (x GossipType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use GossipType.Descriptor instead.
func (GossipType) EnumDescriptor() ([]byte, []int) {
	return file_p2psentinel_sentinel_proto_rawDescGZIP(), []int{0}
}

type EmptyMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *EmptyMessage) Reset() {
	*x = EmptyMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_p2psentinel_sentinel_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EmptyMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EmptyMessage) ProtoMessage() {}

func (x *EmptyMessage) ProtoReflect() protoreflect.Message {
	mi := &file_p2psentinel_sentinel_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EmptyMessage.ProtoReflect.Descriptor instead.
func (*EmptyMessage) Descriptor() ([]byte, []int) {
	return file_p2psentinel_sentinel_proto_rawDescGZIP(), []int{0}
}

type GossipData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data []byte     `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"` // SSZ encoded data
	Type GossipType `protobuf:"varint,2,opt,name=type,proto3,enum=sentinel.GossipType" json:"type,omitempty"`
}

func (x *GossipData) Reset() {
	*x = GossipData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_p2psentinel_sentinel_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GossipData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GossipData) ProtoMessage() {}

func (x *GossipData) ProtoReflect() protoreflect.Message {
	mi := &file_p2psentinel_sentinel_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GossipData.ProtoReflect.Descriptor instead.
func (*GossipData) Descriptor() ([]byte, []int) {
	return file_p2psentinel_sentinel_proto_rawDescGZIP(), []int{1}
}

func (x *GossipData) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *GossipData) GetType() GossipType {
	if x != nil {
		return x.Type
	}
	return GossipType_LightClientFinalityUpdateGossipType
}

type Status struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ForkDigest     uint32      `protobuf:"varint,1,opt,name=fork_digest,json=forkDigest,proto3" json:"fork_digest,omitempty"` // 4 bytes can be repressented in uint32.
	FinalizedRoot  *types.H256 `protobuf:"bytes,2,opt,name=finalized_root,json=finalizedRoot,proto3" json:"finalized_root,omitempty"`
	FinalizedEpoch uint64      `protobuf:"varint,3,opt,name=finalized_epoch,json=finalizedEpoch,proto3" json:"finalized_epoch,omitempty"`
	HeadRoot       *types.H256 `protobuf:"bytes,4,opt,name=head_root,json=headRoot,proto3" json:"head_root,omitempty"`
	HeadSlot       uint64      `protobuf:"varint,5,opt,name=head_slot,json=headSlot,proto3" json:"head_slot,omitempty"`
}

func (x *Status) Reset() {
	*x = Status{}
	if protoimpl.UnsafeEnabled {
		mi := &file_p2psentinel_sentinel_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Status) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Status) ProtoMessage() {}

func (x *Status) ProtoReflect() protoreflect.Message {
	mi := &file_p2psentinel_sentinel_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Status.ProtoReflect.Descriptor instead.
func (*Status) Descriptor() ([]byte, []int) {
	return file_p2psentinel_sentinel_proto_rawDescGZIP(), []int{2}
}

func (x *Status) GetForkDigest() uint32 {
	if x != nil {
		return x.ForkDigest
	}
	return 0
}

func (x *Status) GetFinalizedRoot() *types.H256 {
	if x != nil {
		return x.FinalizedRoot
	}
	return nil
}

func (x *Status) GetFinalizedEpoch() uint64 {
	if x != nil {
		return x.FinalizedEpoch
	}
	return 0
}

func (x *Status) GetHeadRoot() *types.H256 {
	if x != nil {
		return x.HeadRoot
	}
	return nil
}

func (x *Status) GetHeadSlot() uint64 {
	if x != nil {
		return x.HeadSlot
	}
	return 0
}

type PeerCount struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Amount uint64 `protobuf:"varint,1,opt,name=amount,proto3" json:"amount,omitempty"`
}

func (x *PeerCount) Reset() {
	*x = PeerCount{}
	if protoimpl.UnsafeEnabled {
		mi := &file_p2psentinel_sentinel_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PeerCount) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PeerCount) ProtoMessage() {}

func (x *PeerCount) ProtoReflect() protoreflect.Message {
	mi := &file_p2psentinel_sentinel_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PeerCount.ProtoReflect.Descriptor instead.
func (*PeerCount) Descriptor() ([]byte, []int) {
	return file_p2psentinel_sentinel_proto_rawDescGZIP(), []int{3}
}

func (x *PeerCount) GetAmount() uint64 {
	if x != nil {
		return x.Amount
	}
	return 0
}

type RequestData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data  []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"` // SSZ encoded data
	Topic string `protobuf:"bytes,2,opt,name=topic,proto3" json:"topic,omitempty"`
}

func (x *RequestData) Reset() {
	*x = RequestData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_p2psentinel_sentinel_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestData) ProtoMessage() {}

func (x *RequestData) ProtoReflect() protoreflect.Message {
	mi := &file_p2psentinel_sentinel_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestData.ProtoReflect.Descriptor instead.
func (*RequestData) Descriptor() ([]byte, []int) {
	return file_p2psentinel_sentinel_proto_rawDescGZIP(), []int{4}
}

func (x *RequestData) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *RequestData) GetTopic() string {
	if x != nil {
		return x.Topic
	}
	return ""
}

type ResponseData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data  []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`    // prefix-stripped SSZ encoded data
	Error bool   `protobuf:"varint,2,opt,name=error,proto3" json:"error,omitempty"` // did the peer encounter an error
}

func (x *ResponseData) Reset() {
	*x = ResponseData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_p2psentinel_sentinel_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ResponseData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ResponseData) ProtoMessage() {}

func (x *ResponseData) ProtoReflect() protoreflect.Message {
	mi := &file_p2psentinel_sentinel_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ResponseData.ProtoReflect.Descriptor instead.
func (*ResponseData) Descriptor() ([]byte, []int) {
	return file_p2psentinel_sentinel_proto_rawDescGZIP(), []int{5}
}

func (x *ResponseData) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *ResponseData) GetError() bool {
	if x != nil {
		return x.Error
	}
	return false
}

var File_p2psentinel_sentinel_proto protoreflect.FileDescriptor

var file_p2psentinel_sentinel_proto_rawDesc = []byte{
	0x0a, 0x1a, 0x70, 0x32, 0x70, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x2f, 0x73, 0x65,
	0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x73, 0x65,
	0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x1a, 0x11, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2f, 0x74, 0x79,
	0x70, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x0e, 0x0a, 0x0c, 0x45, 0x6d, 0x70,
	0x74, 0x79, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x4a, 0x0a, 0x0a, 0x47, 0x6f, 0x73,
	0x73, 0x69, 0x70, 0x44, 0x61, 0x74, 0x61, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x28, 0x0a, 0x04, 0x74,
	0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x14, 0x2e, 0x73, 0x65, 0x6e, 0x74,
	0x69, 0x6e, 0x65, 0x6c, 0x2e, 0x47, 0x6f, 0x73, 0x73, 0x69, 0x70, 0x54, 0x79, 0x70, 0x65, 0x52,
	0x04, 0x74, 0x79, 0x70, 0x65, 0x22, 0xcd, 0x01, 0x0a, 0x06, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x12, 0x1f, 0x0a, 0x0b, 0x66, 0x6f, 0x72, 0x6b, 0x5f, 0x64, 0x69, 0x67, 0x65, 0x73, 0x74, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0a, 0x66, 0x6f, 0x72, 0x6b, 0x44, 0x69, 0x67, 0x65, 0x73,
	0x74, 0x12, 0x32, 0x0a, 0x0e, 0x66, 0x69, 0x6e, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x5f, 0x72,
	0x6f, 0x6f, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65,
	0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52, 0x0d, 0x66, 0x69, 0x6e, 0x61, 0x6c, 0x69, 0x7a, 0x65,
	0x64, 0x52, 0x6f, 0x6f, 0x74, 0x12, 0x27, 0x0a, 0x0f, 0x66, 0x69, 0x6e, 0x61, 0x6c, 0x69, 0x7a,
	0x65, 0x64, 0x5f, 0x65, 0x70, 0x6f, 0x63, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0e,
	0x66, 0x69, 0x6e, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x45, 0x70, 0x6f, 0x63, 0x68, 0x12, 0x28,
	0x0a, 0x09, 0x68, 0x65, 0x61, 0x64, 0x5f, 0x72, 0x6f, 0x6f, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x48, 0x32, 0x35, 0x36, 0x52, 0x08,
	0x68, 0x65, 0x61, 0x64, 0x52, 0x6f, 0x6f, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x68, 0x65, 0x61, 0x64,
	0x5f, 0x73, 0x6c, 0x6f, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x68, 0x65, 0x61,
	0x64, 0x53, 0x6c, 0x6f, 0x74, 0x22, 0x23, 0x0a, 0x09, 0x50, 0x65, 0x65, 0x72, 0x43, 0x6f, 0x75,
	0x6e, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x06, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0x37, 0x0a, 0x0b, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x44, 0x61, 0x74, 0x61, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74,
	0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x14, 0x0a,
	0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x74, 0x6f,
	0x70, 0x69, 0x63, 0x22, 0x38, 0x0a, 0x0c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x44,
	0x61, 0x74, 0x61, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x14, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x2a, 0xf9, 0x01,
	0x0a, 0x0a, 0x47, 0x6f, 0x73, 0x73, 0x69, 0x70, 0x54, 0x79, 0x70, 0x65, 0x12, 0x27, 0x0a, 0x23,
	0x4c, 0x69, 0x67, 0x68, 0x74, 0x43, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x46, 0x69, 0x6e, 0x61, 0x6c,
	0x69, 0x74, 0x79, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x47, 0x6f, 0x73, 0x73, 0x69, 0x70, 0x54,
	0x79, 0x70, 0x65, 0x10, 0x00, 0x12, 0x29, 0x0a, 0x25, 0x4c, 0x69, 0x67, 0x68, 0x74, 0x43, 0x6c,
	0x69, 0x65, 0x6e, 0x74, 0x4f, 0x70, 0x74, 0x69, 0x6d, 0x69, 0x73, 0x74, 0x69, 0x63, 0x55, 0x70,
	0x64, 0x61, 0x74, 0x65, 0x47, 0x6f, 0x73, 0x73, 0x69, 0x70, 0x54, 0x79, 0x70, 0x65, 0x10, 0x01,
	0x12, 0x19, 0x0a, 0x15, 0x42, 0x65, 0x61, 0x63, 0x6f, 0x6e, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x47,
	0x6f, 0x73, 0x73, 0x69, 0x70, 0x54, 0x79, 0x70, 0x65, 0x10, 0x02, 0x12, 0x1f, 0x0a, 0x1b, 0x41,
	0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x41, 0x6e, 0x64, 0x50, 0x72, 0x6f, 0x6f, 0x66,
	0x47, 0x6f, 0x73, 0x73, 0x69, 0x70, 0x54, 0x79, 0x70, 0x65, 0x10, 0x03, 0x12, 0x1b, 0x0a, 0x17,
	0x56, 0x6f, 0x6c, 0x75, 0x6e, 0x74, 0x61, 0x72, 0x79, 0x45, 0x78, 0x69, 0x74, 0x47, 0x6f, 0x73,
	0x73, 0x69, 0x70, 0x54, 0x79, 0x70, 0x65, 0x10, 0x04, 0x12, 0x1e, 0x0a, 0x1a, 0x50, 0x72, 0x6f,
	0x70, 0x6f, 0x73, 0x65, 0x72, 0x53, 0x6c, 0x61, 0x73, 0x68, 0x69, 0x6e, 0x67, 0x47, 0x6f, 0x73,
	0x73, 0x69, 0x70, 0x54, 0x79, 0x70, 0x65, 0x10, 0x05, 0x12, 0x1e, 0x0a, 0x1a, 0x41, 0x74, 0x74,
	0x65, 0x73, 0x74, 0x65, 0x72, 0x53, 0x6c, 0x61, 0x73, 0x68, 0x69, 0x6e, 0x67, 0x47, 0x6f, 0x73,
	0x73, 0x69, 0x70, 0x54, 0x79, 0x70, 0x65, 0x10, 0x06, 0x32, 0xfb, 0x01, 0x0a, 0x08, 0x53, 0x65,
	0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x12, 0x41, 0x0a, 0x0f, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72,
	0x69, 0x62, 0x65, 0x47, 0x6f, 0x73, 0x73, 0x69, 0x70, 0x12, 0x16, 0x2e, 0x73, 0x65, 0x6e, 0x74,
	0x69, 0x6e, 0x65, 0x6c, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x1a, 0x14, 0x2e, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x2e, 0x47, 0x6f, 0x73,
	0x73, 0x69, 0x70, 0x44, 0x61, 0x74, 0x61, 0x30, 0x01, 0x12, 0x3c, 0x0a, 0x0b, 0x53, 0x65, 0x6e,
	0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x15, 0x2e, 0x73, 0x65, 0x6e, 0x74, 0x69,
	0x6e, 0x65, 0x6c, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x44, 0x61, 0x74, 0x61, 0x1a,
	0x16, 0x2e, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x44, 0x61, 0x74, 0x61, 0x12, 0x35, 0x0a, 0x09, 0x53, 0x65, 0x74, 0x53, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x12, 0x10, 0x2e, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x2e,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x1a, 0x16, 0x2e, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65,
	0x6c, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x37,
	0x0a, 0x08, 0x47, 0x65, 0x74, 0x50, 0x65, 0x65, 0x72, 0x73, 0x12, 0x16, 0x2e, 0x73, 0x65, 0x6e,
	0x74, 0x69, 0x6e, 0x65, 0x6c, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x1a, 0x13, 0x2e, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x2e, 0x50, 0x65,
	0x65, 0x72, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x42, 0x15, 0x5a, 0x13, 0x2e, 0x2f, 0x73, 0x65, 0x6e,
	0x74, 0x69, 0x6e, 0x65, 0x6c, 0x3b, 0x73, 0x65, 0x6e, 0x74, 0x69, 0x6e, 0x65, 0x6c, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_p2psentinel_sentinel_proto_rawDescOnce sync.Once
	file_p2psentinel_sentinel_proto_rawDescData = file_p2psentinel_sentinel_proto_rawDesc
)

func file_p2psentinel_sentinel_proto_rawDescGZIP() []byte {
	file_p2psentinel_sentinel_proto_rawDescOnce.Do(func() {
		file_p2psentinel_sentinel_proto_rawDescData = protoimpl.X.CompressGZIP(file_p2psentinel_sentinel_proto_rawDescData)
	})
	return file_p2psentinel_sentinel_proto_rawDescData
}

var file_p2psentinel_sentinel_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_p2psentinel_sentinel_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_p2psentinel_sentinel_proto_goTypes = []interface{}{
	(GossipType)(0),      // 0: sentinel.GossipType
	(*EmptyMessage)(nil), // 1: sentinel.EmptyMessage
	(*GossipData)(nil),   // 2: sentinel.GossipData
	(*Status)(nil),       // 3: sentinel.Status
	(*PeerCount)(nil),    // 4: sentinel.PeerCount
	(*RequestData)(nil),  // 5: sentinel.RequestData
	(*ResponseData)(nil), // 6: sentinel.ResponseData
	(*types.H256)(nil),   // 7: types.H256
}
var file_p2psentinel_sentinel_proto_depIdxs = []int32{
	0, // 0: sentinel.GossipData.type:type_name -> sentinel.GossipType
	7, // 1: sentinel.Status.finalized_root:type_name -> types.H256
	7, // 2: sentinel.Status.head_root:type_name -> types.H256
	1, // 3: sentinel.Sentinel.SubscribeGossip:input_type -> sentinel.EmptyMessage
	5, // 4: sentinel.Sentinel.SendRequest:input_type -> sentinel.RequestData
	3, // 5: sentinel.Sentinel.SetStatus:input_type -> sentinel.Status
	1, // 6: sentinel.Sentinel.GetPeers:input_type -> sentinel.EmptyMessage
	2, // 7: sentinel.Sentinel.SubscribeGossip:output_type -> sentinel.GossipData
	6, // 8: sentinel.Sentinel.SendRequest:output_type -> sentinel.ResponseData
	1, // 9: sentinel.Sentinel.SetStatus:output_type -> sentinel.EmptyMessage
	4, // 10: sentinel.Sentinel.GetPeers:output_type -> sentinel.PeerCount
	7, // [7:11] is the sub-list for method output_type
	3, // [3:7] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_p2psentinel_sentinel_proto_init() }
func file_p2psentinel_sentinel_proto_init() {
	if File_p2psentinel_sentinel_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_p2psentinel_sentinel_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EmptyMessage); i {
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
		file_p2psentinel_sentinel_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GossipData); i {
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
		file_p2psentinel_sentinel_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Status); i {
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
		file_p2psentinel_sentinel_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PeerCount); i {
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
		file_p2psentinel_sentinel_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestData); i {
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
		file_p2psentinel_sentinel_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ResponseData); i {
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
			RawDescriptor: file_p2psentinel_sentinel_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_p2psentinel_sentinel_proto_goTypes,
		DependencyIndexes: file_p2psentinel_sentinel_proto_depIdxs,
		EnumInfos:         file_p2psentinel_sentinel_proto_enumTypes,
		MessageInfos:      file_p2psentinel_sentinel_proto_msgTypes,
	}.Build()
	File_p2psentinel_sentinel_proto = out.File
	file_p2psentinel_sentinel_proto_rawDesc = nil
	file_p2psentinel_sentinel_proto_goTypes = nil
	file_p2psentinel_sentinel_proto_depIdxs = nil
}
