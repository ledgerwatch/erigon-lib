#[derive(Clone, PartialEq, ::prost::Message)]
pub struct H128 {
    #[prost(uint64, tag="1")]
    pub hi: u64,
    #[prost(uint64, tag="2")]
    pub lo: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct H160 {
    #[prost(message, optional, tag="1")]
    pub hi: ::core::option::Option<H128>,
    #[prost(uint32, tag="2")]
    pub lo: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct H256 {
    #[prost(message, optional, tag="1")]
    pub hi: ::core::option::Option<H128>,
    #[prost(message, optional, tag="2")]
    pub lo: ::core::option::Option<H128>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct H512 {
    #[prost(message, optional, tag="1")]
    pub hi: ::core::option::Option<H256>,
    #[prost(message, optional, tag="2")]
    pub lo: ::core::option::Option<H256>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct H1024 {
    #[prost(message, optional, tag="1")]
    pub hi: ::core::option::Option<H512>,
    #[prost(message, optional, tag="2")]
    pub lo: ::core::option::Option<H512>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct H2048 {
    #[prost(message, optional, tag="1")]
    pub hi: ::core::option::Option<H1024>,
    #[prost(message, optional, tag="2")]
    pub lo: ::core::option::Option<H1024>,
}
/// Reply message containing the current service version on the service side
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VersionReply {
    #[prost(uint32, tag="1")]
    pub major: u32,
    #[prost(uint32, tag="2")]
    pub minor: u32,
    #[prost(uint32, tag="3")]
    pub patch: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionPayload {
    #[prost(message, optional, tag="1")]
    pub parent_hash: ::core::option::Option<H256>,
    #[prost(message, optional, tag="2")]
    pub coinbase: ::core::option::Option<H160>,
    #[prost(message, optional, tag="3")]
    pub state_root: ::core::option::Option<H256>,
    #[prost(message, optional, tag="4")]
    pub receipt_root: ::core::option::Option<H256>,
    #[prost(message, optional, tag="5")]
    pub logs_bloom: ::core::option::Option<H2048>,
    #[prost(message, optional, tag="6")]
    pub prev_randao: ::core::option::Option<H256>,
    #[prost(uint64, tag="7")]
    pub block_number: u64,
    #[prost(uint64, tag="8")]
    pub gas_limit: u64,
    #[prost(uint64, tag="9")]
    pub gas_used: u64,
    #[prost(uint64, tag="10")]
    pub timestamp: u64,
    #[prost(bytes="bytes", tag="11")]
    pub extra_data: ::prost::bytes::Bytes,
    #[prost(message, optional, tag="12")]
    pub base_fee_per_gas: ::core::option::Option<H256>,
    #[prost(message, optional, tag="13")]
    pub block_hash: ::core::option::Option<H256>,
    ///
    ///Array of transaction objects, each object is a byte list. 
    ///See <https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.2/src/engine/interop/specification.md>
    #[prost(bytes="bytes", repeated, tag="14")]
    pub transactions: ::prost::alloc::vec::Vec<::prost::bytes::Bytes>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeInfoPorts {
    #[prost(uint32, tag="1")]
    pub discovery: u32,
    #[prost(uint32, tag="2")]
    pub listener: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeInfoReply {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub enode: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub enr: ::prost::alloc::string::String,
    #[prost(message, optional, tag="5")]
    pub ports: ::core::option::Option<NodeInfoPorts>,
    #[prost(string, tag="6")]
    pub listener_addr: ::prost::alloc::string::String,
    #[prost(bytes="bytes", tag="7")]
    pub protocols: ::prost::bytes::Bytes,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerInfo {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub enode: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub enr: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="5")]
    pub caps: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="6")]
    pub conn_local_addr: ::prost::alloc::string::String,
    #[prost(string, tag="7")]
    pub conn_remote_addr: ::prost::alloc::string::String,
    #[prost(bool, tag="8")]
    pub conn_is_inbound: bool,
    #[prost(bool, tag="9")]
    pub conn_is_trusted: bool,
    #[prost(bool, tag="10")]
    pub conn_is_static: bool,
}
