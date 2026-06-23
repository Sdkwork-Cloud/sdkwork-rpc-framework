//! SDKWork RPC framework core primitives.

pub use sdkwork_rpc_core_rust::{
    map_error_kind_to_code, validate_manifest, SdkworkRpcErrorKind, SdkworkRpcMethod,
    SdkworkRpcServiceManifest, RPC_ACCESS_TOKEN_METADATA, RPC_AUTHORIZATION_METADATA,
    RPC_IDEMPOTENCY_KEY_METADATA, RPC_REQUEST_HASH_METADATA, RPC_REQUEST_ID_METADATA,
    RPC_TRACEPARENT_METADATA,
};

mod identity;
mod profile;
mod bootstrap;
mod surface;

pub use bootstrap::{
    BOOTSTRAP_STAGES, SHUTDOWN_STAGES, STAGE_BIND_RPC_SERVICES,
    STAGE_DEREGISTER_DISCOVERY_INSTANCE, STAGE_DRAIN_RPC_SERVERS,
    STAGE_INITIALIZE_RPC_FRAMEWORK, STAGE_REGISTER_DISCOVERY_INSTANCE,
    STAGE_START_DISCOVERY_RENEW_LOOP, STAGE_STOP_DISCOVERY_RENEW_LOOP,
    STAGE_VALIDATE_RPC_CONTRACTS,
};
pub use identity::{build_rpc_identity_uri, RpcIdentityParts};
pub use profile::{ResilienceProfile, ResolverProfile};
pub use surface::{validate_rpc_surface, RpcSurface};

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum RpcFrameworkError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("configuration error: {0}")]
    Configuration(String),
}

pub type RpcFrameworkResult<T> = Result<T, RpcFrameworkError>;
