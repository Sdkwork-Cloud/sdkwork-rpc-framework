//! SDKWork RPC framework core primitives.

pub use sdkwork_rpc_core_rust::{
    map_error_kind_to_code, validate_manifest, SdkworkRpcErrorKind, SdkworkRpcMethod,
    SdkworkRpcServiceManifest, RPC_ACCESS_TOKEN_METADATA, RPC_AUTHORIZATION_METADATA,
    RPC_IDEMPOTENCY_KEY_METADATA, RPC_REQUEST_HASH_METADATA, RPC_REQUEST_ID_METADATA,
    RPC_TRACEPARENT_METADATA,
};

mod bootstrap;
mod caller_context;
mod identity;
mod profile;
mod service_identity;
mod surface;

pub use bootstrap::{
    BOOTSTRAP_STAGES, SHUTDOWN_STAGES, STAGE_BIND_RPC_SERVICES,
    STAGE_DEREGISTER_DISCOVERY_INSTANCE, STAGE_DRAIN_RPC_SERVERS, STAGE_INITIALIZE_RPC_FRAMEWORK,
    STAGE_REGISTER_DISCOVERY_INSTANCE, STAGE_START_DISCOVERY_RENEW_LOOP,
    STAGE_STOP_DISCOVERY_RENEW_LOOP, STAGE_VALIDATE_RPC_CONTRACTS,
};
pub use caller_context::{
    RpcCallerActorKind, RpcCallerContext, RpcCallerContextBuilder, RpcCallerContextSigner,
    RpcCallerContextSigningKey, RpcCallerContextVerifier, SignedRpcCallerContext,
    VerifiedRpcCallerContext, RPC_CALLER_CONTEXT_METADATA, RPC_CALLER_CONTEXT_SIGNATURE_METADATA,
    RPC_SERVICE_IDENTITY_ASSERTION_METADATA,
};
pub use identity::{build_rpc_identity_uri, RpcIdentityParts};
pub use profile::{ResilienceProfile, ResolverProfile};
pub use service_identity::{
    RpcServiceIdentityPolicy, VerifiedRpcServiceIdentity, SPIFFE_SERVICE_PATH_PREFIX,
};
pub use surface::{validate_rpc_surface, RpcSurface};

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum RpcFrameworkError {
    /// Caller-supplied input failed validation (blank fields, malformed URIs).
    /// Not retryable without correcting the input.
    #[error("validation failed: {0}")]
    Validation(String),
    /// Framework or deployment configuration is invalid or incomplete (missing
    /// TLS feature, unreadable cert paths, malformed channel config). Not
    /// retryable without correcting the configuration.
    #[error("configuration error: {0}")]
    Configuration(String),
    /// Transport-level failure: TCP connection refused, TLS handshake failure,
    /// HTTP/2 protocol error. Often transient and retryable.
    #[error("transport error: {0}")]
    Transport(String),
    /// Service discovery failure: no healthy instances resolved, discover RPC
    /// returned an error, or the watch stream failed. May be retryable after
    /// a backoff if the control plane or target service recovers.
    #[error("discovery error: {0}")]
    Discovery(String),
}

pub type RpcFrameworkResult<T> = Result<T, RpcFrameworkError>;
