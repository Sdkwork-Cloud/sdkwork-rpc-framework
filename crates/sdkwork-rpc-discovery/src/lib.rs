//! SDKWork discovery registration lifecycle for RPC data-plane servers.

mod endpoint;
mod lifecycle;
mod metadata;
mod rpc_metadata;

pub use endpoint::{normalize_grpc_endpoint, normalize_http_endpoint, require_non_blank};

pub use lifecycle::{
    default_instance_id, grpc_advertised_endpoint, DiscoveryInstanceConfig,
    DiscoveryInstanceHandle, DiscoveryInstanceLifecycle, DiscoveryRegistrationError,
};
pub use metadata::{build_registration_metadata, RegistrationMetadataInput};
pub use rpc_metadata::{
    apply_metadata_template, unsigned_registry_read_metadata, unsigned_registry_write_metadata,
};
