//! SDKWork RPC client resolver and invocation context.

mod discovery_resolver;
mod load_balance;
mod metadata;
mod resolver;
mod retry_budget;
mod tls;
mod transport;
mod watching_discovery_resolver;

pub use discovery_resolver::{DiscoveryNameResolver, DiscoveryNameResolverConfig};
pub use load_balance::{pick_endpoint, LoadBalanceAlgorithm, RoundRobinCursor};
pub use metadata::RpcCallMetadata;
pub use resolver::{CompositeNameResolver, NameResolver, ResolvedEndpoint, StaticNameResolver};
pub use retry_budget::{RetryBudgetConfig, RetryBudgetRegistry};
pub use tls::RpcTlsConfig;
pub use transport::{
    connect_grpc_channel, connect_grpc_channel_with_config, resolve_and_connect,
    tonic_endpoint_uri, GrpcChannelConfig,
};
pub use watching_discovery_resolver::{WatchingDiscoveryNameResolver, WatchLoopConfig};

use sdkwork_rpc_framework_core::{ResilienceProfile, ResolverProfile};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcInvocationContext {
    pub resolver_profile: ResolverProfile,
    pub resilience_profile: ResilienceProfile,
    pub default_deadline_ms: u64,
}

impl Default for RpcInvocationContext {
    fn default() -> Self {
        Self {
            resolver_profile: ResolverProfile::Static,
            resilience_profile: ResilienceProfile::RpcDefault,
            default_deadline_ms: 30_000,
        }
    }
}
