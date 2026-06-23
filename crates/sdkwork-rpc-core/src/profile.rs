#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolverProfile {
    Static,
    StaticComposite,
    Discovery,
    Composite,
}

impl ResolverProfile {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "static" => Some(Self::Static),
            "static-composite" => Some(Self::StaticComposite),
            "discovery" => Some(Self::Discovery),
            "composite" => Some(Self::Composite),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResilienceProfile {
    RpcDefault,
    RpcReadOnly,
    RpcIdempotentWrite,
    RpcCriticalWrite,
    RpcStream,
    RpcLocalDev,
}

impl ResilienceProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RpcDefault => "rpc-default",
            Self::RpcReadOnly => "rpc-read-only",
            Self::RpcIdempotentWrite => "rpc-idempotent-write",
            Self::RpcCriticalWrite => "rpc-critical-write",
            Self::RpcStream => "rpc-stream",
            Self::RpcLocalDev => "rpc-local-dev",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "rpc-default" => Some(Self::RpcDefault),
            "rpc-read-only" => Some(Self::RpcReadOnly),
            "rpc-idempotent-write" => Some(Self::RpcIdempotentWrite),
            "rpc-critical-write" => Some(Self::RpcCriticalWrite),
            "rpc-stream" => Some(Self::RpcStream),
            "rpc-local-dev" => Some(Self::RpcLocalDev),
            _ => None,
        }
    }

    /// Returns false for profiles that must not be used in production deployments.
    pub fn is_production_safe(self) -> bool {
        !matches!(self, Self::RpcLocalDev)
    }
}
