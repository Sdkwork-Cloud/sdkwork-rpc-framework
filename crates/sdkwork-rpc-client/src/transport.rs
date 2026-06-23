//! Shared gRPC transport helpers for framework client factories.

use std::time::Duration;

use sdkwork_rpc_framework_core::RpcFrameworkError;
use sdkwork_utils_rust::{is_blank, trim};
use tonic::transport::{Channel, Endpoint};

use crate::load_balance::{pick_endpoint, LoadBalanceAlgorithm, RoundRobinCursor};
use crate::resolver::NameResolver;

/// Production-oriented gRPC channel defaults aligned with gRPC keepalive guidance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrpcChannelConfig {
    pub connect_timeout: Duration,
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
    pub keepalive_while_idle: bool,
}

impl Default for GrpcChannelConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            keepalive_while_idle: true,
        }
    }
}

/// Converts a resolver `grpc://` or `grpcs://` endpoint into a tonic-compatible URI.
pub fn tonic_endpoint_uri(grpc_endpoint: &str) -> Result<String, RpcFrameworkError> {
    let trimmed = trim(grpc_endpoint);
    if is_blank(Some(&trimmed)) {
        return Err(RpcFrameworkError::Validation(
            "grpc endpoint is required".to_string(),
        ));
    }

    if let Some(rest) = trimmed.strip_prefix("grpcs://") {
        return Ok(format!("https://{rest}"));
    }
    if let Some(rest) = trimmed.strip_prefix("grpc://") {
        return Ok(format!("http://{rest}"));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(trimmed);
    }

    Ok(format!("http://{trimmed}"))
}

/// Connects a tonic channel to a resolved gRPC data-plane endpoint.
pub async fn connect_grpc_channel(endpoint: &str) -> Result<Channel, RpcFrameworkError> {
    connect_grpc_channel_with_config(endpoint, &GrpcChannelConfig::default()).await
}

/// Connects a tonic channel using explicit keepalive and timeout settings.
pub async fn connect_grpc_channel_with_config(
    endpoint: &str,
    config: &GrpcChannelConfig,
) -> Result<Channel, RpcFrameworkError> {
    let uri = tonic_endpoint_uri(endpoint)?;
    Endpoint::from_shared(uri)
        .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))?
        .connect_timeout(config.connect_timeout)
        .http2_keep_alive_interval(config.keepalive_interval)
        .keep_alive_timeout(config.keepalive_timeout)
        .keep_alive_while_idle(config.keepalive_while_idle)
        .connect()
        .await
        .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))
}

/// Resolves, load-balances, and connects a shared tonic channel.
pub async fn resolve_and_connect(
    resolver: &dyn NameResolver,
    service_name: &str,
    algorithm: LoadBalanceAlgorithm,
    cursor: &mut RoundRobinCursor,
    config: &GrpcChannelConfig,
) -> Result<Channel, RpcFrameworkError> {
    let endpoints = resolver.resolve(service_name).await?;
    let picked = pick_endpoint(&endpoints, algorithm, cursor).ok_or_else(|| {
        RpcFrameworkError::Configuration(format!(
            "no endpoints resolved for service {service_name}"
        ))
    })?;
    connect_grpc_channel_with_config(&picked.endpoint, config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_grpc_scheme_to_http_for_tonic() {
        assert_eq!(
            tonic_endpoint_uri("grpc://127.0.0.1:50051").expect("uri"),
            "http://127.0.0.1:50051"
        );
        assert_eq!(
            tonic_endpoint_uri("grpcs://example.com:443").expect("uri"),
            "https://example.com:443"
        );
    }

    #[test]
    fn rejects_blank_endpoint() {
        assert!(tonic_endpoint_uri("  ").is_err());
    }

    #[test]
    fn default_channel_config_uses_keepalive() {
        let config = GrpcChannelConfig::default();
        assert!(config.keepalive_while_idle);
        assert!(config.keepalive_interval >= Duration::from_secs(30));
    }
}
