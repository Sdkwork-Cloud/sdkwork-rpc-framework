use async_trait::async_trait;
use sdkwork_rpc_framework_core::RpcFrameworkError;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedEndpoint {
    pub endpoint: String,
    pub weight: u32,
    pub healthy: bool,
}

#[async_trait]
pub trait NameResolver: Send + Sync {
    async fn resolve(&self, service_name: &str)
        -> Result<Vec<ResolvedEndpoint>, RpcFrameworkError>;
}

#[derive(Clone)]
pub struct CompositeNameResolver {
    pub primary: Arc<dyn NameResolver>,
    pub fallback: StaticNameResolver,
}

impl CompositeNameResolver {
    pub fn new(primary: Arc<dyn NameResolver>, fallback: StaticNameResolver) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl NameResolver for CompositeNameResolver {
    async fn resolve(
        &self,
        service_name: &str,
    ) -> Result<Vec<ResolvedEndpoint>, RpcFrameworkError> {
        match self.primary.resolve(service_name).await {
            Ok(endpoints) if !endpoints.is_empty() => Ok(endpoints),
            _ => self.fallback.resolve(service_name).await,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticNameResolver {
    pub endpoints: Vec<ResolvedEndpoint>,
}

impl StaticNameResolver {
    pub fn single(endpoint: impl Into<String>) -> Self {
        Self {
            endpoints: vec![ResolvedEndpoint {
                endpoint: endpoint.into(),
                weight: 100,
                healthy: true,
            }],
        }
    }
}

#[async_trait]
impl NameResolver for StaticNameResolver {
    async fn resolve(
        &self,
        _service_name: &str,
    ) -> Result<Vec<ResolvedEndpoint>, RpcFrameworkError> {
        Ok(self.endpoints.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_resolver_returns_configured_endpoint() {
        let resolver = StaticNameResolver::single("grpc://127.0.0.1:50051");
        let endpoints = resolver
            .resolve("sdkwork-commerce-app-rpc")
            .await
            .expect("static resolver");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].endpoint, "grpc://127.0.0.1:50051");
    }
}
