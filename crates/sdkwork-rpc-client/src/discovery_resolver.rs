use async_trait::async_trait;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::common::v1::InstanceStatus;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::internal::v1::registry_service_client::RegistryServiceClient;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::internal::v1::DiscoverInstancesRequest;
use sdkwork_rpc_discovery::{
    apply_metadata_template, normalize_http_endpoint, require_non_blank,
    unsigned_registry_read_metadata,
};
use sdkwork_rpc_framework_core::RpcFrameworkError;
use sdkwork_utils_rust::is_blank;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::resolver::{NameResolver, ResolvedEndpoint};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryNameResolverConfig {
    pub discovery_endpoint: String,
    pub namespace: String,
    pub environment: String,
    pub subject_id: String,
    pub healthy_only: bool,
    pub protocol: String,
}

impl DiscoveryNameResolverConfig {
    pub fn validate(&self) -> Result<(), RpcFrameworkError> {
        require_non_blank(&self.discovery_endpoint, "discovery_endpoint")
            .map_err(RpcFrameworkError::Validation)?;
        require_non_blank(&self.namespace, "namespace").map_err(RpcFrameworkError::Validation)?;
        require_non_blank(&self.environment, "environment").map_err(RpcFrameworkError::Validation)?;
        require_non_blank(&self.subject_id, "subject_id").map_err(RpcFrameworkError::Validation)?;
        require_non_blank(&self.protocol, "protocol").map_err(RpcFrameworkError::Validation)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DiscoveryNameResolver {
    config: DiscoveryNameResolverConfig,
}

impl DiscoveryNameResolver {
    pub fn new(config: DiscoveryNameResolverConfig) -> Result<Self, RpcFrameworkError> {
        config.validate()?;
        Ok(Self { config })
    }

    pub fn config(&self) -> &DiscoveryNameResolverConfig {
        &self.config
    }

    async fn connect(&self) -> Result<RegistryServiceClient<Channel>, RpcFrameworkError> {
        let endpoint = normalize_http_endpoint(&self.config.discovery_endpoint);
        let channel = Endpoint::from_shared(endpoint)
            .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))?
            .connect()
            .await
            .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))?;
        Ok(RegistryServiceClient::new(channel))
    }

    pub(crate) async fn discover_endpoints(
        &self,
        service_name: &str,
    ) -> Result<Vec<ResolvedEndpoint>, RpcFrameworkError> {
        let mut client = self.connect().await?;
        let mut grpc_request = Request::new(DiscoverInstancesRequest {
            namespace: self.config.namespace.clone(),
            environment: self.config.environment.clone(),
            service_name: service_name.to_string(),
            healthy_only: self.config.healthy_only,
            protocol: self.config.protocol.clone(),
            label_filters: Vec::new(),
            sort_by: 0,
            page: None,
        });
        apply_metadata_template(
            grpc_request.metadata_mut(),
            &unsigned_registry_read_metadata(&self.config.subject_id),
        );

        let response = client
            .discover_instances(grpc_request)
            .await
            .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))?
            .into_inner();

        let endpoints: Vec<ResolvedEndpoint> = response
            .instances
            .into_iter()
            .filter_map(|instance| map_discovered_instance(&instance))
            .filter(|endpoint| endpoint.healthy)
            .collect();

        if endpoints.is_empty() {
            return Err(RpcFrameworkError::Configuration(format!(
                "no healthy instances resolved for service {service_name}"
            )));
        }

        Ok(endpoints)
    }
}

#[async_trait]
impl NameResolver for DiscoveryNameResolver {
    async fn resolve(
        &self,
        service_name: &str,
    ) -> Result<Vec<ResolvedEndpoint>, RpcFrameworkError> {
        if is_blank(Some(service_name)) {
            return Err(RpcFrameworkError::Validation(
                "service_name is required".to_string(),
            ));
        }

        self.discover_endpoints(service_name).await
    }
}

pub(crate) fn map_discovered_instance(
    instance: &sdkwork_discovery_rpc_proto::sdkwork::discovery::common::v1::ServiceInstance,
) -> Option<ResolvedEndpoint> {
    if is_blank(Some(&instance.endpoint)) {
        return None;
    }

    let healthy = matches!(
        InstanceStatus::try_from(instance.status).ok(),
        Some(InstanceStatus::Serving) | Some(InstanceStatus::Degraded)
    );

    Some(ResolvedEndpoint {
        endpoint: instance.endpoint.clone(),
        weight: instance.weight.max(1),
        healthy,
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_resolver_config_requires_endpoint() {
        let config = DiscoveryNameResolverConfig {
            discovery_endpoint: String::new(),
            namespace: "sdkwork".to_string(),
            environment: "development".to_string(),
            subject_id: "resolver-1".to_string(),
            healthy_only: true,
            protocol: "grpc".to_string(),
        };
        assert!(DiscoveryNameResolver::new(config).is_err());
    }

    #[tokio::test]
    async fn discovery_resolver_rejects_blank_service_name() {
        let resolver = DiscoveryNameResolver::new(DiscoveryNameResolverConfig {
            discovery_endpoint: "http://127.0.0.1:50051".to_string(),
            namespace: "sdkwork".to_string(),
            environment: "development".to_string(),
            subject_id: "resolver-1".to_string(),
            healthy_only: true,
            protocol: "grpc".to_string(),
        })
        .expect("config");

        let error = resolver
            .resolve("  ")
            .await
            .expect_err("blank service name");
        assert!(matches!(error, RpcFrameworkError::Validation(_)));
    }
}
