use std::collections::BTreeMap;
use std::sync::Arc;

use sdkwork_discovery_rpc_proto::sdkwork::discovery::common::v1::InstanceStatus;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::internal::v1::discovery_watch_service_client::DiscoveryWatchServiceClient;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::internal::v1::WatchServiceRequest;
use sdkwork_rpc_discovery::{apply_metadata_template, normalize_http_endpoint, unsigned_registry_read_metadata};
use sdkwork_rpc_framework_core::RpcFrameworkError;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::transport::Endpoint;
use tonic::Request;
use tracing::{info, warn};

use crate::discovery_resolver::{
    map_discovered_instance, DiscoveryNameResolver, DiscoveryNameResolverConfig,
};
use crate::resolver::{NameResolver, ResolvedEndpoint};
use async_trait::async_trait;

#[derive(Clone, Debug)]
pub struct WatchingDiscoveryNameResolver {
    inner: DiscoveryNameResolver,
    cache: Arc<RwLock<BTreeMap<String, Vec<ResolvedEndpoint>>>>,
}

impl WatchingDiscoveryNameResolver {
    pub fn new(config: DiscoveryNameResolverConfig) -> Result<Self, RpcFrameworkError> {
        Ok(Self {
            inner: DiscoveryNameResolver::new(config)?,
            cache: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    pub fn spawn_watch_loop(self: &Arc<Self>, service_name: impl Into<String>) -> JoinHandle<()> {
        let service_name = service_name.into();
        let resolver = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(error) = resolver.run_watch_loop(&service_name).await {
                warn!(
                    service_name = %service_name,
                    error = %error,
                    "discovery watch loop exited"
                );
            }
        })
    }

    async fn run_watch_loop(&self, service_name: &str) -> Result<(), RpcFrameworkError> {
        let config = self.inner.config();
        let endpoint = normalize_http_endpoint(&config.discovery_endpoint);
        let channel = Endpoint::from_shared(endpoint)
            .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))?
            .connect()
            .await
            .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))?;

        let mut client = DiscoveryWatchServiceClient::new(channel);
        let mut request = Request::new(WatchServiceRequest {
            namespace: config.namespace.clone(),
            environment: config.environment.clone(),
            service_name: service_name.to_string(),
            from_revision: 0,
        });
        apply_metadata_template(
            request.metadata_mut(),
            &unsigned_registry_read_metadata(&config.subject_id),
        );

        let mut stream = client
            .watch_service(request)
            .await
            .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))?
            .into_inner();

        info!(service_name = %service_name, "started discovery watch loop");

        while let Some(message) = stream
            .message()
            .await
            .map_err(|error| RpcFrameworkError::Configuration(error.to_string()))?
        {
            if let Some(instance) = message.instance {
                let endpoint = map_discovered_instance(&instance);
                let mut cache = self.cache.write().await;
                let entry = cache.entry(service_name.to_string()).or_default();
                match InstanceStatus::try_from(instance.status).ok() {
                    Some(InstanceStatus::Serving) | Some(InstanceStatus::Degraded) => {
                        if let Some(endpoint) = endpoint {
                            upsert_endpoint(entry, endpoint);
                        }
                    }
                    _ => {
                        entry.retain(|existing| existing.endpoint != instance.endpoint);
                        if entry.is_empty() {
                            cache.remove(service_name);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn cached_endpoints(&self, service_name: &str) -> Option<Vec<ResolvedEndpoint>> {
        let cache = self.cache.read().await;
        let endpoints = cache.get(service_name)?;
        if endpoints.is_empty() {
            None
        } else {
            Some(endpoints.clone())
        }
    }
}

#[async_trait]
impl NameResolver for WatchingDiscoveryNameResolver {
    async fn resolve(
        &self,
        service_name: &str,
    ) -> Result<Vec<ResolvedEndpoint>, RpcFrameworkError> {
        if let Some(endpoints) = self.cached_endpoints(service_name).await {
            return Ok(endpoints);
        }

        let endpoints = self.inner.resolve(service_name).await?;
        let mut cache = self.cache.write().await;
        cache.insert(service_name.to_string(), endpoints.clone());
        Ok(endpoints)
    }
}

fn upsert_endpoint(endpoints: &mut Vec<ResolvedEndpoint>, endpoint: ResolvedEndpoint) {
    if let Some(existing) = endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint == endpoint.endpoint)
    {
        *existing = endpoint;
        return;
    }
    endpoints.push(endpoint);
}