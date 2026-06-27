use std::collections::BTreeMap;
use std::time::Duration;

use crate::endpoint::{normalize_grpc_endpoint, normalize_http_endpoint, require_non_blank};
use crate::rpc_metadata::{
    apply_metadata_template, unsigned_registry_read_metadata, unsigned_registry_write_metadata,
};
use rand::Rng;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::common::v1::InstanceStatus;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::internal::v1::registry_service_client::RegistryServiceClient;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::internal::v1::{
    DeregisterInstanceRequest, RegisterInstanceRequest, RenewLeaseRequest, RetrieveInstanceRequest,
};
use sdkwork_utils_rust::is_blank;
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryInstanceConfig {
    pub discovery_endpoint: String,
    pub namespace: String,
    pub environment: String,
    pub service_name: String,
    pub instance_id: String,
    pub advertised_endpoint: String,
    pub protocol: String,
    pub version: String,
    pub region: String,
    pub zone: String,
    pub lease_ttl_seconds: u64,
    pub subject_id: String,
    pub metadata: BTreeMap<String, String>,
    /// When true, retrieve the current instance revision before register and pass CAS `expected_revision`.
    pub revision_cas_on_register: bool,
    /// Explicit revision for CAS. When `Some`, overrides auto-retrieval.
    pub expected_revision: Option<u64>,
}

impl DiscoveryInstanceConfig {
    pub fn validate(&self) -> Result<(), DiscoveryRegistrationError> {
        require_non_empty(&self.discovery_endpoint, "discovery_endpoint")?;
        require_non_empty(&self.namespace, "namespace")?;
        require_non_empty(&self.environment, "environment")?;
        require_non_empty(&self.service_name, "service_name")?;
        require_non_empty(&self.instance_id, "instance_id")?;
        require_non_empty(&self.advertised_endpoint, "advertised_endpoint")?;
        require_non_empty(&self.protocol, "protocol")?;
        require_non_empty(&self.version, "version")?;
        require_non_empty(&self.region, "region")?;
        require_non_empty(&self.zone, "zone")?;
        require_non_empty(&self.subject_id, "subject_id")?;
        if self.lease_ttl_seconds == 0 {
            return Err(DiscoveryRegistrationError::Validation(
                "lease_ttl_seconds must be greater than zero".to_string(),
            ));
        }
        if !self.metadata.contains_key("rpc_surface") {
            return Err(DiscoveryRegistrationError::Validation(
                "metadata.rpc_surface is required".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum DiscoveryRegistrationError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("transport failed: {0}")]
    Transport(String),
    #[error("discovery rpc failed: {0}")]
    Rpc(String),
}

/// Tunable parameters for the lease renewal loop backoff and alerting.
///
/// When renewal fails, the loop switches from the steady-state
/// `renew_interval` to exponential backoff with Full Jitter (Google SRE) so a
/// down control plane is not hammered. After `max_consecutive_failures` the
/// loop logs a critical error because the lease has likely expired and the
/// instance will be evicted from the discovery registry.
///
/// Defaults are conservative: 1s initial, capped at the renew interval, and 3
/// consecutive failures before critical alert. This ensures several retry
/// attempts fit within a typical lease TTL (e.g., 30s).
#[derive(Clone, Debug)]
pub struct RenewLoopConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_consecutive_failures: u32,
}

impl Default for RenewLoopConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(10),
            max_consecutive_failures: 3,
        }
    }
}

pub struct DiscoveryInstanceHandle {
    client: RegistryServiceClient<Channel>,
    config: DiscoveryInstanceConfig,
    lease_id: String,
    renew_interval: Duration,
    metadata_template: Vec<(String, String)>,
}

pub struct DiscoveryInstanceLifecycle;

impl DiscoveryInstanceLifecycle {
    pub async fn register(
        config: DiscoveryInstanceConfig,
    ) -> Result<DiscoveryInstanceHandle, DiscoveryRegistrationError> {
        config.validate()?;

        let channel = connect(&config.discovery_endpoint).await?;
        let mut client = RegistryServiceClient::new(channel);
        let metadata_template = unsigned_registry_write_metadata(&config.subject_id);
        let expected_revision = resolve_expected_revision(
            &mut client,
            &config,
            &config.subject_id,
        )
        .await?;

        let request = RegisterInstanceRequest {
            namespace: config.namespace.clone(),
            environment: config.environment.clone(),
            service_name: config.service_name.clone(),
            instance_id: config.instance_id.clone(),
            endpoint: config.advertised_endpoint.clone(),
            protocol: config.protocol.clone(),
            version: config.version.clone(),
            region: config.region.clone(),
            zone: config.zone.clone(),
            weight: 100,
            priority: 0,
            status: InstanceStatus::Serving as i32,
            metadata: config
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            lease_ttl_seconds: config.lease_ttl_seconds,
            expected_revision,
            persistent: false,
            health_check: None,
        };

        let mut grpc_request = Request::new(request);
        apply_metadata_template(grpc_request.metadata_mut(), &metadata_template);

        let response = client
            .register_instance(grpc_request)
            .await
            .map_err(map_status)?
            .into_inner();

        let lease_id = response.lease_id;
        if is_blank(Some(&lease_id)) {
            return Err(DiscoveryRegistrationError::Rpc(
                "register_instance returned empty lease_id".to_string(),
            ));
        }

        let renew_interval = Duration::from_secs(config.lease_ttl_seconds.max(3) / 3);

        info!(
            service_name = %config.service_name,
            instance_id = %config.instance_id,
            lease_id = %lease_id,
            "registered rpc instance with sdkwork-discovery"
        );

        Ok(DiscoveryInstanceHandle {
            client,
            config,
            lease_id,
            renew_interval,
            metadata_template,
        })
    }
}

impl DiscoveryInstanceHandle {
    /// Spawns the lease renewal loop with default backoff config.
    ///
    /// See [`spawn_renew_loop_with_config`] for backoff behavior.
    pub fn spawn_renew_loop(self: &std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.spawn_renew_loop_with_config(RenewLoopConfig::default())
    }

    /// Spawns the lease renewal loop with explicit backoff tunables.
    ///
    /// In steady state the loop sleeps for `renew_interval` between renewals.
    /// On failure it switches to exponential backoff with Full Jitter so a
    /// down control plane is not hammered. After `max_consecutive_failures`
    /// consecutive failures, the loop logs a critical error because the lease
    /// has likely expired and the instance will be evicted.
    pub fn spawn_renew_loop_with_config(
        self: &std::sync::Arc<Self>,
        renew_config: RenewLoopConfig,
    ) -> tokio::task::JoinHandle<()> {
        let handle = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            let mut failures: u32 = 0;
            loop {
                if failures == 0 {
                    // Steady state: sleep for the normal renew interval.
                    tokio::time::sleep(handle.renew_interval).await;
                } else {
                    // Failure backoff: exponential with Full Jitter, capped at
                    // max_backoff (which should not exceed renew_interval).
                    let backoff = renew_backoff(
                        failures,
                        renew_config.initial_backoff,
                        renew_config.max_backoff,
                    );
                    tokio::time::sleep(backoff).await;
                }

                match handle.renew_once().await {
                    Ok(()) => {
                        if failures > 0 {
                            info!(
                                target: "sdkwork.rpc.discovery.renew",
                                service_name = %handle.config.service_name,
                                instance_id = %handle.config.instance_id,
                                previous_failures = failures,
                                "discovery lease renewal recovered after failures"
                            );
                        }
                        failures = 0;
                    }
                    Err(error) => {
                        failures = failures.saturating_add(1);
                        if failures >= renew_config.max_consecutive_failures {
                            error!(
                                target: "sdkwork.rpc.discovery.renew",
                                service_name = %handle.config.service_name,
                                instance_id = %handle.config.instance_id,
                                consecutive_failures = failures,
                                lease_ttl_seconds = handle.config.lease_ttl_seconds,
                                error = %error,
                                "discovery lease renewal has failed beyond threshold; \
                                 lease likely expired and instance will be evicted"
                            );
                        } else {
                            warn!(
                                target: "sdkwork.rpc.discovery.renew",
                                service_name = %handle.config.service_name,
                                instance_id = %handle.config.instance_id,
                                consecutive_failures = failures,
                                error = %error,
                                "discovery lease renewal failed, backing off"
                            );
                        }
                    }
                }
            }
        })
    }

    pub async fn renew_once(&self) -> Result<(), DiscoveryRegistrationError> {
        let mut grpc_request = Request::new(RenewLeaseRequest {
            lease_id: self.lease_id.clone(),
            lease_ttl_seconds: self.config.lease_ttl_seconds,
        });
        apply_metadata_template(grpc_request.metadata_mut(), &self.metadata_template);
        self.client
            .clone()
            .renew_lease(grpc_request)
            .await
            .map_err(map_status)?;
        Ok(())
    }

    pub async fn deregister(&self) -> Result<(), DiscoveryRegistrationError> {
        let mut grpc_request = Request::new(DeregisterInstanceRequest {
            namespace: self.config.namespace.clone(),
            environment: self.config.environment.clone(),
            service_name: self.config.service_name.clone(),
            instance_id: self.config.instance_id.clone(),
        });
        apply_metadata_template(grpc_request.metadata_mut(), &self.metadata_template);
        self.client
            .clone()
            .deregister_instance(grpc_request)
            .await
            .map_err(map_status)?;

        info!(
            service_name = %self.config.service_name,
            instance_id = %self.config.instance_id,
            "deregistered rpc instance from sdkwork-discovery"
        );
        Ok(())
    }
}

async fn resolve_expected_revision(
    client: &mut RegistryServiceClient<Channel>,
    config: &DiscoveryInstanceConfig,
    subject_id: &str,
) -> Result<Option<u64>, DiscoveryRegistrationError> {
    if let Some(expected_revision) = config.expected_revision {
        return Ok(Some(expected_revision));
    }

    if !config.revision_cas_on_register {
        return Ok(None);
    }

    let read_metadata = unsigned_registry_read_metadata(subject_id);
    let mut grpc_request = Request::new(RetrieveInstanceRequest {
        namespace: config.namespace.clone(),
        environment: config.environment.clone(),
        service_name: config.service_name.clone(),
        instance_id: config.instance_id.clone(),
    });
    apply_metadata_template(grpc_request.metadata_mut(), &read_metadata);

    match client.retrieve_instance(grpc_request).await {
        Ok(response) => match response.into_inner().instance {
            Some(instance) => Ok(Some(instance.revision)),
            None => Ok(None),
        },
        Err(status) if status.code() == Code::NotFound => Ok(None),
        Err(status) => Err(map_status(status)),
    }
}

async fn connect(endpoint: &str) -> Result<Channel, DiscoveryRegistrationError> {
    let normalized = normalize_http_endpoint(endpoint);
    Endpoint::from_shared(normalized)
        .map_err(|error| DiscoveryRegistrationError::Transport(error.to_string()))?
        .connect()
        .await
        .map_err(|error| DiscoveryRegistrationError::Transport(error.to_string()))
}

fn require_non_empty(value: &str, field: &str) -> Result<(), DiscoveryRegistrationError> {
    require_non_blank(value, field).map_err(DiscoveryRegistrationError::Validation)
}

fn map_status(status: Status) -> DiscoveryRegistrationError {
    DiscoveryRegistrationError::Rpc(status.to_string())
}

/// Computes a Full Jitter backoff for lease renewal retries.
///
/// Mirrors the watch reconnect backoff contract: the base grows exponentially
/// with the failure count and the actual delay is uniform in `[0, base]`,
/// bounded by `max_backoff`. Full Jitter (Google SRE) avoids thundering-herd
/// retries when many instances lose connectivity to the control plane
/// simultaneously.
fn renew_backoff(failures: u32, initial: Duration, max_backoff: Duration) -> Duration {
    let exponent = failures.min(16);
    let base = initial
        .saturating_mul(1u32 << exponent)
        .min(max_backoff);
    if base.is_zero() {
        return initial;
    }
    let mut rng = rand::rng();
    let millis = rng.random_range(0..=base.as_millis().min(u128::from(u64::MAX)) as u64);
    Duration::from_millis(millis)
}

pub fn default_instance_id(service_name: &str) -> String {
    format!("{service_name}-{}", Uuid::new_v4())
}

pub fn grpc_advertised_endpoint(bind_addr: &str) -> String {
    normalize_grpc_endpoint(bind_addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_required_metadata() {
        let mut config = sample_config();
        config.metadata.remove("rpc_surface");
        assert!(config.validate().is_err());
    }

    #[test]
    fn builds_grpc_advertised_endpoint() {
        assert_eq!(
            grpc_advertised_endpoint("127.0.0.1:50051"),
            "grpc://127.0.0.1:50051"
        );
    }

    #[test]
    fn renew_backoff_never_exceeds_max() {
        let initial = Duration::from_millis(500);
        let max = Duration::from_secs(5);
        for failures in 1..20 {
            let backoff = renew_backoff(failures, initial, max);
            assert!(
                backoff <= max,
                "failures {failures} backoff {backoff:?} > max {max:?}"
            );
        }
    }

    #[test]
    fn renew_backoff_grows_with_failures_before_jitter() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(30);
        let max_seen_1 = (0..256).map(|_| renew_backoff(1, initial, max).as_millis()).max();
        let max_seen_5 = (0..256).map(|_| renew_backoff(5, initial, max).as_millis()).max();
        assert!(
            max_seen_5 >= max_seen_1,
            "higher failure count should allow larger backoff"
        );
    }

    #[test]
    fn renew_loop_config_defaults_are_sane() {
        let config = RenewLoopConfig::default();
        assert!(config.initial_backoff < config.max_backoff);
        assert!(config.max_consecutive_failures > 0);
    }

    fn sample_config() -> DiscoveryInstanceConfig {
        DiscoveryInstanceConfig {
            discovery_endpoint: "http://127.0.0.1:19090".to_string(),
            namespace: "sdkwork".to_string(),
            environment: "development".to_string(),
            service_name: "sdkwork-commerce-app-rpc".to_string(),
            instance_id: "commerce-1".to_string(),
            advertised_endpoint: "grpc://127.0.0.1:50051".to_string(),
            protocol: "grpc".to_string(),
            version: "0.1.0".to_string(),
            region: "local".to_string(),
            zone: "local".to_string(),
            lease_ttl_seconds: 30,
            subject_id: "sdkwork-commerce-service-host".to_string(),
            metadata: BTreeMap::from([("rpc_surface".to_string(), "app".to_string())]),
            revision_cas_on_register: true,
            expected_revision: None,
        }
    }
}
