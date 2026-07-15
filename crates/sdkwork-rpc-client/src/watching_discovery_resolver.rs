//! Discovery watch-driven name resolver with reconnect and cache TTL.
//!
//! Implements the watch semantics from `DISCOVERY_SPEC.md` §10 and the client
//! integration flow from §12. The watch loop:
//!
//! 1. Connects to `DiscoveryWatchService` starting from the last observed
//!    revision so reconnects replay only the missing mutations (§10
//!    "Reconnects remain safe because durable watch storage is the source of
//!    truth for revision replay").
//! 2. Updates a TTL-bounded cache from stream events. When the stream is
//!    disconnected, the cache expires after `cache_ttl` so callers fall back to
//!    a fresh `DiscoverInstances` RPC instead of serving stale picks.
//! 3. Reconnects with exponential backoff + Full Jitter (Google SRE) so a
//!    down control plane is not hammered.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rand::Rng;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::common::v1::{InstanceStatus, WatchEventType};
use sdkwork_discovery_rpc_proto::sdkwork::discovery::internal::v1::discovery_watch_service_client::DiscoveryWatchServiceClient;
use sdkwork_discovery_rpc_proto::sdkwork::discovery::internal::v1::WatchServiceRequest;
use sdkwork_rpc_discovery::{apply_metadata_template, unsigned_registry_read_metadata};
use sdkwork_rpc_framework_core::RpcFrameworkError;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::Request;
use tracing::{info, warn};

use crate::discovery_resolver::{
    map_discovered_instance, DiscoveryNameResolver, DiscoveryNameResolverConfig,
};
use crate::resolver::{NameResolver, ResolvedEndpoint};

/// Tunable parameters for the watch loop reconnect and cache behavior.
///
/// Defaults follow `DISCOVERY_SPEC.md` §10 guidance: bounded reconnect with
/// backoff, and a cache TTL short enough that a disconnected watch does not
/// serve stale picks for long, yet long enough to avoid a discover RPC on every
/// resolve call.
#[derive(Clone, Debug)]
pub struct WatchLoopConfig {
    /// Initial reconnect backoff. Doubles per attempt up to `max_reconnect_backoff`.
    pub initial_reconnect_backoff: Duration,
    /// Upper bound on reconnect backoff.
    pub max_reconnect_backoff: Duration,
    /// How long cached endpoints remain valid after the last stream event.
    pub cache_ttl: Duration,
}

impl Default for WatchLoopConfig {
    fn default() -> Self {
        Self {
            initial_reconnect_backoff: Duration::from_millis(500),
            max_reconnect_backoff: Duration::from_secs(30),
            cache_ttl: Duration::from_secs(15),
        }
    }
}

/// Cached endpoints with the timestamp of the last stream mutation.
#[derive(Clone, Debug)]
struct CachedEndpoints {
    endpoints: Vec<ResolvedEndpoint>,
    updated_at: Instant,
}

#[derive(Clone, Debug)]
pub struct WatchingDiscoveryNameResolver {
    inner: DiscoveryNameResolver,
    cache: Arc<RwLock<BTreeMap<String, CachedEndpoints>>>,
    last_revision: Arc<AtomicU64>,
    watch_config: WatchLoopConfig,
}

impl WatchingDiscoveryNameResolver {
    pub fn new(config: DiscoveryNameResolverConfig) -> Result<Self, RpcFrameworkError> {
        Self::with_watch_config(config, WatchLoopConfig::default())
    }

    /// Constructs the resolver with explicit watch-loop tunables, enabling
    /// tests to shorten backoff and TTL without waiting real durations.
    pub fn with_watch_config(
        config: DiscoveryNameResolverConfig,
        watch_config: WatchLoopConfig,
    ) -> Result<Self, RpcFrameworkError> {
        Ok(Self {
            inner: DiscoveryNameResolver::new(config)?,
            cache: Arc::new(RwLock::new(BTreeMap::new())),
            last_revision: Arc::new(AtomicU64::new(0)),
            watch_config,
        })
    }

    /// Returns the last revision observed from the watch stream. Used by tests
    /// and operational probes to confirm the stream is advancing.
    pub fn last_revision(&self) -> u64 {
        self.last_revision.load(Ordering::Acquire)
    }

    /// Spawns the reconnect-capable watch loop for a single service.
    ///
    /// The task runs until the runtime is dropped; it never panics the
    /// process, only logs reconnect attempts. Returns a handle the caller can
    /// abort during shutdown.
    pub fn spawn_watch_loop(self: &Arc<Self>, service_name: impl Into<String>) -> JoinHandle<()> {
        let service_name = service_name.into();
        let resolver = Arc::clone(self);
        tokio::spawn(async move {
            resolver.run_reconnect_loop(&service_name).await;
        })
    }

    /// Outer reconnect loop. Each iteration runs one stream session; on end or
    /// error it backs off and reconnects from the last observed revision.
    async fn run_reconnect_loop(&self, service_name: &str) {
        let mut attempt: u32 = 0;
        loop {
            match self.run_watch_session(service_name).await {
                Ok(()) => {
                    // Stream ended cleanly (e.g., server-initiated close).
                    // Reset backoff and reconnect immediately; the control
                    // plane is reachable.
                    attempt = 0;
                    info!(
                        target: "sdkwork.rpc.discovery.watch",
                        service_name = %service_name,
                        "watch stream ended cleanly, reconnecting"
                    );
                }
                Err(error) => {
                    warn!(
                        target: "sdkwork.rpc.discovery.watch",
                        service_name = %service_name,
                        error = %error,
                        attempt,
                        "watch stream error, reconnecting after backoff"
                    );
                    let backoff = reconnect_backoff(
                        attempt,
                        self.watch_config.initial_reconnect_backoff,
                        self.watch_config.max_reconnect_backoff,
                    );
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Runs a single watch stream session until the stream ends or errors.
    ///
    /// Reuses the cached discovery control-plane channel from the inner
    /// resolver so watch reconnects do not pay a fresh TCP+HTTP2 handshake.
    /// Tonic channels auto-reconnect on transient failures.
    async fn run_watch_session(&self, service_name: &str) -> Result<(), RpcFrameworkError> {
        let config = self.inner.config();
        let channel = self.inner.get_or_init_channel().await?;
        let mut client = DiscoveryWatchServiceClient::new(channel);
        let from_revision = self.last_revision();
        let mut request = Request::new(WatchServiceRequest {
            namespace: config.namespace.clone(),
            environment: config.environment.clone(),
            service_name: service_name.to_string(),
            from_revision,
        });
        apply_metadata_template(
            request.metadata_mut(),
            &unsigned_registry_read_metadata(&config.subject_id),
        );

        let mut stream = client
            .watch_service(request)
            .await
            .map_err(|error| RpcFrameworkError::Transport(error.to_string()))?
            .into_inner();

        info!(
            target: "sdkwork.rpc.discovery.watch",
            service_name = %service_name,
            from_revision,
            "started discovery watch session"
        );

        while let Some(message) = stream
            .message()
            .await
            .map_err(|error| RpcFrameworkError::Transport(error.to_string()))?
        {
            // Advance the revision cursor from response metadata so reconnects
            // resume from the correct point (§10 revision-ordered replay).
            if let Some(metadata) = message.metadata {
                if metadata.revision > 0 {
                    self.last_revision
                        .fetch_max(metadata.revision, Ordering::AcqRel);
                }
            }

            let event_type =
                WatchEventType::try_from(message.event_type).unwrap_or(WatchEventType::Unspecified);

            // Heartbeat events carry no instance payload; they only confirm the
            // stream is alive (§10 "Idle streams SHOULD receive heartbeat
            // events with the last delivered revision"). Refresh the cache
            // timestamp so a healthy stream does not expire its entries.
            if event_type == WatchEventType::Heartbeat {
                let mut cache = self.cache.write().await;
                if let Some(cached) = cache.get_mut(service_name) {
                    cached.updated_at = Instant::now();
                }
                continue;
            }

            let Some(instance) = message.instance else {
                continue;
            };
            let endpoint = map_discovered_instance(&instance);
            let mut cache = self.cache.write().await;
            let entry = cache
                .entry(service_name.to_string())
                .or_insert_with(|| CachedEndpoints {
                    endpoints: Vec::new(),
                    updated_at: Instant::now(),
                });

            let status = InstanceStatus::try_from(instance.status).ok();
            let is_serving = matches!(
                status,
                Some(InstanceStatus::Serving) | Some(InstanceStatus::Degraded)
            );

            // Explicit deregister events and non-serving tombstones remove the
            // instance from the cache so load balancing stops picking it.
            if event_type == WatchEventType::InstanceDeregistered || !is_serving {
                entry
                    .endpoints
                    .retain(|existing| existing.endpoint != instance.endpoint);
                if entry.endpoints.is_empty() {
                    cache.remove(service_name);
                }
            } else if let Some(endpoint) = endpoint {
                upsert_endpoint(&mut entry.endpoints, endpoint);
                entry.updated_at = Instant::now();
            }
        }

        Ok(())
    }

    /// Returns cached endpoints if present and within TTL, otherwise `None`.
    async fn cached_endpoints(&self, service_name: &str) -> Option<Vec<ResolvedEndpoint>> {
        let cache = self.cache.read().await;
        let cached = cache.get(service_name)?;
        if cached.endpoints.is_empty() {
            return None;
        }
        if cached.updated_at.elapsed() > self.watch_config.cache_ttl {
            // Stale cache: let the caller fall through to a fresh discover RPC.
            return None;
        }
        Some(cached.endpoints.clone())
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

        // Cache miss or expired: perform a synchronous discover RPC and refresh
        // the cache so subsequent resolves are served from the watch cache
        // until the stream catches up or TTL elapses again.
        let endpoints = self.inner.resolve(service_name).await?;
        let mut cache = self.cache.write().await;
        cache.insert(
            service_name.to_string(),
            CachedEndpoints {
                endpoints: endpoints.clone(),
                updated_at: Instant::now(),
            },
        );
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

/// Computes a Full Jitter backoff for watch reconnects.
///
/// Mirrors the retry backoff contract in `RPC_RESILIENCE_SPEC.md` §4
/// ("Backoff SHOULD use exponential delay with jitter"): the base grows
/// exponentially with the attempt and the actual delay is uniform in
/// `[0, base]`, bounded by `max_backoff`. Full Jitter (Google SRE) avoids
/// thundering-herd reconnects when many clients lose a stream simultaneously.
fn reconnect_backoff(attempt: u32, initial: Duration, max_backoff: Duration) -> Duration {
    let exponent = attempt.min(16);
    let base = initial.saturating_mul(1u32 << exponent).min(max_backoff);
    if base.is_zero() {
        return initial;
    }
    let mut rng = rand::rng();
    let millis = rng.random_range(0..=base.as_millis().min(u128::from(u64::MAX)) as u64);
    Duration::from_millis(millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_never_exceeds_max() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_millis(500);
        for attempt in 0..20 {
            let backoff = reconnect_backoff(attempt, initial, max);
            assert!(
                backoff <= max,
                "attempt {attempt} backoff {backoff:?} > max {max:?}"
            );
        }
    }

    #[test]
    fn reconnect_backoff_grows_with_attempt_before_jitter() {
        // Without jitter the base at attempt 0 is initial, attempt 1 is 2x,
        // attempt 2 is 4x. With Full Jitter the value is in [0, base], so the
        // upper bound grows; we assert the base computation via a large sample.
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(10);
        let max_seen_0 = (0..256)
            .map(|_| reconnect_backoff(0, initial, max).as_millis())
            .max();
        let max_seen_5 = (0..256)
            .map(|_| reconnect_backoff(5, initial, max).as_millis())
            .max();
        assert!(
            max_seen_5 >= max_seen_0,
            "higher attempt should allow larger backoff"
        );
    }

    #[test]
    fn last_revision_starts_at_zero() {
        let resolver = WatchingDiscoveryNameResolver::new(DiscoveryNameResolverConfig {
            discovery_endpoint: "http://127.0.0.1:50051".to_string(),
            namespace: "sdkwork".to_string(),
            environment: "development".to_string(),
            subject_id: "watcher-1".to_string(),
            healthy_only: true,
            protocol: "grpc".to_string(),
        })
        .expect("valid config");
        assert_eq!(resolver.last_revision(), 0);
    }

    #[test]
    fn watch_loop_config_defaults_are_sane() {
        let config = WatchLoopConfig::default();
        assert!(config.initial_reconnect_backoff < config.max_reconnect_backoff);
        assert!(config.cache_ttl > Duration::ZERO);
    }
}
