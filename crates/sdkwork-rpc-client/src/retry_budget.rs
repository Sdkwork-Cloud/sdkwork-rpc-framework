//! Cross-call retry budget registry per `RPC_RESILIENCE_SPEC.md` §4.2.
//!
//! Each caller process enforces a retry budget per target `service_name` so a
//! degraded service cannot trigger a retry storm. The registry holds one token
//! bucket per service; tokens refill continuously while the service is healthy,
//! and each retry attempt consumes one token. When a bucket is empty,
//! [`RetryBudgetRegistry::try_acquire`] returns `false` and the caller MUST
//! fail fast with `UNAVAILABLE` rather than issuing the retry.
//!
//! This is distinct from the per-call [`sdkwork_rpc_resilience::RetryBudgetTracker`],
//! which bounds retries for a single top-level invocation. The registry bounds
//! the aggregate retry rate across all concurrent calls to a given service.
//!
//! Time is measured with [`std::time::Instant`] (monotonic clock) rather than
//! wall-clock time because token-bucket refill math requires a monotonically
//! increasing source; wall-clock can jump backwards on NTP sync and would
//! corrupt the refill calculation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tracing::warn;

/// Configuration for a per-service retry token bucket.
///
/// Defaults follow the Google SRE "Handling Overload" guidance: a retry
/// budget sized at roughly 10% of steady-state call rate, refilled
/// continuously so transient failures can be retried while a sustained
/// outage fails fast.
#[derive(Clone, Debug)]
pub struct RetryBudgetConfig {
    /// Maximum tokens a bucket can hold. Caps the burst of retries that can
    /// accumulate while the service is healthy.
    pub capacity: u32,
    /// Tokens added per second when the service is healthy. Set relative to
    /// the expected call rate so the budget represents the tolerated retry
    /// fraction (e.g., 0.1 × call_rate for a 10% retry budget).
    pub refill_per_second: f64,
}

impl Default for RetryBudgetConfig {
    fn default() -> Self {
        Self {
            capacity: 100,
            refill_per_second: 10.0,
        }
    }
}

/// A single token bucket for one `service_name`.
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(filled_to: u32) -> Self {
        Self {
            tokens: filled_to as f64,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self, config: &RetryBudgetConfig) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            let added = elapsed * config.refill_per_second;
            self.tokens = (self.tokens + added).min(config.capacity as f64);
            self.last_refill = now;
        }
    }

    fn try_consume(&mut self) -> bool {
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Process-wide registry of per-`service_name` retry token buckets.
///
/// Cloning is cheap (inner state is behind an [`Arc`]); all clones share the
/// same buckets, so a single registry instantiated at client bootstrap can be
/// handed to every RPC client factory.
#[derive(Clone, Debug)]
pub struct RetryBudgetRegistry {
    inner: Arc<Mutex<HashMap<String, TokenBucket>>>,
    config: RetryBudgetConfig,
}

impl RetryBudgetRegistry {
    /// Creates an empty registry with the given per-service bucket config.
    pub fn new(config: RetryBudgetConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Attempts to consume one retry token for `service_name`.
    ///
    /// Returns `true` when a token was available (the retry may proceed) and
    /// `false` when the budget is exhausted, in which case the caller MUST
    /// fail fast with `UNAVAILABLE` rather than issuing the retry
    /// (`RPC_RESILIENCE_SPEC.md` §4.2). Exhaustion is logged with
    /// `service_name` and `operation_id` so operations can observe budget
    /// pressure per target.
    ///
    /// A previously-unseen `service_name` lazily initializes a full bucket,
    /// so the first retry against any service is always admitted.
    pub fn try_acquire(&self, service_name: &str, operation_id: &str) -> bool {
        let mut buckets = self.inner.lock().expect("retry budget mutex poisoned");
        let bucket = buckets
            .entry(service_name.to_string())
            .or_insert_with(|| TokenBucket::new(self.config.capacity));
        bucket.refill(&self.config);
        if bucket.try_consume() {
            true
        } else {
            warn!(
                target: "sdkwork.rpc.retry.budget",
                service_name = %service_name,
                operation_id = %operation_id,
                "cross-call retry budget exhausted; failing fast instead of retrying"
            );
            false
        }
    }

    /// Returns the current token count for `service_name` after refilling.
    ///
    /// Intended for observability/metrics. A returned count below `1.0`
    /// indicates the next retry will be rejected.
    pub fn available_tokens(&self, service_name: &str) -> f64 {
        let mut buckets = self.inner.lock().expect("retry budget mutex poisoned");
        let bucket = buckets
            .entry(service_name.to_string())
            .or_insert_with(|| TokenBucket::new(self.config.capacity));
        bucket.refill(&self.config);
        bucket.tokens
    }
}

impl Default for RetryBudgetRegistry {
    fn default() -> Self {
        Self::new(RetryBudgetConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn first_acquire_for_new_service_succeeds() {
        let registry = RetryBudgetRegistry::default();
        assert!(registry.try_acquire("svc-a", "op-1"));
    }

    #[test]
    fn budget_exhaustion_fails_fast() {
        let registry = RetryBudgetRegistry::new(RetryBudgetConfig {
            capacity: 2,
            refill_per_second: 0.0, // no refill → deterministic exhaustion
        });
        assert!(registry.try_acquire("svc-b", "op-1"));
        assert!(registry.try_acquire("svc-b", "op-2"));
        assert!(
            !registry.try_acquire("svc-b", "op-3"),
            "third retry must be rejected once the 2-token budget is drained"
        );
    }

    #[test]
    fn budgets_are_isolated_per_service() {
        let registry = RetryBudgetRegistry::new(RetryBudgetConfig {
            capacity: 1,
            refill_per_second: 0.0,
        });
        assert!(registry.try_acquire("svc-c", "op-1"));
        assert!(
            registry.try_acquire("svc-d", "op-1"),
            "exhausting svc-c must not affect svc-d"
        );
        assert!(!registry.try_acquire("svc-c", "op-2"));
    }

    #[test]
    fn refill_restores_tokens_over_time() {
        let registry = RetryBudgetRegistry::new(RetryBudgetConfig {
            capacity: 1,
            refill_per_second: 10_000.0, // very fast refill
        });
        assert!(registry.try_acquire("svc-e", "op-1"));
        // After a short sleep the bucket refills past 1.0 again.
        thread::sleep(std::time::Duration::from_millis(20));
        assert!(
            registry.try_acquire("svc-e", "op-2"),
            "refill must restore the token after enough wall time"
        );
    }

    #[test]
    fn clones_share_state() {
        let registry = RetryBudgetRegistry::new(RetryBudgetConfig {
            capacity: 1,
            refill_per_second: 0.0,
        });
        let cloned = registry.clone();
        assert!(registry.try_acquire("svc-f", "op-1"));
        assert!(
            !cloned.try_acquire("svc-f", "op-2"),
            "clones must share the same per-service bucket"
        );
    }

    #[test]
    fn registry_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RetryBudgetRegistry>();
    }
}
