//! SDKWork RPC resilience profiles, retry policy, retry budgets, and server
//! retry pushback parsing.
//!
//! Implements the retry policy contract from `RPC_RESILIENCE_SPEC.md` §3-§4:
//! per-profile retryable code whitelists, deadline-aware retry decisions, a
//! per-call budget tracker ([`RetryBudgetTracker`]), and gRPC server pushback
//! parsing ([`pushback`]). Cross-call retry budgets (per-`service_name` token
//! bucket) live in the client pipeline via
//! [`sdkwork_rpc_client::RetryBudgetRegistry`]; this module exposes the
//! primitives the pipeline composes.

mod backoff;
mod circuit_breaker;
mod idempotency;
mod pushback;

use std::time::Duration;

use sdkwork_rpc_framework_core::ResilienceProfile;
use tonic::Code;
use tracing::warn;

pub use backoff::retry_backoff_ms;
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState};
pub use idempotency::{should_retry_call, should_retry_call_with_deadline, RetryAdmission};
pub use pushback::{
    effective_retry_backoff_ms, extract_retry_pushback_ms, should_retry_call_with_pushback,
    RetryDecision, GRPC_RETRY_PUSHBACK_MS,
};

/// Minimum deadline reserve (milliseconds) required for any retry attempt,
/// independent of the jittered backoff. Represents the floor time needed to
/// establish a connection and exchange at least one request/response, so a
/// Full-Jitter backoff of 0 cannot collapse the required deadline to 0.
pub(crate) const MIN_RPC_DEADLINE_RESERVE_MS: u64 = 10;

/// Checks whether `remaining` can cover `backoff_ms` plus a conservative RPC
/// round-trip estimate, floored at [`MIN_RPC_DEADLINE_RESERVE_MS`].
///
/// Shared by [`RetryPolicy::should_retry_with_deadline`] and the pushback-aware
/// decision path ([`should_retry_call_with_pushback`]) so the deadline math
/// stays in one place and the two paths cannot drift.
pub(crate) fn deadline_covers_backoff(backoff_ms: u64, remaining: Duration) -> bool {
    let rpc_estimate_ms = (backoff_ms / 2).max(MIN_RPC_DEADLINE_RESERVE_MS);
    let required_ms = backoff_ms.saturating_add(rpc_estimate_ms);
    match u64::try_from(remaining.as_millis()) {
        Ok(remaining_ms) if remaining_ms >= required_ms => true,
        Ok(remaining_ms) => {
            warn!(
                target: "sdkwork.rpc.retry.deadline",
                remaining_ms,
                required_ms,
                "retry skipped: insufficient remaining deadline"
            );
            false
        }
        // `remaining.as_millis()` exceeded `u128::MAX`-ish; treat as unbounded.
        Err(_) => true,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub retry_budget: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub retryable_codes: Vec<Code>,
}

impl RetryPolicy {
    pub fn for_profile(profile: ResilienceProfile) -> Self {
        match profile {
            ResilienceProfile::RpcReadOnly => Self {
                max_attempts: 3,
                retry_budget: 32,
                initial_backoff_ms: 50,
                max_backoff_ms: 2_000,
                retryable_codes: vec![Code::Unavailable, Code::ResourceExhausted],
            },
            ResilienceProfile::RpcIdempotentWrite => Self {
                max_attempts: 2,
                retry_budget: 16,
                initial_backoff_ms: 100,
                max_backoff_ms: 2_000,
                retryable_codes: vec![Code::Unavailable],
            },
            ResilienceProfile::RpcCriticalWrite => Self {
                max_attempts: 1,
                retry_budget: 4,
                initial_backoff_ms: 100,
                max_backoff_ms: 500,
                retryable_codes: vec![],
            },
            ResilienceProfile::RpcStream => Self {
                max_attempts: 1,
                retry_budget: 4,
                initial_backoff_ms: 100,
                max_backoff_ms: 500,
                retryable_codes: vec![],
            },
            ResilienceProfile::RpcLocalDev => Self {
                max_attempts: 5,
                retry_budget: 64,
                initial_backoff_ms: 25,
                max_backoff_ms: 1_000,
                retryable_codes: vec![Code::Unavailable, Code::ResourceExhausted],
            },
            ResilienceProfile::RpcDefault => Self {
                max_attempts: 3,
                retry_budget: 24,
                initial_backoff_ms: 50,
                max_backoff_ms: 2_000,
                retryable_codes: vec![Code::Unavailable],
            },
        }
    }

    /// Returns true when the gRPC code is in the profile's retryable whitelist
    /// and the budget still has tokens. Pure check; does not consume budget.
    pub fn allows_retry(&self, code: Code, remaining_budget: u32) -> bool {
        remaining_budget > 0 && self.retryable_codes.contains(&code)
    }

    /// Convenience wrapper around [`should_retry_with_deadline`] that ignores
    /// the parent deadline. New call sites should prefer the deadline-aware
    /// variant so retry storms cannot amplify past the caller's deadline.
    pub fn should_retry(
        &self,
        code: Code,
        attempt: u32,
        budget: &mut RetryBudgetTracker,
    ) -> bool {
        self.should_retry_with_deadline(code, attempt, budget, Duration::MAX)
    }

    /// Returns true when a retry should be issued, consuming one budget token.
    ///
    /// Per `RPC_RESILIENCE_SPEC.md` §4 "Retry decisions MUST respect remaining
    /// parent deadline", this refuses to retry when the remaining deadline is
    /// insufficient to cover the backoff plus a conservative RPC estimate.
    /// Refusing early prevents wasted retries that would immediately hit
    /// `DEADLINE_EXCEEDED` and amplify the failure surface.
    pub fn should_retry_with_deadline(
        &self,
        code: Code,
        attempt: u32,
        budget: &mut RetryBudgetTracker,
        remaining: Duration,
    ) -> bool {
        if attempt >= self.max_attempts {
            return false;
        }
        if !self.allows_retry(code, budget.remaining()) {
            return false;
        }

        let backoff_ms = retry_backoff_ms(self, attempt);
        if !deadline_covers_backoff(backoff_ms, remaining) {
            return false;
        }

        if !budget.consume() {
            warn!(
                target: "sdkwork.rpc.retry.budget",
                attempt,
                "retry skipped: per-call retry budget exhausted"
            );
            return false;
        }
        true
    }
}

/// Per-call retry budget tracker.
///
/// A new tracker is constructed for each top-level RPC invocation with the
/// profile's budget; subsequent retries decrement `remaining` until it
/// reaches zero, at which point retries fail fast. Cross-call (per-service)
/// retry budgets are handled at the client pipeline layer via a shared
/// token bucket — see [`sdkwork_rpc_client::RetryBudgetRegistry`].
#[derive(Clone, Debug, Default)]
pub struct RetryBudgetTracker {
    remaining: u32,
}

impl RetryBudgetTracker {
    pub fn new(budget: u32) -> Self {
        Self { remaining: budget }
    }

    /// Decrements the budget by one. Returns false when no tokens remain.
    pub fn consume(&mut self) -> bool {
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }

    pub fn remaining(&self) -> u32 {
        self.remaining
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn critical_write_profile_does_not_retry() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcCriticalWrite);
        assert!(!policy.allows_retry(Code::Unavailable, 10));
    }

    #[test]
    fn default_profile_retries_unavailable_within_budget() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        assert!(policy.allows_retry(Code::Unavailable, 1));
        assert!(!policy.allows_retry(Code::InvalidArgument, 1));
    }

    #[test]
    fn should_retry_respects_budget_and_attempt_limits() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        let mut budget = RetryBudgetTracker::new(1);
        assert!(policy.should_retry(Code::Unavailable, 1, &mut budget));
        assert!(!policy.should_retry(Code::Unavailable, 2, &mut budget));
    }

    #[test]
    fn should_retry_with_deadline_skips_when_remaining_is_insufficient() {
        // Even with budget and a retryable code, a near-zero deadline must
        // suppress the retry (§4 "Retry decisions MUST respect remaining
        // parent deadline").
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        let mut budget = RetryBudgetTracker::new(10);
        assert!(!policy.should_retry_with_deadline(
            Code::Unavailable,
            1,
            &mut budget,
            Duration::from_millis(1),
        ));
        // Budget must not be consumed when the retry is skipped.
        assert_eq!(budget.remaining(), 10);
    }

    #[test]
    fn should_retry_with_deadline_proceeds_when_remaining_is_sufficient() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        let mut budget = RetryBudgetTracker::new(10);
        assert!(policy.should_retry_with_deadline(
            Code::Unavailable,
            1,
            &mut budget,
            Duration::from_secs(10),
        ));
        assert_eq!(budget.remaining(), 9);
    }
}
