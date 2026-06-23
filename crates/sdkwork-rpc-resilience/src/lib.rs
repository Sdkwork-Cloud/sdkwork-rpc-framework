//! SDKWork RPC resilience profiles and retry policy.

mod backoff;
mod circuit_breaker;
mod idempotency;

use sdkwork_rpc_framework_core::ResilienceProfile;
use tonic::Code;

pub use backoff::retry_backoff_ms;
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState};
pub use idempotency::{should_retry_call, RetryAdmission};

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

    pub fn allows_retry(&self, code: Code, remaining_budget: u32) -> bool {
        remaining_budget > 0 && self.retryable_codes.contains(&code)
    }

    pub fn should_retry(
        &self,
        code: Code,
        attempt: u32,
        budget: &mut RetryBudgetTracker,
    ) -> bool {
        if attempt >= self.max_attempts {
            return false;
        }
        if !self.allows_retry(code, budget.remaining()) {
            return false;
        }
        budget.consume()
    }
}

#[derive(Clone, Debug, Default)]
pub struct RetryBudgetTracker {
    remaining: u32,
}

impl RetryBudgetTracker {
    pub fn new(budget: u32) -> Self {
        Self { remaining: budget }
    }

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
}
