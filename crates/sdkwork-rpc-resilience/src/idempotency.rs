//! Idempotency admission for retry decisions per `RPC_RESILIENCE_SPEC.md` section 4.

use std::time::Duration;

use tonic::Code;

use crate::{RetryBudgetTracker, RetryPolicy};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryAdmission {
    /// Whether the RPC manifest marks the method as requiring idempotency metadata.
    pub method_requires_idempotency: bool,
    /// Whether the outbound call carries an idempotency key.
    pub has_idempotency_key: bool,
}

impl RetryAdmission {
    pub fn allows_retry_metadata(&self) -> bool {
        if self.method_requires_idempotency && !self.has_idempotency_key {
            return false;
        }
        true
    }
}

/// Evaluates retry eligibility including idempotency admission rules.
///
/// This delegates to [`should_retry_call_with_deadline`] with `Duration::MAX`
/// for backward compatibility. New call sites SHOULD prefer the deadline-aware
/// variant so retry storms cannot amplify past the caller's remaining deadline
/// (per `RPC_RESILIENCE_SPEC.md` §4 "Retry decisions MUST respect remaining
/// parent deadline").
pub fn should_retry_call(
    policy: &RetryPolicy,
    code: Code,
    attempt: u32,
    budget: &mut RetryBudgetTracker,
    admission: RetryAdmission,
) -> bool {
    should_retry_call_with_deadline(policy, code, attempt, budget, admission, Duration::MAX)
}

/// Evaluates retry eligibility including idempotency admission rules and the
/// caller's remaining deadline.
///
/// Combines [`RetryAdmission`] gating with [`RetryPolicy::should_retry_with_deadline`]
/// so a retry is refused when the remaining deadline cannot cover the backoff
/// plus a conservative RPC estimate. This closes a bypass where the previous
/// `should_retry_call` reached the deadline-ignoring `should_retry` path and
/// could issue retries that immediately hit `DEADLINE_EXCEEDED`.
pub fn should_retry_call_with_deadline(
    policy: &RetryPolicy,
    code: Code,
    attempt: u32,
    budget: &mut RetryBudgetTracker,
    admission: RetryAdmission,
    remaining: Duration,
) -> bool {
    if !admission.allows_retry_metadata() {
        return false;
    }
    policy.should_retry_with_deadline(code, attempt, budget, remaining)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryPolicy;
    use sdkwork_rpc_framework_core::ResilienceProfile;

    #[test]
    fn blocks_retry_without_idempotency_key_when_required() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcIdempotentWrite);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: true,
            has_idempotency_key: false,
        };
        assert!(!should_retry_call(
            &policy,
            Code::Unavailable,
            1,
            &mut budget,
            admission
        ));
    }

    #[test]
    fn allows_retry_when_idempotency_key_is_present() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcIdempotentWrite);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: true,
            has_idempotency_key: true,
        };
        assert!(should_retry_call(
            &policy,
            Code::Unavailable,
            1,
            &mut budget,
            admission
        ));
    }

    #[test]
    fn should_retry_call_with_deadline_skips_when_remaining_is_insufficient() {
        // Regression: previously the idempotency-aware path ignored the
        // deadline and could issue retries that immediately hit
        // DEADLINE_EXCEEDED. The deadline-aware variant MUST suppress the
        // retry and MUST NOT consume budget when suppressed.
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: false,
            has_idempotency_key: false,
        };
        assert!(!should_retry_call_with_deadline(
            &policy,
            Code::Unavailable,
            1,
            &mut budget,
            admission,
            Duration::from_millis(1),
        ));
        assert_eq!(budget.remaining(), 10, "budget must not be consumed when retry skipped");
    }

    #[test]
    fn should_retry_call_with_deadline_still_enforces_idempotency_admission() {
        // Even with an ample deadline, a method requiring idempotency metadata
        // MUST NOT retry when no idempotency key is present.
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcIdempotentWrite);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: true,
            has_idempotency_key: false,
        };
        assert!(!should_retry_call_with_deadline(
            &policy,
            Code::Unavailable,
            1,
            &mut budget,
            admission,
            Duration::from_secs(10),
        ));
        assert_eq!(budget.remaining(), 10);
    }

    #[test]
    fn should_retry_call_with_deadline_proceeds_when_admission_and_deadline_ok() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcIdempotentWrite);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: true,
            has_idempotency_key: true,
        };
        assert!(should_retry_call_with_deadline(
            &policy,
            Code::Unavailable,
            1,
            &mut budget,
            admission,
            Duration::from_secs(10),
        ));
        assert_eq!(budget.remaining(), 9);
    }
}
