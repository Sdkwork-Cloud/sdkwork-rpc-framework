//! Idempotency admission for retry decisions per `RPC_RESILIENCE_SPEC.md` section 4.

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
pub fn should_retry_call(
    policy: &RetryPolicy,
    code: Code,
    attempt: u32,
    budget: &mut RetryBudgetTracker,
    admission: RetryAdmission,
) -> bool {
    if !admission.allows_retry_metadata() {
        return false;
    }
    policy.should_retry(code, attempt, budget)
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
}
