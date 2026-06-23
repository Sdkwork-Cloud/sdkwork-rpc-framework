//! Exponential backoff with jitter per `RPC_RESILIENCE_SPEC.md` section 4.

use crate::RetryPolicy;

/// Computes the delay before the next retry attempt using exponential backoff and jitter.
pub fn retry_backoff_ms(policy: &RetryPolicy, attempt: u32) -> u64 {
    if attempt == 0 {
        return policy.initial_backoff_ms;
    }

    let exponent = attempt.saturating_sub(1).min(16);
    let base = policy
        .initial_backoff_ms
        .saturating_mul(1u64 << exponent)
        .min(policy.max_backoff_ms);

    let jitter_span = (base / 4).max(1);
    let jitter = pseudo_jitter(attempt, jitter_span);
    base.saturating_add(jitter).min(policy.max_backoff_ms)
}

fn pseudo_jitter(attempt: u32, span: u64) -> u64 {
    let seed = (attempt as u64)
        .wrapping_mul(1_103_515_245)
        .wrapping_add(12_345);
    seed % span
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryPolicy;
    use sdkwork_rpc_framework_core::ResilienceProfile;

    #[test]
    fn backoff_grows_with_attempts_and_stays_bounded() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        let first = retry_backoff_ms(&policy, 1);
        let later = retry_backoff_ms(&policy, 4);
        assert!(later >= first);
        assert!(later <= policy.max_backoff_ms);
    }
}
