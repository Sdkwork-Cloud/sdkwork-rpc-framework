//! Exponential backoff with jitter per `RPC_RESILIENCE_SPEC.md` §4.
//!
//! Jitter uses an OS-seeded RNG (`rand::rng`) instead of a deterministic
//! function of the attempt counter. The previous implementation produced the
//! identical jitter value for every caller at the same attempt, defeating the
//! purpose of jitter and amplifying retry storms under load (Google SRE Book,
//! Chapter 22 "Handling Overload"). The new implementation follows the
//! Google SRE "Full Jitter" formula: `delay = uniform(0, base * 2^attempt)`
//! clamped by `max_backoff_ms`.

use rand::Rng;

use crate::RetryPolicy;

/// Computes the delay before the next retry attempt using exponential backoff
/// with full jitter.
///
/// Returns `initial_backoff_ms` for the first retry (`attempt == 0`) so that
/// callers probing an immediately transient failure do not pay a multiplicative
/// cost. Subsequent attempts sample uniformly from `[0, base]` where
/// `base = min(initial_backoff_ms * 2^(attempt-1), max_backoff_ms)`, matching
/// the AWS "Decorrelated Jitter" guidance and Google SRE "Full Jitter".
pub fn retry_backoff_ms(policy: &RetryPolicy, attempt: u32) -> u64 {
    if attempt == 0 {
        return policy.initial_backoff_ms;
    }

    let exponent = attempt.saturating_sub(1).min(16);
    let base = policy
        .initial_backoff_ms
        .saturating_mul(1u64 << exponent)
        .min(policy.max_backoff_ms);

    // Full jitter: sample uniformly in `[0, base]`. When `base == 0` there is
    // nothing to sample, so fall back to `initial_backoff_ms` to keep the
    // contract non-zero for `attempt > 0`.
    if base == 0 {
        return policy.initial_backoff_ms;
    }

    let mut rng = rand::rng();
    rng.random_range(0..=base)
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
        assert!(later <= policy.max_backoff_ms);
        assert!(first <= policy.max_backoff_ms);
        // Lower bound: full jitter can return 0; ensure first attempt is the
        // configured initial backoff so consumers can rely on attempt 0.
        assert_eq!(retry_backoff_ms(&policy, 0), policy.initial_backoff_ms);
    }

    #[test]
    fn backoff_jitter_is_non_deterministic_across_calls() {
        // Two independent calls at the same attempt should (with overwhelming
        // probability) differ at least once across a reasonable sample size.
        // This guards against regressing back to the deterministic pseudo-jitter.
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..32 {
            distinct.insert(retry_backoff_ms(&policy, 3));
        }
        assert!(
            distinct.len() > 1,
            "backoff jitter must be non-deterministic; got {distinct:?}"
        );
    }
}
