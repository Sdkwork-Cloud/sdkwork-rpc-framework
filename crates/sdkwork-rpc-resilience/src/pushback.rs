//! gRPC server retry pushback parsing per `RPC_RESILIENCE_SPEC.md` §4.1 and
//! the gRPC A6 client-side retry spec.
//!
//! When a server returns `RESOURCE_EXHAUSTED` with the `grpc-retry-pushback-ms`
//! trailer, it signals that the client MAY retry after the indicated delay
//! (clamped to the policy's `max_backoff_ms`). Absence of the trailer on a
//! `RESOURCE_EXHAUSTED` status means the server did not signal retryability
//! and the client MUST NOT retry. For non-`RESOURCE_EXHAUSTED` codes the
//! pushback trailer is ignored and the standard policy whitelist applies.

use std::time::Duration;

use tonic::{Code, Status};
use tracing::warn;

use crate::{
    deadline_covers_backoff, retry_backoff_ms, RetryAdmission, RetryBudgetTracker, RetryPolicy,
};

/// Metadata key used by gRPC servers to signal retry pushback (milliseconds).
///
/// Per the gRPC A6 client-side retry specification, the value is an ASCII
/// decimal number of milliseconds. Some implementations also emit the
/// `-bin` variant; this implementation reads the ASCII form which is the
/// interoperable default.
pub const GRPC_RETRY_PUSHBACK_MS: &str = "grpc-retry-pushback-ms";

/// Outcome of a pushback-aware retry evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    /// The call should not be retried.
    Stop,
    /// The call should be retried after sleeping for `backoff_ms`.
    Retry { backoff_ms: u64 },
}

/// Parses `grpc-retry-pushback-ms` from a gRPC status trailers.
///
/// Returns `Some(ms)` when the server signaled a retryable backoff. Returns
/// `None` when the trailer is absent, non-ASCII, or not a valid unsigned
/// integer. Per gRPC A6, a `None` result on a `RESOURCE_EXHAUSTED` status
/// means the server did not signal retryability and the client MUST NOT retry.
pub fn extract_retry_pushback_ms(status: &Status) -> Option<u64> {
    let value = status.metadata().get(GRPC_RETRY_PUSHBACK_MS)?;
    let text = value.to_str().ok()?;
    text.parse::<u64>().ok()
}

/// Resolves the effective backoff for the next retry attempt, honoring a
/// server-supplied pushback when present.
///
/// When `pushback_ms` is `Some`, the pushback value is clamped to the
/// policy's `max_backoff_ms`: servers can request up to the policy ceiling,
/// but values above the ceiling are capped to prevent a stalled server from
/// stretching retries beyond the caller's configured budget. When `None`,
/// the standard Full-Jitter backoff is computed from the policy.
pub fn effective_retry_backoff_ms(
    policy: &RetryPolicy,
    attempt: u32,
    pushback_ms: Option<u64>,
) -> u64 {
    match pushback_ms {
        Some(ms) => ms.min(policy.max_backoff_ms),
        None => retry_backoff_ms(policy, attempt),
    }
}

/// Evaluates retry eligibility with pushback awareness.
///
/// This is the canonical retry entry point for call sites that have access to
/// the gRPC [`Status`]. It refines [`crate::should_retry_call_with_deadline`]
/// in one way: for `RESOURCE_EXHAUSTED`, the call is retryable ONLY when the
/// server supplied `grpc-retry-pushback-ms` (per gRPC A6). For all other
/// retryable codes, pushback is ignored and the standard policy applies.
///
/// On `Retry`, the returned `backoff_ms` is the pushback value (clamped to
/// `max_backoff_ms`) when present, otherwise the computed Full-Jitter
/// backoff. The budget is consumed only when the decision is `Retry`.
pub fn should_retry_call_with_pushback(
    policy: &RetryPolicy,
    status: &Status,
    attempt: u32,
    budget: &mut RetryBudgetTracker,
    admission: RetryAdmission,
    remaining: Duration,
) -> RetryDecision {
    if !admission.allows_retry_metadata() {
        return RetryDecision::Stop;
    }
    if attempt >= policy.max_attempts {
        return RetryDecision::Stop;
    }

    let code = status.code();
    // Per RPC_RESILIENCE_SPEC.md §4.1, pushback governs RESOURCE_EXHAUSTED:
    // it both gates retryability (absent pushback ⇒ not retryable) and
    // supplies the backoff. For other retryable codes the pushback trailer
    // is ignored and the standard Full-Jitter backoff applies.
    let pushback_ms = if code == Code::ResourceExhausted {
        extract_retry_pushback_ms(status)
    } else {
        None
    };

    // RESOURCE_EXHAUSTED is retryable ONLY when the server signaled pushback.
    // This closes a gap where the profile whitelist unconditionally retried
    // RESOURCE_EXHAUSTED, ignoring the server's explicit non-retryability
    // signal (gRPC A6 / RPC_RESILIENCE_SPEC.md §4.1).
    let code_allowed = if code == Code::ResourceExhausted {
        pushback_ms.is_some() && policy.retryable_codes.contains(&code)
    } else {
        policy.retryable_codes.contains(&code)
    };
    if !code_allowed {
        return RetryDecision::Stop;
    }

    let backoff_ms = effective_retry_backoff_ms(policy, attempt, pushback_ms);
    if !deadline_covers_backoff(backoff_ms, remaining) {
        return RetryDecision::Stop;
    }

    if !budget.consume() {
        warn!(
            target: "sdkwork.rpc.retry.budget",
            attempt,
            "retry skipped: per-call retry budget exhausted"
        );
        return RetryDecision::Stop;
    }

    RetryDecision::Retry { backoff_ms }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryPolicy;
    use sdkwork_rpc_framework_core::ResilienceProfile;
    use tonic::metadata::MetadataMap;

    fn status_with(code: Code, pushback: Option<&str>) -> Status {
        let mut metadata = MetadataMap::new();
        if let Some(value) = pushback {
            metadata.insert(GRPC_RETRY_PUSHBACK_MS, value.parse().unwrap());
        }
        Status::with_metadata(code, "test", metadata)
    }

    #[test]
    fn extract_pushback_parses_valid_ascii_ms() {
        let status = status_with(Code::ResourceExhausted, Some("250"));
        assert_eq!(extract_retry_pushback_ms(&status), Some(250));
    }

    #[test]
    fn extract_pushback_returns_none_when_trailer_absent() {
        let status = status_with(Code::ResourceExhausted, None);
        assert_eq!(extract_retry_pushback_ms(&status), None);
    }

    #[test]
    fn extract_pushback_returns_none_for_non_numeric() {
        let status = status_with(Code::ResourceExhausted, Some("soon"));
        assert_eq!(extract_retry_pushback_ms(&status), None);
    }

    #[test]
    fn effective_backoff_uses_pushback_clamped_to_max() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcReadOnly);
        // max_backoff_ms for RpcReadOnly is 2_000
        assert_eq!(effective_retry_backoff_ms(&policy, 1, Some(100)), 100);
        assert_eq!(
            effective_retry_backoff_ms(&policy, 1, Some(10_000)),
            policy.max_backoff_ms
        );
    }

    #[test]
    fn effective_backoff_falls_back_to_jitter_when_pushback_absent() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        let backoff = effective_retry_backoff_ms(&policy, 0, None);
        assert_eq!(backoff, policy.initial_backoff_ms);
    }

    #[test]
    fn resource_exhausted_not_retryable_without_pushback() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcReadOnly);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: false,
            has_idempotency_key: false,
        };
        let status = status_with(Code::ResourceExhausted, None);
        assert_eq!(
            should_retry_call_with_pushback(
                &policy,
                &status,
                1,
                &mut budget,
                admission,
                Duration::from_secs(10),
            ),
            RetryDecision::Stop
        );
        assert_eq!(budget.remaining(), 10, "budget must not be consumed on Stop");
    }

    #[test]
    fn resource_exhausted_retryable_with_pushback() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcReadOnly);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: false,
            has_idempotency_key: false,
        };
        let status = status_with(Code::ResourceExhausted, Some("100"));
        match should_retry_call_with_pushback(
            &policy,
            &status,
            1,
            &mut budget,
            admission,
            Duration::from_secs(10),
        ) {
            RetryDecision::Retry { backoff_ms } => assert_eq!(backoff_ms, 100),
            RetryDecision::Stop => panic!("expected retry when pushback present"),
        }
        assert_eq!(budget.remaining(), 9);
    }

    #[test]
    fn unavailable_ignores_pushback_and_uses_jitter() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: false,
            has_idempotency_key: false,
        };
        // Pushback on UNAVAILABLE should be ignored (it only governs
        // RESOURCE_EXHAUSTED), and the standard jitter backoff applies.
        let status = status_with(Code::Unavailable, Some("99999"));
        match should_retry_call_with_pushback(
            &policy,
            &status,
            0,
            &mut budget,
            admission,
            Duration::from_secs(10),
        ) {
            RetryDecision::Retry { backoff_ms } => {
                assert_eq!(backoff_ms, policy.initial_backoff_ms);
            }
            RetryDecision::Stop => panic!("expected retry for unavailable"),
        }
        assert_eq!(budget.remaining(), 9);
    }

    #[test]
    fn pushback_skips_when_deadline_insufficient() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcReadOnly);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: false,
            has_idempotency_key: false,
        };
        let status = status_with(Code::ResourceExhausted, Some("100"));
        assert_eq!(
            should_retry_call_with_pushback(
                &policy,
                &status,
                1,
                &mut budget,
                admission,
                Duration::from_millis(1),
            ),
            RetryDecision::Stop
        );
        assert_eq!(budget.remaining(), 10);
    }

    #[test]
    fn idempotency_admission_still_enforced_with_pushback() {
        let policy = RetryPolicy::for_profile(ResilienceProfile::RpcIdempotentWrite);
        let mut budget = RetryBudgetTracker::new(10);
        let admission = RetryAdmission {
            method_requires_idempotency: true,
            has_idempotency_key: false,
        };
        // RpcIdempotentWrite does not whitelist ResourceExhausted, but even if
        // it did, admission must block first.
        let status = status_with(Code::Unavailable, None);
        assert_eq!(
            should_retry_call_with_pushback(
                &policy,
                &status,
                1,
                &mut budget,
                admission,
                Duration::from_secs(10),
            ),
            RetryDecision::Stop
        );
        assert_eq!(budget.remaining(), 10);
    }
}
