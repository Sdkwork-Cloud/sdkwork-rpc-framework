use sdkwork_rpc_client::{
    pick_endpoint, LoadBalanceAlgorithm, ResolvedEndpoint, RoundRobinCursor,
};
use sdkwork_rpc_resilience::{
    retry_backoff_ms, should_retry_call, CircuitBreaker, CircuitBreakerConfig,
    CircuitBreakerState, RetryAdmission, RetryBudgetTracker, RetryPolicy,
};
use sdkwork_rpc_framework_core::{
    ResilienceProfile, RpcSurface, BOOTSTRAP_STAGES, SHUTDOWN_STAGES,
    STAGE_DEREGISTER_DISCOVERY_INSTANCE, STAGE_INITIALIZE_RPC_FRAMEWORK, validate_rpc_surface,
};
use std::time::Duration;
use tonic::Code;

#[test]
fn resilience_profiles_match_retry_whitelists() {
    let default_policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
    assert!(default_policy.allows_retry(Code::Unavailable, 1));
    assert!(!default_policy.allows_retry(Code::InvalidArgument, 1));

    let critical_policy = RetryPolicy::for_profile(ResilienceProfile::RpcCriticalWrite);
    assert!(!critical_policy.allows_retry(Code::Unavailable, 10));
}

#[test]
fn retry_budget_exhaustion_fails_fast() {
    let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
    let mut budget = RetryBudgetTracker::new(1);
    assert!(policy.should_retry(Code::Unavailable, 1, &mut budget));
    assert!(!policy.should_retry(Code::Unavailable, 2, &mut budget));
}

#[test]
fn retry_backoff_is_monotonic_and_bounded() {
    let policy = RetryPolicy::for_profile(ResilienceProfile::RpcReadOnly);
    let first = retry_backoff_ms(&policy, 1);
    let later = retry_backoff_ms(&policy, 3);
    assert!(later >= first);
    assert!(later <= policy.max_backoff_ms);
}

#[test]
fn circuit_breaker_opens_and_recovers() {
    let mut breaker = CircuitBreaker::new(CircuitBreakerConfig {
        failure_threshold: 2,
        recovery_timeout: Duration::from_millis(1),
        half_open_max_probes: 1,
    });

    breaker.record_failure();
    breaker.record_failure();
    assert_eq!(breaker.state(), CircuitBreakerState::Open);
    assert!(!breaker.allow_request());

    std::thread::sleep(Duration::from_millis(2));
    assert!(breaker.allow_request());
    breaker.record_success();
    assert_eq!(breaker.state(), CircuitBreakerState::Closed);
}

#[test]
fn load_balancer_prefers_healthy_endpoints() {
    let endpoints = vec![
        ResolvedEndpoint {
            endpoint: "grpc://bad:1".to_string(),
            weight: 100,
            healthy: false,
        },
        ResolvedEndpoint {
            endpoint: "grpc://good:2".to_string(),
            weight: 100,
            healthy: true,
        },
    ];
    let mut cursor = RoundRobinCursor::default();
    let picked = pick_endpoint(&endpoints, LoadBalanceAlgorithm::PickFirst, &mut cursor)
        .expect("endpoint");
    assert_eq!(picked.endpoint, "grpc://good:2");
}

#[test]
fn idempotency_admission_blocks_unsafe_retries() {
    let policy = RetryPolicy::for_profile(ResilienceProfile::RpcIdempotentWrite);
    let mut budget = RetryBudgetTracker::new(10);
    let blocked = should_retry_call(
        &policy,
        tonic::Code::Unavailable,
        1,
        &mut budget,
        RetryAdmission {
            method_requires_idempotency: true,
            has_idempotency_key: false,
        },
    );
    assert!(!blocked);
}

#[test]
fn bootstrap_and_shutdown_stage_names_are_canonical() {
    assert!(BOOTSTRAP_STAGES.contains(&STAGE_INITIALIZE_RPC_FRAMEWORK));
    assert!(SHUTDOWN_STAGES.contains(&STAGE_DEREGISTER_DISCOVERY_INSTANCE));
    assert_eq!(validate_rpc_surface("backend").unwrap(), RpcSurface::Backend);
}
