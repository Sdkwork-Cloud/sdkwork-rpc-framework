use sdkwork_rpc_client::{
    pick_endpoint, LoadBalanceAlgorithm, ResolvedEndpoint, RetryBudgetConfig, RetryBudgetRegistry,
    RoundRobinCursor,
};
use sdkwork_rpc_resilience::{
    extract_retry_pushback_ms, retry_backoff_ms, should_retry_call, should_retry_call_with_pushback,
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState, RetryAdmission, RetryBudgetTracker,
    RetryDecision, RetryPolicy, GRPC_RETRY_PUSHBACK_MS,
};
use sdkwork_rpc_framework_core::{
    ResilienceProfile, RpcSurface, BOOTSTRAP_STAGES, SHUTDOWN_STAGES,
    STAGE_DEREGISTER_DISCOVERY_INSTANCE, STAGE_INITIALIZE_RPC_FRAMEWORK, validate_rpc_surface,
};
use std::time::Duration;
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};

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
    let breaker = CircuitBreaker::new(CircuitBreakerConfig {
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

#[test]
fn pushback_governs_resource_exhausted_retry() {
    // Per RPC_RESILIENCE_SPEC.md §4.1, RESOURCE_EXHAUSTED is retryable ONLY
    // when the server signals grpc-retry-pushback-ms. Without pushback the
    // decision MUST be Stop; with pushback it MUST be Retry using the clamped
    // pushback value as backoff.
    let policy = RetryPolicy::for_profile(ResilienceProfile::RpcReadOnly);
    let admission = RetryAdmission {
        method_requires_idempotency: false,
        has_idempotency_key: false,
    };

    let mut no_pushback_metadata = MetadataMap::new();
    let no_pushback = Status::with_metadata(
        Code::ResourceExhausted,
        "no pushback",
        std::mem::take(&mut no_pushback_metadata),
    );
    let mut budget = RetryBudgetTracker::new(10);
    assert_eq!(
        should_retry_call_with_pushback(
            &policy,
            &no_pushback,
            1,
            &mut budget,
            admission,
            Duration::from_secs(10),
        ),
        RetryDecision::Stop
    );

    let mut pushback_metadata = MetadataMap::new();
    pushback_metadata.insert(GRPC_RETRY_PUSHBACK_MS, "120".parse().unwrap());
    let with_pushback = Status::with_metadata(
        Code::ResourceExhausted,
        "pushback",
        pushback_metadata,
    );
    assert_eq!(extract_retry_pushback_ms(&with_pushback), Some(120));
    match should_retry_call_with_pushback(
        &policy,
        &with_pushback,
        1,
        &mut budget,
        admission,
        Duration::from_secs(10),
    ) {
        RetryDecision::Retry { backoff_ms } => assert_eq!(backoff_ms, 120),
        RetryDecision::Stop => panic!("pushback present ⇒ must retry"),
    }
}

#[test]
fn cross_call_retry_budget_fails_fast_per_service() {
    // Per RPC_RESILIENCE_SPEC.md §4.2, when the cross-call budget is exhausted
    // the caller MUST fail fast. The registry isolates budgets per service.
    let registry = RetryBudgetRegistry::new(RetryBudgetConfig {
        capacity: 1,
        refill_per_second: 0.0,
    });
    assert!(registry.try_acquire("billing-svc", "ChargeOrder"),);
    assert!(
        !registry.try_acquire("billing-svc", "ChargeOrder"),
        "exhausted billing-svc budget must fail fast"
    );
    assert!(
        registry.try_acquire("catalog-svc", "ListItems"),
        "catalog-svc budget must be independent of billing-svc"
    );
}
