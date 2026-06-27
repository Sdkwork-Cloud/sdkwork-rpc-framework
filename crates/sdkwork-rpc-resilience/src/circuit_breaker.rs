//! Per-target circuit breaker per `RPC_RESILIENCE_SPEC.md` §5.
//!
//! `CircuitBreaker` uses lock-free atomics so a single instance can be shared
//! (directly or via `Arc`) across all concurrent RPC calls targeting the same
//! `service_name`. Half-open probe admission is enforced atomically to honor the
//! "single trial request before full recovery" guidance in §5.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use tracing::{info, warn};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreakerState {
    pub const CLOSED_U8: u8 = 0;
    pub const OPEN_U8: u8 = 1;
    pub const HALF_OPEN_U8: u8 = 2;

    fn from_u8(value: u8) -> Self {
        match value {
            Self::OPEN_U8 => Self::Open,
            Self::HALF_OPEN_U8 => Self::HalfOpen,
            _ => Self::Closed,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub recovery_timeout: Duration,
    pub half_open_max_probes: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(30),
            half_open_max_probes: 1,
        }
    }
}

/// Per-target circuit breaker with lock-free concurrent-safe state transitions.
///
/// All mutating methods take `&self` and use atomics, so a single breaker (or
/// `Arc<CircuitBreaker>`) can be shared across all concurrent calls targeting
/// the same service as required by `RPC_RESILIENCE_SPEC.md` §5. State
/// transitions emit `tracing` events so operations can observe breaker health.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: AtomicU8,
    consecutive_failures: AtomicU32,
    half_open_probes: AtomicU32,
    /// Elapsed nanos since `created_at` when the breaker opened. Stored as
    /// `AtomicU64` because `Instant` itself is not atomically storable.
    opened_at_nanos: AtomicU64,
    created_at: Instant,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: AtomicU8::new(CircuitBreakerState::CLOSED_U8),
            consecutive_failures: AtomicU32::new(0),
            half_open_probes: AtomicU32::new(0),
            opened_at_nanos: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    /// Returns the breaker state after applying time-based recovery transitions.
    pub fn state(&self) -> CircuitBreakerState {
        self.refresh_state();
        CircuitBreakerState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Returns true when a request is allowed to proceed.
    ///
    /// In `HalfOpen` state a probe slot is atomically reserved against
    /// `half_open_max_probes`; once the limit is reached additional probes
    /// are rejected until `record_success`/`record_failure` resolves the
    /// trial. This fixes the previous bug where the probe counter was never
    /// incremented and concurrent requests all bypassed the half-open gate.
    pub fn allow_request(&self) -> bool {
        self.refresh_state();
        let current = CircuitBreakerState::from_u8(self.state.load(Ordering::Acquire));
        match current {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => false,
            CircuitBreakerState::HalfOpen => self.acquire_half_open_probe(),
        }
    }

    fn acquire_half_open_probe(&self) -> bool {
        let max_probes = self.config.half_open_max_probes;
        loop {
            let observed = self.half_open_probes.load(Ordering::Acquire);
            if observed >= max_probes {
                return false;
            }
            let result = self.half_open_probes.compare_exchange(
                observed,
                observed.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            if result.is_ok() {
                return true;
            }
            // Lost the race; retry once more so concurrent callers do not
            // overshoot the configured probe budget.
        }
    }

    pub fn record_success(&self) {
        self.refresh_state();
        let prev = self
            .state
            .swap(CircuitBreakerState::CLOSED_U8, Ordering::AcqRel);
        self.consecutive_failures.store(0, Ordering::Release);
        self.half_open_probes.store(0, Ordering::Release);
        self.opened_at_nanos.store(0, Ordering::Release);
        if CircuitBreakerState::from_u8(prev) != CircuitBreakerState::Closed {
            info!(
                target: "sdkwork.rpc.circuit_breaker",
                state = "closed",
                "circuit breaker closed after recovery"
            );
        }
    }

    pub fn record_failure(&self) {
        self.refresh_state();
        let current = CircuitBreakerState::from_u8(self.state.load(Ordering::Acquire));
        if current == CircuitBreakerState::HalfOpen {
            self.open();
            return;
        }

        let failures = self
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if failures >= self.config.failure_threshold {
            self.open();
        }
    }

    fn open(&self) {
        let prev = self
            .state
            .swap(CircuitBreakerState::OPEN_U8, Ordering::AcqRel);
        self.opened_at_nanos
            .store(self.created_at.elapsed().as_nanos() as u64, Ordering::Release);
        self.half_open_probes.store(0, Ordering::Release);
        if CircuitBreakerState::from_u8(prev) != CircuitBreakerState::Open {
            warn!(
                target: "sdkwork.rpc.circuit_breaker",
                state = "open",
                threshold = self.config.failure_threshold,
                "circuit breaker opened after threshold failures"
            );
        }
    }

    fn refresh_state(&self) {
        let observed = self.state.load(Ordering::Acquire);
        if CircuitBreakerState::from_u8(observed) != CircuitBreakerState::Open {
            return;
        }

        let opened_nanos = self.opened_at_nanos.load(Ordering::Acquire);
        if opened_nanos == 0 {
            return;
        }

        let opened_at = self.created_at + Duration::from_nanos(opened_nanos);
        if opened_at.elapsed() < self.config.recovery_timeout {
            return;
        }

        let prev = self.state.compare_exchange(
            observed,
            CircuitBreakerState::HALF_OPEN_U8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if prev.is_ok() {
            self.half_open_probes.store(0, Ordering::Release);
            self.consecutive_failures.store(0, Ordering::Release);
            info!(
                target: "sdkwork.rpc.circuit_breaker",
                state = "half_open",
                "circuit breaker entered half-open after recovery timeout"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn opens_after_failure_threshold_and_recovers_in_half_open() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout: Duration::from_millis(1),
            half_open_max_probes: 1,
        });

        breaker.record_failure();
        assert!(breaker.allow_request());
        breaker.record_failure();
        assert!(!breaker.allow_request());

        std::thread::sleep(Duration::from_millis(2));
        assert!(breaker.allow_request());
        assert_eq!(breaker.state(), CircuitBreakerState::HalfOpen);

        breaker.record_success();
        assert_eq!(breaker.state(), CircuitBreakerState::Closed);
    }

    #[test]
    fn half_open_rejects_excess_probes_until_resolution() {
        // Regression test for the previously undetected bug where the
        // half-open probe counter was never incremented, allowing every
        // concurrent request to bypass the half-open gate (see §5).
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(1),
            half_open_max_probes: 1,
        });

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitBreakerState::Open);

        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(breaker.state(), CircuitBreakerState::HalfOpen);

        // First probe acquires the single slot; second must be rejected.
        assert!(breaker.allow_request());
        assert!(
            !breaker.allow_request(),
            "half-open must reject excess probes while a trial is in flight"
        );

        // Failure during half-open reopens; next probe must wait for recovery.
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitBreakerState::Open);
        assert!(!breaker.allow_request());
    }

    #[test]
    fn half_open_allows_multiple_probes_when_configured() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(1),
            half_open_max_probes: 3,
        });

        breaker.record_failure();
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(breaker.state(), CircuitBreakerState::HalfOpen);

        assert!(breaker.allow_request());
        assert!(breaker.allow_request());
        assert!(breaker.allow_request());
        assert!(
            !breaker.allow_request(),
            "fourth probe must be rejected when max_probes is three"
        );

        breaker.record_success();
        assert_eq!(breaker.state(), CircuitBreakerState::Closed);
    }

    #[test]
    fn shared_breaker_is_safe_across_threads() {
        let breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 100,
            recovery_timeout: Duration::from_secs(60),
            half_open_max_probes: 1,
        }));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let b = Arc::clone(&breaker);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    b.record_failure();
                    let _ = b.allow_request();
                }
            }));
        }
        for handle in handles {
            handle.join().expect("breaker thread");
        }

        // Final state must be Open and observable without UB.
        assert_eq!(breaker.state(), CircuitBreakerState::Open);
    }
}
