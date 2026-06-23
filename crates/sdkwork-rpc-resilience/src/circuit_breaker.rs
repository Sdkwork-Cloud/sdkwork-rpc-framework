//! Per-target circuit breaker per `RPC_RESILIENCE_SPEC.md` section 5.

use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
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

#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    consecutive_failures: u32,
    half_open_probes: u32,
    opened_at: Option<Instant>,
    state: CircuitBreakerState,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            consecutive_failures: 0,
            half_open_probes: 0,
            opened_at: None,
            state: CircuitBreakerState::Closed,
        }
    }

    pub fn state(&mut self) -> CircuitBreakerState {
        self.refresh_state();
        self.state
    }

    pub fn allow_request(&mut self) -> bool {
        self.refresh_state();
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => false,
            CircuitBreakerState::HalfOpen => self.half_open_probes < self.config.half_open_max_probes,
        }
    }

    pub fn record_success(&mut self) {
        self.refresh_state();
        self.consecutive_failures = 0;
        self.half_open_probes = 0;
        self.opened_at = None;
        self.state = CircuitBreakerState::Closed;
    }

    pub fn record_failure(&mut self) {
        self.refresh_state();
        if self.state == CircuitBreakerState::HalfOpen {
            self.open();
            return;
        }

        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures >= self.config.failure_threshold {
            self.open();
        }
    }

    fn open(&mut self) {
        self.state = CircuitBreakerState::Open;
        self.opened_at = Some(Instant::now());
        self.half_open_probes = 0;
    }

    fn refresh_state(&mut self) {
        if self.state != CircuitBreakerState::Open {
            return;
        }

        let Some(opened_at) = self.opened_at else {
            return;
        };

        if opened_at.elapsed() >= self.config.recovery_timeout {
            self.state = CircuitBreakerState::HalfOpen;
            self.half_open_probes = 0;
            self.consecutive_failures = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_after_failure_threshold_and_recovers_in_half_open() {
        let mut breaker = CircuitBreaker::new(CircuitBreakerConfig {
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
}
