//! Load balancing over resolver endpoint snapshots per `RPC_RESILIENCE_SPEC.md` section 6.

use crate::resolver::ResolvedEndpoint;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoadBalanceAlgorithm {
    PickFirst,
    RoundRobin,
    Weighted,
}

#[derive(Clone, Debug, Default)]
pub struct RoundRobinCursor {
    next: usize,
}

/// Picks a target endpoint from a resolver snapshot.
///
/// Healthy endpoints are preferred. When every candidate is unhealthy the full
/// snapshot is used as a last resort so callers can surface configuration errors.
pub fn pick_endpoint<'a>(
    endpoints: &'a [ResolvedEndpoint],
    algorithm: LoadBalanceAlgorithm,
    cursor: &mut RoundRobinCursor,
) -> Option<&'a ResolvedEndpoint> {
    if endpoints.is_empty() {
        return None;
    }

    let healthy: Vec<&ResolvedEndpoint> = endpoints.iter().filter(|e| e.healthy).collect();
    let candidates: Vec<&ResolvedEndpoint> = if healthy.is_empty() {
        endpoints.iter().collect()
    } else {
        healthy
    };

    match algorithm {
        LoadBalanceAlgorithm::PickFirst => candidates.first().copied(),
        LoadBalanceAlgorithm::RoundRobin => {
            let index = cursor.next % candidates.len();
            cursor.next = cursor.next.saturating_add(1);
            Some(candidates[index])
        }
        LoadBalanceAlgorithm::Weighted => pick_weighted(&candidates, cursor),
    }
}

fn pick_weighted<'a>(
    candidates: &[&'a ResolvedEndpoint],
    cursor: &mut RoundRobinCursor,
) -> Option<&'a ResolvedEndpoint> {
    let total_weight: u64 = candidates.iter().map(|e| e.weight.max(1) as u64).sum();
    if total_weight == 0 {
        return candidates.first().copied();
    }

    let slot = (cursor.next as u64) % total_weight;
    cursor.next = cursor.next.saturating_add(1);

    let mut accumulated = 0u64;
    for endpoint in candidates {
        accumulated += endpoint.weight.max(1) as u64;
        if slot < accumulated {
            return Some(endpoint);
        }
    }

    candidates.last().copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(uri: &str, weight: u32, healthy: bool) -> ResolvedEndpoint {
        ResolvedEndpoint {
            endpoint: uri.to_string(),
            weight,
            healthy,
        }
    }

    #[test]
    fn pick_first_prefers_healthy_endpoint() {
        let endpoints = vec![
            endpoint("grpc://unhealthy:1", 100, false),
            endpoint("grpc://healthy:2", 100, true),
        ];
        let mut cursor = RoundRobinCursor::default();
        let picked = pick_endpoint(&endpoints, LoadBalanceAlgorithm::PickFirst, &mut cursor)
            .expect("endpoint");
        assert_eq!(picked.endpoint, "grpc://healthy:2");
    }

    #[test]
    fn round_robin_cycles_through_healthy_endpoints() {
        let endpoints = vec![
            endpoint("grpc://a:1", 100, true),
            endpoint("grpc://b:2", 100, true),
        ];
        let mut cursor = RoundRobinCursor::default();
        let first = pick_endpoint(&endpoints, LoadBalanceAlgorithm::RoundRobin, &mut cursor)
            .expect("first")
            .endpoint
            .clone();
        let second = pick_endpoint(&endpoints, LoadBalanceAlgorithm::RoundRobin, &mut cursor)
            .expect("second")
            .endpoint
            .clone();
        let third = pick_endpoint(&endpoints, LoadBalanceAlgorithm::RoundRobin, &mut cursor)
            .expect("third")
            .endpoint
            .clone();
        assert_ne!(first, second);
        assert_eq!(first, third);
    }

    #[test]
    fn weighted_honors_higher_weight() {
        let endpoints = vec![
            endpoint("grpc://light:1", 1, true),
            endpoint("grpc://heavy:2", 9, true),
        ];
        let mut cursor = RoundRobinCursor::default();
        let mut heavy_hits = 0;
        for _ in 0..100 {
            let picked = pick_endpoint(&endpoints, LoadBalanceAlgorithm::Weighted, &mut cursor)
                .expect("endpoint");
            if picked.endpoint == "grpc://heavy:2" {
                heavy_hits += 1;
            }
        }
        assert!(heavy_hits > 50);
    }
}
