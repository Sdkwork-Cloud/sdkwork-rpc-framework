//! Shared endpoint normalization for discovery HTTP/gRPC transports.

use sdkwork_utils_rust::{is_blank, trim};

/// Normalizes a discovery control-plane HTTP endpoint for tonic transport.
pub fn normalize_http_endpoint(endpoint: &str) -> String {
    let trimmed = trim(endpoint);
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed
    } else {
        format!("http://{trimmed}")
    }
}

/// Normalizes a gRPC data-plane advertised endpoint.
pub fn normalize_grpc_endpoint(bind_addr: &str) -> String {
    let trimmed = trim(bind_addr);
    if trimmed.starts_with("grpc://") || trimmed.starts_with("grpcs://") {
        trimmed
    } else {
        format!("grpc://{trimmed}")
    }
}

pub fn require_non_blank(value: &str, field: &str) -> Result<(), String> {
    if is_blank(Some(value)) {
        Err(format!("{field} is required"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_http_endpoint_without_scheme() {
        assert_eq!(
            normalize_http_endpoint("127.0.0.1:19090"),
            "http://127.0.0.1:19090"
        );
    }

    #[test]
    fn normalizes_grpc_endpoint_without_scheme() {
        assert_eq!(
            normalize_grpc_endpoint("127.0.0.1:50051"),
            "grpc://127.0.0.1:50051"
        );
    }
}
