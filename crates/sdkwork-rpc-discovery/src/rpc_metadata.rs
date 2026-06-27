//! Discovery RPC metadata construction.
//!
//! Builds the per-call metadata headers required by `SECURITY_SPEC.md` and
//! `DISCOVERY_SPEC.md` §11: subject identity, registry permission, request id,
//! and a W3C Trace Context `traceparent`.
//!
//! Subject identity is resolved through [`SubjectTokenProvider`] so production
//! deployments inject signed bearer tokens, while dev/test uses
//! [`UnsignedLocalSubject`] (the `allow_unsigned_local_context` loopback-only
//! mode that `DISCOVERY_SPEC.md` §11 forbids in production).

use rand::Rng;
use sdkwork_rpc_framework_core::{RPC_REQUEST_ID_METADATA, RPC_TRACEPARENT_METADATA};
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use uuid::Uuid;

/// Resolves the caller subject identity carried in discovery RPC metadata.
///
/// Production deployments inject a signed bearer token provider (e.g., a JWT
/// minted by the IAM control plane). Dev/test uses [`UnsignedLocalSubject`].
/// The resolved token is placed in the `x-sdkwork-subject-id` header.
pub trait SubjectTokenProvider: Send + Sync {
    fn subject_token(&self) -> String;
}

/// Dev/test-only plaintext subject identity.
///
/// Corresponds to `allow_unsigned_local_context` in `DISCOVERY_SPEC.md` §11,
/// which `MUST NOT` be enabled in production. Use a signed token provider in
/// cloud and multi-instance deployments.
pub struct UnsignedLocalSubject {
    subject_id: String,
}

impl UnsignedLocalSubject {
    pub fn new(subject_id: impl Into<String>) -> Self {
        Self {
            subject_id: subject_id.into(),
        }
    }
}

impl SubjectTokenProvider for UnsignedLocalSubject {
    fn subject_token(&self) -> String {
        self.subject_id.clone()
    }
}

/// Builds registry read metadata using a subject token provider.
pub fn registry_read_metadata(provider: &dyn SubjectTokenProvider) -> Vec<(String, String)> {
    registry_metadata(provider, "read")
}

/// Builds registry write metadata using a subject token provider.
pub fn registry_write_metadata(provider: &dyn SubjectTokenProvider) -> Vec<(String, String)> {
    registry_metadata(provider, "write")
}

/// Dev-only convenience: builds read metadata from a plaintext subject id.
/// Equivalent to `registry_read_metadata(&UnsignedLocalSubject::new(subject_id))`.
pub fn unsigned_registry_read_metadata(subject_id: &str) -> Vec<(String, String)> {
    registry_read_metadata(&UnsignedLocalSubject::new(subject_id))
}

/// Dev-only convenience: builds write metadata from a plaintext subject id.
/// Equivalent to `registry_write_metadata(&UnsignedLocalSubject::new(subject_id))`.
pub fn unsigned_registry_write_metadata(subject_id: &str) -> Vec<(String, String)> {
    registry_write_metadata(&UnsignedLocalSubject::new(subject_id))
}

pub fn apply_metadata_template(target: &mut MetadataMap, template: &[(String, String)]) {
    for (key, value) in template {
        insert_ascii(target, key, value);
    }
}

fn registry_metadata(
    provider: &dyn SubjectTokenProvider,
    permission: &str,
) -> Vec<(String, String)> {
    vec![
        (
            "x-sdkwork-subject-id".to_string(),
            provider.subject_token(),
        ),
        (
            "x-sdkwork-registry-permissions".to_string(),
            permission.to_string(),
        ),
        (
            RPC_REQUEST_ID_METADATA.to_string(),
            Uuid::new_v4().to_string(),
        ),
        (
            RPC_TRACEPARENT_METADATA.to_string(),
            build_traceparent(),
        ),
    ]
}

/// Builds a W3C Trace Context `traceparent` header value.
///
/// Format: `version-trace_id-parent_id-trace_flags` per
/// <https://www.w3.org/TR/trace-context/#traceparent-header>.
///
/// - `version`: `00`
/// - `trace_id`: 32 lowercase hex chars (UUIDv4 simple; never all-zero because
///   the version nibble is `4`)
/// - `parent_id`: exactly 16 lowercase hex chars (8 bytes). The previous
///   implementation used a full UUIDv4 simple (32 chars), which violates the
///   W3C requirement that `parent_id` be 16 hex chars.
/// - `trace_flags`: `01` (recorded/sampled)
fn build_traceparent() -> String {
    let trace_id = Uuid::new_v4().simple().to_string();
    let mut rng = rand::rng();
    // parent_id MUST be exactly 16 hex chars. Generate a u64 and format as
    // zero-padded lowercase hex. OR-ing the low bit guarantees a non-zero
    // value (W3C forbids all-zero parent_id) without a retry loop.
    let parent_id_val = rng.random::<u64>() | 1;
    let parent_id = format!("{parent_id_val:016x}");
    format!("00-{trace_id}-{parent_id}-01")
}

fn insert_ascii(metadata: &mut MetadataMap, key: &str, value: &str) {
    if let (Ok(parsed_key), Ok(parsed_value)) = (
        MetadataKey::<Ascii>::from_bytes(key.as_bytes()),
        MetadataValue::try_from(value),
    ) {
        metadata.insert(parsed_key, parsed_value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traceparent_is_w3c_compliant() {
        let traceparent = build_traceparent();
        let parts: Vec<&str> = traceparent.split('-').collect();
        assert_eq!(parts.len(), 4, "traceparent must have 4 dash-separated fields");
        assert_eq!(parts[0], "00", "version must be 00");
        assert_eq!(parts[1].len(), 32, "trace_id must be 32 hex chars");
        assert_eq!(parts[2].len(), 16, "parent_id must be 16 hex chars");
        assert_eq!(parts[3], "01", "trace_flags must be 01");
        assert!(
            parts[1].chars().all(|c| c.is_ascii_hexdigit()),
            "trace_id must be lowercase hex"
        );
        assert!(
            parts[2].chars().all(|c| c.is_ascii_hexdigit()),
            "parent_id must be lowercase hex"
        );
        assert_ne!(parts[1], "00000000000000000000000000000000", "trace_id must not be all-zero");
        assert_ne!(parts[2], "0000000000000000", "parent_id must not be all-zero");
    }

    #[test]
    fn traceparent_parent_id_is_never_32_chars() {
        // Regression: the old implementation produced a 32-char parent_id from a
        // full UUID. Sample many values to confirm the fix holds.
        for _ in 0..256 {
            let traceparent = build_traceparent();
            let parent_id = traceparent.split('-').nth(2).expect("parent_id field");
            assert_eq!(parent_id.len(), 16, "parent_id must be 16 hex chars, got {parent_id}");
        }
    }

    #[test]
    fn unsigned_subject_provider_returns_plaintext() {
        let provider = UnsignedLocalSubject::new("svc-1");
        assert_eq!(provider.subject_token(), "svc-1");
    }

    #[test]
    fn registry_metadata_includes_required_headers() {
        let provider = UnsignedLocalSubject::new("svc-1");
        let metadata = registry_read_metadata(&provider);
        let keys: Vec<&str> = metadata.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"x-sdkwork-subject-id"));
        assert!(keys.contains(&"x-sdkwork-registry-permissions"));
        assert!(keys.contains(&"x-request-id"));
        assert!(keys.contains(&"traceparent"));
    }

    #[test]
    fn unsigned_convenience_wrappers_match_provider_api() {
        let provider = UnsignedLocalSubject::new("svc-1");
        let via_provider = registry_read_metadata(&provider);
        let via_convenience = unsigned_registry_read_metadata("svc-1");
        // Request id and traceparent are random; compare only the stable keys.
        assert_eq!(
            via_provider[0], via_convenience[0],
            "subject-id header must match"
        );
        assert_eq!(
            via_provider[1], via_convenience[1],
            "permissions header must match"
        );
    }
}
