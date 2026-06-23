//! Outbound RPC metadata providers per `RPC_FRAMEWORK_SPEC.md` client pipeline stage 1.

use sdkwork_rpc_framework_core::{
    RPC_ACCESS_TOKEN_METADATA, RPC_AUTHORIZATION_METADATA, RPC_IDEMPOTENCY_KEY_METADATA,
    RPC_REQUEST_HASH_METADATA, RPC_REQUEST_ID_METADATA, RPC_TRACEPARENT_METADATA,
};
use sdkwork_utils_rust::is_blank;
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RpcCallMetadata {
    pub authorization: Option<String>,
    pub access_token: Option<String>,
    pub request_id: Option<String>,
    pub traceparent: Option<String>,
    pub idempotency_key: Option<String>,
    pub request_hash: Option<String>,
}

impl RpcCallMetadata {
    pub fn has_idempotency_key(&self) -> bool {
        !is_blank(self.idempotency_key.as_deref())
    }

    pub fn apply_to(&self, metadata: &mut MetadataMap) {
        insert_optional(metadata, RPC_AUTHORIZATION_METADATA, &self.authorization);
        insert_optional(metadata, RPC_ACCESS_TOKEN_METADATA, &self.access_token);
        insert_optional(metadata, RPC_REQUEST_ID_METADATA, &self.request_id);
        insert_optional(metadata, RPC_TRACEPARENT_METADATA, &self.traceparent);
        insert_optional(metadata, RPC_IDEMPOTENCY_KEY_METADATA, &self.idempotency_key);
        insert_optional(metadata, RPC_REQUEST_HASH_METADATA, &self.request_hash);
    }
}

fn insert_optional(metadata: &mut MetadataMap, key: &str, value: &Option<String>) {
    let Some(value) = value else {
        return;
    };
    if is_blank(Some(value)) {
        return;
    }
    if let (Ok(parsed_key), Ok(parsed_value)) = (
        MetadataKey::<Ascii>::from_bytes(key.as_bytes()),
        MetadataValue::try_from(value.as_str()),
    ) {
        metadata.insert(parsed_key, parsed_value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_standard_metadata_keys() {
        let mut map = MetadataMap::new();
        RpcCallMetadata {
            request_id: Some("req-1".to_string()),
            traceparent: Some("00-abc-def-01".to_string()),
            idempotency_key: Some("idem-1".to_string()),
            ..RpcCallMetadata::default()
        }
        .apply_to(&mut map);

        assert_eq!(
            map.get(RPC_REQUEST_ID_METADATA).map(|v| v.to_str().ok()),
            Some(Some("req-1"))
        );
        assert!(RpcCallMetadata {
            idempotency_key: Some("idem-1".to_string()),
            ..RpcCallMetadata::default()
        }
        .has_idempotency_key());
    }
}
