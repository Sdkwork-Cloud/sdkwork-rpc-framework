use sdkwork_rpc_framework_core::{RPC_REQUEST_ID_METADATA, RPC_TRACEPARENT_METADATA};
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use uuid::Uuid;

pub fn unsigned_registry_read_metadata(subject_id: &str) -> Vec<(String, String)> {
    unsigned_registry_metadata(subject_id, "read")
}

pub fn unsigned_registry_write_metadata(subject_id: &str) -> Vec<(String, String)> {
    unsigned_registry_metadata(subject_id, "write")
}

pub fn apply_metadata_template(target: &mut MetadataMap, template: &[(String, String)]) {
    for (key, value) in template {
        insert_ascii(target, key, value);
    }
}

fn unsigned_registry_metadata(subject_id: &str, permission: &str) -> Vec<(String, String)> {
    vec![
        ("x-sdkwork-subject-id".to_string(), subject_id.to_string()),
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
            format!(
                "00-{}-{}-01",
                Uuid::new_v4().simple(),
                Uuid::new_v4().simple()
            ),
        ),
    ]
}

fn insert_ascii(metadata: &mut MetadataMap, key: &str, value: &str) {
    if let (Ok(parsed_key), Ok(parsed_value)) = (
        MetadataKey::<Ascii>::from_bytes(key.as_bytes()),
        MetadataValue::try_from(value),
    ) {
        metadata.insert(parsed_key, parsed_value);
    }
}
