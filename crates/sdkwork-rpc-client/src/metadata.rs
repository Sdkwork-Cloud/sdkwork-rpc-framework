//! Outbound RPC metadata providers per `RPC_FRAMEWORK_SPEC.md` client pipeline stage 1.

use std::time::Duration;

use sdkwork_rpc_framework_core::{
    RpcCallerContext, RpcCallerContextSigner, RpcFrameworkError, RpcFrameworkResult,
    SignedRpcCallerContext, RPC_ACCESS_TOKEN_METADATA, RPC_AUTHORIZATION_METADATA,
    RPC_CALLER_CONTEXT_METADATA, RPC_CALLER_CONTEXT_SIGNATURE_METADATA,
    RPC_IDEMPOTENCY_KEY_METADATA, RPC_REQUEST_HASH_METADATA, RPC_REQUEST_ID_METADATA,
    RPC_TRACEPARENT_METADATA,
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
        insert_optional(
            metadata,
            RPC_IDEMPOTENCY_KEY_METADATA,
            &self.idempotency_key,
        );
        insert_optional(metadata, RPC_REQUEST_HASH_METADATA, &self.request_hash);
    }
}

/// Supplies a framework-signed caller context to generated internal RPC
/// clients. Business services receive this provider by injection; they never
/// construct service identity or raw metadata headers themselves.
pub trait RpcServiceCredentialProvider: Send + Sync {
    fn issue(&self, caller_context: RpcCallerContext) -> RpcFrameworkResult<RpcServiceCredential>;
}

/// Typed service credential applied by an RPC adapter immediately before a
/// generated tonic call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcServiceCredential {
    signed_caller_context: SignedRpcCallerContext,
    idempotency_key: Option<String>,
}

impl RpcServiceCredential {
    pub fn signed_caller_context(&self) -> &SignedRpcCallerContext {
        &self.signed_caller_context
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    pub fn apply_to(&self, metadata: &mut MetadataMap) -> RpcFrameworkResult<()> {
        validate_bound_idempotency_metadata(metadata, self.idempotency_key())?;
        insert_required(
            metadata,
            RPC_CALLER_CONTEXT_METADATA,
            self.signed_caller_context.encoded_context(),
        )?;
        insert_required(
            metadata,
            RPC_CALLER_CONTEXT_SIGNATURE_METADATA,
            self.signed_caller_context.signature(),
        )?;
        if let Some(idempotency_key) = self.idempotency_key() {
            insert_required(
                metadata,
                RPC_IDEMPOTENCY_KEY_METADATA,
                idempotency_key,
            )?;
        }
        Ok(())
    }
}

/// Default provider for a service bootstrap that owns a short-lived caller
/// context signer. The signer itself is configured from a secret manager or
/// other secure runtime source outside business modules.
#[derive(Clone, Debug)]
pub struct SignedRpcServiceCredentialProvider {
    signer: RpcCallerContextSigner,
    ttl: Duration,
}

impl SignedRpcServiceCredentialProvider {
    pub fn new(signer: RpcCallerContextSigner, ttl: Duration) -> Result<Self, RpcFrameworkError> {
        if ttl.is_zero() || ttl > Duration::from_secs(300) {
            return Err(RpcFrameworkError::Configuration(
                "rpc service credential ttl must be between 1 second and 300 seconds".to_owned(),
            ));
        }
        Ok(Self { signer, ttl })
    }
}

impl RpcServiceCredentialProvider for SignedRpcServiceCredentialProvider {
    fn issue(&self, caller_context: RpcCallerContext) -> RpcFrameworkResult<RpcServiceCredential> {
        let idempotency_key = caller_context.idempotency_key.clone();
        Ok(RpcServiceCredential {
            signed_caller_context: self.signer.sign(caller_context, self.ttl)?,
            idempotency_key,
        })
    }
}

fn validate_bound_idempotency_metadata(
    metadata: &MetadataMap,
    expected: Option<&str>,
) -> RpcFrameworkResult<()> {
    let actual = metadata
        .get(RPC_IDEMPOTENCY_KEY_METADATA)
        .map(|value| {
            value.to_str().map_err(|_| {
                RpcFrameworkError::Validation(
                    "existing RPC idempotency metadata is not valid ASCII".to_owned(),
                )
            })
        })
        .transpose()?;

    match (expected, actual) {
        (Some(expected), Some(actual)) if expected == actual => Ok(()),
        // The credential provider owns inserting the paired transport metadata.
        // A caller need not pre-populate it before applying a signed credential.
        (Some(_), None) => Ok(()),
        (None, None) => Ok(()),
        _ => Err(RpcFrameworkError::Validation(
            "existing RPC idempotency metadata does not match the signed caller context"
                .to_owned(),
        )),
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

fn insert_required(metadata: &mut MetadataMap, key: &str, value: &str) -> RpcFrameworkResult<()> {
    if value.trim().is_empty() {
        return Err(RpcFrameworkError::Validation(format!(
            "required RPC metadata `{key}` must not be blank"
        )));
    }
    let parsed_key = MetadataKey::<Ascii>::from_bytes(key.as_bytes())
        .map_err(|_| RpcFrameworkError::Validation(format!("invalid RPC metadata key `{key}`")))?;
    let parsed_value = MetadataValue::try_from(value).map_err(|_| {
        RpcFrameworkError::Validation(format!("invalid RPC metadata value for `{key}`"))
    })?;
    metadata.insert(parsed_key, parsed_value);
    Ok(())
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

    #[test]
    fn signed_service_credential_writes_only_framework_owned_metadata() {
        use sdkwork_rpc_framework_core::{RpcCallerActorKind, RpcCallerContextSigningKey};

        let signer = RpcCallerContextSigner::new(
            "sdkwork-knowledgebase",
            RpcCallerContextSigningKey::from_secret_bytes([9_u8; 32]).expect("signing key"),
        )
        .expect("signer");
        let provider = SignedRpcServiceCredentialProvider::new(signer, Duration::from_secs(60))
            .expect("credential provider");
        let credential = provider
            .issue(
                RpcCallerContext::builder()
                    .tenant_id("100001")
                    .organization_id("200001")
                    .actor_id("300001")
                    .actor_kind(RpcCallerActorKind::User)
                    .session_id("session-1")
                    .request_id("request-1")
                    .idempotency_key("idempotency-1")
                    .audience_service_id("sdkwork-im")
                    .build()
                    .expect("caller context"),
            )
            .expect("credential");
        let mut metadata = MetadataMap::new();
        credential.apply_to(&mut metadata).expect("metadata");

        assert!(metadata.get(RPC_CALLER_CONTEXT_METADATA).is_some());
        assert!(metadata
            .get(RPC_CALLER_CONTEXT_SIGNATURE_METADATA)
            .is_some());
        assert_eq!(
            metadata
                .get(RPC_IDEMPOTENCY_KEY_METADATA)
                .and_then(|value| value.to_str().ok()),
            Some("idempotency-1")
        );
    }

    #[test]
    fn signed_service_credential_omits_absent_idempotency_metadata() {
        use sdkwork_rpc_framework_core::{RpcCallerActorKind, RpcCallerContextSigningKey};

        let signer = RpcCallerContextSigner::new(
            "sdkwork-knowledgebase",
            RpcCallerContextSigningKey::from_secret_bytes([9_u8; 32]).expect("signing key"),
        )
        .expect("signer");
        let provider = SignedRpcServiceCredentialProvider::new(signer, Duration::from_secs(60))
            .expect("credential provider");
        let credential = provider
            .issue(
                RpcCallerContext::builder()
                    .tenant_id("100001")
                    .organization_id("200001")
                    .actor_id("300001")
                    .actor_kind(RpcCallerActorKind::User)
                    .session_id("session-1")
                    .request_id("request-1")
                    .audience_service_id("sdkwork-im")
                    .build()
                    .expect("caller context"),
            )
            .expect("credential");
        let mut metadata = MetadataMap::new();
        credential.apply_to(&mut metadata).expect("metadata");

        assert!(metadata.get(RPC_IDEMPOTENCY_KEY_METADATA).is_none());
    }

    #[test]
    fn signed_service_credential_rejects_conflicting_idempotency_metadata() {
        use sdkwork_rpc_framework_core::{RpcCallerActorKind, RpcCallerContextSigningKey};

        let signer = RpcCallerContextSigner::new(
            "sdkwork-knowledgebase",
            RpcCallerContextSigningKey::from_secret_bytes([9_u8; 32]).expect("signing key"),
        )
        .expect("signer");
        let provider = SignedRpcServiceCredentialProvider::new(signer, Duration::from_secs(60))
            .expect("credential provider");
        let credential = provider
            .issue(
                RpcCallerContext::builder()
                    .tenant_id("100001")
                    .organization_id("200001")
                    .actor_id("300001")
                    .actor_kind(RpcCallerActorKind::User)
                    .session_id("session-1")
                    .request_id("request-1")
                    .idempotency_key("idempotency-1")
                    .audience_service_id("sdkwork-im")
                    .build()
                    .expect("caller context"),
            )
            .expect("credential");
        let mut metadata = MetadataMap::new();
        metadata.insert(
            RPC_IDEMPOTENCY_KEY_METADATA,
            MetadataValue::try_from("different-idempotency-key").expect("metadata value"),
        );

        let error = credential
            .apply_to(&mut metadata)
            .expect_err("conflicting metadata must fail closed");
        assert!(matches!(error, RpcFrameworkError::Validation(_)));
        assert!(metadata.get(RPC_CALLER_CONTEXT_METADATA).is_none());
    }
}
