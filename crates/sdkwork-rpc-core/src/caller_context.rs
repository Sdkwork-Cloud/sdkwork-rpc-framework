use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sdkwork_utils_rust::{
    base64url_decode, base64url_encode, hmac_sha256_base64url, verify_hmac_sha256_base64url,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::service_identity::validate_identifier;
use crate::{RpcFrameworkError, RpcFrameworkResult, VerifiedRpcServiceIdentity};

const CALLER_CONTEXT_SIGNATURE_DOMAIN: &str = "sdkwork-rpc-caller-context/v1\\n";
const CALLER_CONTEXT_VERSION: u8 = 1;
const MAX_CALLER_CONTEXT_TTL: Duration = Duration::from_secs(300);
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(30);
const MAX_CONTEXT_METADATA_BYTES: usize = 4096;

/// Framework-owned gRPC metadata key for an opaque signed caller context.
pub const RPC_CALLER_CONTEXT_METADATA: &str = "x-sdkwork-rpc-caller-context";
/// Framework-owned gRPC metadata key for the caller-context HMAC signature.
pub const RPC_CALLER_CONTEXT_SIGNATURE_METADATA: &str = "x-sdkwork-rpc-caller-signature";
/// Optional client assertion. It is never an authentication source and must
/// equal the mTLS certificate identity when present.
pub const RPC_SERVICE_IDENTITY_ASSERTION_METADATA: &str = "x-sdkwork-service";

/// The subject kind carried by a short-lived internal caller context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcCallerActorKind {
    User,
    Service,
}

/// Typed caller data obtained from a framework-authenticated request context.
///
/// This type contains no bearer credential. It becomes trustworthy to a
/// receiver only after [`RpcCallerContextSigner`] has signed it and
/// [`RpcCallerContextVerifier`] has matched the signature issuer to the mTLS
/// peer certificate identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcCallerContext {
    pub tenant_id: String,
    pub organization_id: String,
    pub actor_id: String,
    pub actor_kind: RpcCallerActorKind,
    pub session_id: Option<String>,
    pub request_id: String,
    pub trace_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub audience_service_id: String,
}

impl RpcCallerContext {
    pub fn builder() -> RpcCallerContextBuilder {
        RpcCallerContextBuilder::default()
    }
}

#[derive(Default)]
pub struct RpcCallerContextBuilder {
    tenant_id: Option<String>,
    organization_id: Option<String>,
    actor_id: Option<String>,
    actor_kind: Option<RpcCallerActorKind>,
    session_id: Option<String>,
    request_id: Option<String>,
    trace_id: Option<String>,
    idempotency_key: Option<String>,
    audience_service_id: Option<String>,
}

impl RpcCallerContextBuilder {
    pub fn tenant_id(mut self, value: impl Into<String>) -> Self {
        self.tenant_id = Some(value.into());
        self
    }

    pub fn organization_id(mut self, value: impl Into<String>) -> Self {
        self.organization_id = Some(value.into());
        self
    }

    pub fn actor_id(mut self, value: impl Into<String>) -> Self {
        self.actor_id = Some(value.into());
        self
    }

    pub fn actor_kind(mut self, value: RpcCallerActorKind) -> Self {
        self.actor_kind = Some(value);
        self
    }

    pub fn session_id(mut self, value: impl Into<String>) -> Self {
        self.session_id = Some(value.into());
        self
    }

    pub fn request_id(mut self, value: impl Into<String>) -> Self {
        self.request_id = Some(value.into());
        self
    }

    pub fn trace_id(mut self, value: impl Into<String>) -> Self {
        self.trace_id = Some(value.into());
        self
    }

    pub fn idempotency_key(mut self, value: impl Into<String>) -> Self {
        self.idempotency_key = Some(value.into());
        self
    }

    pub fn audience_service_id(mut self, value: impl Into<String>) -> Self {
        self.audience_service_id = Some(value.into());
        self
    }

    pub fn build(self) -> RpcFrameworkResult<RpcCallerContext> {
        let tenant_id = required_identifier("tenant_id", self.tenant_id)?;
        let organization_id = required_identifier("organization_id", self.organization_id)?;
        let actor_id = required_identifier("actor_id", self.actor_id)?;
        let actor_kind = self.actor_kind.ok_or_else(|| {
            RpcFrameworkError::Validation("caller context actor_kind is required".to_owned())
        })?;
        let request_id = required_identifier("request_id", self.request_id)?;
        let audience_service_id =
            required_identifier("audience_service_id", self.audience_service_id)?;
        let session_id = optional_identifier("session_id", self.session_id)?;
        let trace_id = optional_identifier("trace_id", self.trace_id)?;
        let idempotency_key = optional_identifier("idempotency_key", self.idempotency_key)?;

        if matches!(actor_kind, RpcCallerActorKind::User) && session_id.is_none() {
            return Err(RpcFrameworkError::Validation(
                "user caller context requires a session_id".to_owned(),
            ));
        }

        Ok(RpcCallerContext {
            tenant_id,
            organization_id,
            actor_id,
            actor_kind,
            session_id,
            request_id,
            trace_id,
            idempotency_key,
            audience_service_id,
        })
    }
}

/// A signing key for caller context delegation. Debug output never exposes key
/// material, and callers must inject the key through secure bootstrap config.
#[derive(Clone, Eq, PartialEq)]
pub struct RpcCallerContextSigningKey(Vec<u8>);

impl RpcCallerContextSigningKey {
    pub fn from_base64url(encoded: &str) -> RpcFrameworkResult<Self> {
        let bytes = base64url_decode(encoded).ok_or_else(|| {
            RpcFrameworkError::Configuration(
                "rpc caller context signing key must be unpadded base64url".to_owned(),
            )
        })?;
        Self::from_secret_bytes(bytes)
    }

    pub fn from_secret_bytes(bytes: impl Into<Vec<u8>>) -> RpcFrameworkResult<Self> {
        let bytes = bytes.into();
        if bytes.len() != 32 {
            return Err(RpcFrameworkError::Configuration(
                "rpc caller context signing key must be exactly 32 bytes".to_owned(),
            ));
        }
        Ok(Self(bytes))
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl fmt::Debug for RpcCallerContextSigningKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RpcCallerContextSigningKey(REDACTED)")
    }
}

/// Signs a caller context on behalf of one mTLS service identity.
#[derive(Clone, Debug)]
pub struct RpcCallerContextSigner {
    issuer_service_id: String,
    signing_key: RpcCallerContextSigningKey,
    max_ttl: Duration,
}

impl RpcCallerContextSigner {
    pub fn new(
        issuer_service_id: impl Into<String>,
        signing_key: RpcCallerContextSigningKey,
    ) -> RpcFrameworkResult<Self> {
        let issuer_service_id = issuer_service_id.into();
        validate_identifier("issuer_service_id", &issuer_service_id)?;
        Ok(Self {
            issuer_service_id,
            signing_key,
            max_ttl: MAX_CALLER_CONTEXT_TTL,
        })
    }

    pub fn sign(
        &self,
        context: RpcCallerContext,
        ttl: Duration,
    ) -> RpcFrameworkResult<SignedRpcCallerContext> {
        self.sign_at(context, ttl, SystemTime::now())
    }

    pub fn sign_at(
        &self,
        context: RpcCallerContext,
        ttl: Duration,
        now: SystemTime,
    ) -> RpcFrameworkResult<SignedRpcCallerContext> {
        if ttl.is_zero() || ttl > self.max_ttl {
            return Err(RpcFrameworkError::Validation(format!(
                "rpc caller context ttl must be between 1 second and {} seconds",
                self.max_ttl.as_secs()
            )));
        }
        let issued_at_unix_seconds = unix_seconds(now)?;
        let expires_at_unix_seconds = issued_at_unix_seconds
            .checked_add(i64::try_from(ttl.as_secs()).map_err(|_| {
                RpcFrameworkError::Validation("rpc caller context ttl is too large".to_owned())
            })?)
            .ok_or_else(|| {
                RpcFrameworkError::Validation("rpc caller context expiry overflow".to_owned())
            })?;
        let payload = SignedRpcCallerContextPayload {
            version: CALLER_CONTEXT_VERSION,
            issuer_service_id: self.issuer_service_id.clone(),
            audience_service_id: context.audience_service_id,
            tenant_id: context.tenant_id,
            organization_id: context.organization_id,
            actor_id: context.actor_id,
            actor_kind: context.actor_kind,
            session_id: context.session_id,
            request_id: context.request_id,
            trace_id: context.trace_id,
            idempotency_key: context.idempotency_key,
            issued_at_unix_seconds,
            expires_at_unix_seconds,
            nonce: Uuid::new_v4().simple().to_string(),
        };
        let serialized = serde_json::to_vec(&payload).map_err(|error| {
            RpcFrameworkError::Validation(format!(
                "failed to serialize rpc caller context: {error}"
            ))
        })?;
        let encoded_context = base64url_encode(&serialized);
        let signature = hmac_sha256_base64url(
            signing_input(&encoded_context).as_bytes(),
            self.signing_key.as_bytes(),
        );
        Ok(SignedRpcCallerContext {
            encoded_context,
            signature,
        })
    }
}

/// Opaque, transport-safe signed caller context. Only framework metadata
/// providers should serialize it onto an outbound RPC request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedRpcCallerContext {
    encoded_context: String,
    signature: String,
}

impl SignedRpcCallerContext {
    /// Creates an untrusted transport value for framework-side verification.
    /// This constructor does not assert authenticity; callers must pass the
    /// returned value to [`RpcCallerContextVerifier::verify`].
    pub fn from_transport_metadata(
        encoded_context: impl Into<String>,
        signature: impl Into<String>,
    ) -> Self {
        Self {
            encoded_context: encoded_context.into(),
            signature: signature.into(),
        }
    }

    pub fn encoded_context(&self) -> &str {
        self.encoded_context.as_str()
    }

    pub fn signature(&self) -> &str {
        self.signature.as_str()
    }
}

/// A caller context accepted after signature, time-window, audience, and mTLS
/// issuer binding verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedRpcCallerContext {
    pub issuer_service_id: String,
    pub audience_service_id: String,
    pub tenant_id: String,
    pub organization_id: String,
    pub actor_id: String,
    pub actor_kind: RpcCallerActorKind,
    pub session_id: Option<String>,
    pub request_id: String,
    pub trace_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub issued_at_unix_seconds: i64,
    pub expires_at_unix_seconds: i64,
    pub nonce: String,
}

/// Verifies signed caller contexts from explicitly trusted mTLS issuers.
#[derive(Clone, Debug)]
pub struct RpcCallerContextVerifier {
    audience_service_id: String,
    issuer_keys: BTreeMap<String, RpcCallerContextSigningKey>,
    max_ttl: Duration,
    max_clock_skew: Duration,
}

impl RpcCallerContextVerifier {
    pub fn new<I, S>(
        audience_service_id: impl Into<String>,
        issuer_keys: I,
    ) -> RpcFrameworkResult<Self>
    where
        I: IntoIterator<Item = (S, RpcCallerContextSigningKey)>,
        S: Into<String>,
    {
        let audience_service_id = audience_service_id.into();
        validate_identifier("audience_service_id", &audience_service_id)?;
        let issuer_keys = issuer_keys
            .into_iter()
            .map(|(service_id, key)| {
                Ok((
                    validated_identifier("issuer_service_id", service_id.into())?,
                    key,
                ))
            })
            .collect::<RpcFrameworkResult<BTreeMap<_, _>>>()?;
        if issuer_keys.is_empty() {
            return Err(RpcFrameworkError::Configuration(
                "rpc caller context verifier requires at least one trusted issuer key".to_owned(),
            ));
        }
        Ok(Self {
            audience_service_id,
            issuer_keys,
            max_ttl: MAX_CALLER_CONTEXT_TTL,
            max_clock_skew: MAX_CLOCK_SKEW,
        })
    }

    pub fn verify(
        &self,
        signed: &SignedRpcCallerContext,
        peer_identity: &VerifiedRpcServiceIdentity,
        metadata_idempotency_key: Option<&str>,
        now: SystemTime,
    ) -> RpcFrameworkResult<VerifiedRpcCallerContext> {
        if signed.encoded_context.len() > MAX_CONTEXT_METADATA_BYTES
            || signed.signature.len() > MAX_CONTEXT_METADATA_BYTES
        {
            return Err(RpcFrameworkError::Validation(
                "rpc caller context metadata exceeds maximum size".to_owned(),
            ));
        }
        let decoded = base64url_decode(signed.encoded_context()).ok_or_else(|| {
            RpcFrameworkError::Validation("rpc caller context is not valid base64url".to_owned())
        })?;
        let payload: SignedRpcCallerContextPayload =
            serde_json::from_slice(&decoded).map_err(|_| {
                RpcFrameworkError::Validation("rpc caller context has an invalid schema".to_owned())
            })?;
        validate_payload(&payload, self.max_ttl)?;

        if payload.audience_service_id != self.audience_service_id {
            return Err(RpcFrameworkError::Validation(
                "rpc caller context is issued for a different audience service".to_owned(),
            ));
        }
        if payload.issuer_service_id != peer_identity.service_id {
            return Err(RpcFrameworkError::Validation(
                "rpc caller context issuer does not match the mTLS peer identity".to_owned(),
            ));
        }
        let signing_key = self
            .issuer_keys
            .get(&payload.issuer_service_id)
            .ok_or_else(|| {
                RpcFrameworkError::Validation(
                    "rpc caller context issuer is not trusted for this listener".to_owned(),
                )
            })?;
        let signature = base64url_decode(signed.signature()).ok_or_else(|| {
            RpcFrameworkError::Validation(
                "rpc caller context signature is not valid base64url".to_owned(),
            )
        })?;
        if !verify_hmac_sha256_base64url(
            signing_input(signed.encoded_context()).as_bytes(),
            signing_key.as_bytes(),
            &signature,
        ) {
            return Err(RpcFrameworkError::Validation(
                "rpc caller context signature is invalid".to_owned(),
            ));
        }
        validate_time_window(&payload, now, self.max_clock_skew)?;
        validate_metadata_idempotency(&payload, metadata_idempotency_key)?;

        Ok(VerifiedRpcCallerContext {
            issuer_service_id: payload.issuer_service_id,
            audience_service_id: payload.audience_service_id,
            tenant_id: payload.tenant_id,
            organization_id: payload.organization_id,
            actor_id: payload.actor_id,
            actor_kind: payload.actor_kind,
            session_id: payload.session_id,
            request_id: payload.request_id,
            trace_id: payload.trace_id,
            idempotency_key: payload.idempotency_key,
            issued_at_unix_seconds: payload.issued_at_unix_seconds,
            expires_at_unix_seconds: payload.expires_at_unix_seconds,
            nonce: payload.nonce,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SignedRpcCallerContextPayload {
    version: u8,
    issuer_service_id: String,
    audience_service_id: String,
    tenant_id: String,
    organization_id: String,
    actor_id: String,
    actor_kind: RpcCallerActorKind,
    session_id: Option<String>,
    request_id: String,
    trace_id: Option<String>,
    idempotency_key: Option<String>,
    issued_at_unix_seconds: i64,
    expires_at_unix_seconds: i64,
    nonce: String,
}

fn signing_input(encoded_context: &str) -> String {
    format!("{CALLER_CONTEXT_SIGNATURE_DOMAIN}{encoded_context}")
}

fn required_identifier(field: &str, value: Option<String>) -> RpcFrameworkResult<String> {
    let value = value.ok_or_else(|| {
        RpcFrameworkError::Validation(format!("caller context {field} is required"))
    })?;
    validated_identifier(field, value)
}

fn optional_identifier(field: &str, value: Option<String>) -> RpcFrameworkResult<Option<String>> {
    value
        .map(|value| validated_identifier(field, value))
        .transpose()
}

fn validated_identifier(field: &str, value: String) -> RpcFrameworkResult<String> {
    validate_identifier(field, &value)?;
    Ok(value)
}

fn validate_payload(
    payload: &SignedRpcCallerContextPayload,
    max_ttl: Duration,
) -> RpcFrameworkResult<()> {
    if payload.version != CALLER_CONTEXT_VERSION {
        return Err(RpcFrameworkError::Validation(
            "unsupported rpc caller context version".to_owned(),
        ));
    }
    for (field, value) in [
        ("issuer_service_id", payload.issuer_service_id.as_str()),
        ("audience_service_id", payload.audience_service_id.as_str()),
        ("tenant_id", payload.tenant_id.as_str()),
        ("organization_id", payload.organization_id.as_str()),
        ("actor_id", payload.actor_id.as_str()),
        ("request_id", payload.request_id.as_str()),
        ("nonce", payload.nonce.as_str()),
    ] {
        validate_identifier(field, value)?;
    }
    if matches!(payload.actor_kind, RpcCallerActorKind::User) && payload.session_id.is_none() {
        return Err(RpcFrameworkError::Validation(
            "user rpc caller context requires a session_id".to_owned(),
        ));
    }
    for (field, value) in [
        ("session_id", payload.session_id.as_deref()),
        ("trace_id", payload.trace_id.as_deref()),
        ("idempotency_key", payload.idempotency_key.as_deref()),
    ] {
        if let Some(value) = value {
            validate_identifier(field, value)?;
        }
    }
    let ttl = payload
        .expires_at_unix_seconds
        .checked_sub(payload.issued_at_unix_seconds)
        .ok_or_else(|| {
            RpcFrameworkError::Validation(
                "rpc caller context expiry precedes issue time".to_owned(),
            )
        })?;
    if ttl <= 0
        || u64::try_from(ttl)
            .ok()
            .is_none_or(|value| value > max_ttl.as_secs())
    {
        return Err(RpcFrameworkError::Validation(format!(
            "rpc caller context ttl must be between 1 second and {} seconds",
            max_ttl.as_secs()
        )));
    }
    Ok(())
}

fn validate_time_window(
    payload: &SignedRpcCallerContextPayload,
    now: SystemTime,
    max_clock_skew: Duration,
) -> RpcFrameworkResult<()> {
    let now = unix_seconds(now)?;
    let skew = i64::try_from(max_clock_skew.as_secs()).map_err(|_| {
        RpcFrameworkError::Validation("rpc caller context clock skew is too large".to_owned())
    })?;
    if payload.issued_at_unix_seconds > now.saturating_add(skew) {
        return Err(RpcFrameworkError::Validation(
            "rpc caller context is issued in the future".to_owned(),
        ));
    }
    if payload.expires_at_unix_seconds < now.saturating_sub(skew) {
        return Err(RpcFrameworkError::Validation(
            "rpc caller context has expired".to_owned(),
        ));
    }
    Ok(())
}

fn validate_metadata_idempotency(
    payload: &SignedRpcCallerContextPayload,
    metadata_idempotency_key: Option<&str>,
) -> RpcFrameworkResult<()> {
    match (&payload.idempotency_key, metadata_idempotency_key) {
        (Some(expected), Some(actual)) if expected == actual => Ok(()),
        (None, None) => Ok(()),
        _ => Err(RpcFrameworkError::Validation(
            "rpc caller context idempotency key does not match request metadata".to_owned(),
        )),
    }
}

fn unix_seconds(value: SystemTime) -> RpcFrameworkResult<i64> {
    value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            RpcFrameworkError::Validation(
                "system clock precedes the Unix epoch while issuing rpc caller context".to_owned(),
            )
        })
        .and_then(|duration| {
            i64::try_from(duration.as_secs()).map_err(|_| {
                RpcFrameworkError::Validation(
                    "system clock is outside supported rpc caller context range".to_owned(),
                )
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [7; 32];

    fn caller_context() -> RpcCallerContext {
        RpcCallerContext::builder()
            .tenant_id("100001")
            .organization_id("200001")
            .actor_id("300001")
            .actor_kind(RpcCallerActorKind::User)
            .session_id("session-1")
            .request_id("request-1")
            .trace_id("trace-1")
            .idempotency_key("idempotency-1")
            .audience_service_id("sdkwork-im")
            .build()
            .expect("caller context")
    }

    fn peer_identity() -> VerifiedRpcServiceIdentity {
        VerifiedRpcServiceIdentity {
            service_id: "sdkwork-knowledgebase".to_owned(),
            trust_domain: "sdkwork.internal".to_owned(),
            spiffe_uri: "spiffe://sdkwork.internal/sdkwork/service/sdkwork-knowledgebase"
                .to_owned(),
            certificate_sha256: "a".repeat(64),
        }
    }

    #[test]
    fn signs_and_verifies_context_bound_to_peer_and_idempotency_key() {
        let key = RpcCallerContextSigningKey::from_secret_bytes(KEY).expect("key");
        let signer =
            RpcCallerContextSigner::new("sdkwork-knowledgebase", key.clone()).expect("signer");
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let signed = signer
            .sign_at(caller_context(), Duration::from_secs(60), now)
            .expect("signed context");
        let verifier =
            RpcCallerContextVerifier::new("sdkwork-im", [("sdkwork-knowledgebase", key)])
                .expect("verifier");

        let verified = verifier
            .verify(&signed, &peer_identity(), Some("idempotency-1"), now)
            .expect("verified context");
        assert_eq!(verified.actor_id, "300001");
        assert_eq!(verified.issuer_service_id, "sdkwork-knowledgebase");
    }

    #[test]
    fn rejects_signature_tampering_peer_mismatch_and_expired_context() {
        let key = RpcCallerContextSigningKey::from_secret_bytes(KEY).expect("key");
        let signer =
            RpcCallerContextSigner::new("sdkwork-knowledgebase", key.clone()).expect("signer");
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let signed = signer
            .sign_at(caller_context(), Duration::from_secs(60), now)
            .expect("signed context");
        let verifier =
            RpcCallerContextVerifier::new("sdkwork-im", [("sdkwork-knowledgebase", key)])
                .expect("verifier");

        let mut tampered = signed.clone();
        tampered.signature.push('A');
        assert!(verifier
            .verify(&tampered, &peer_identity(), Some("idempotency-1"), now)
            .is_err());

        let wrong_peer = VerifiedRpcServiceIdentity {
            service_id: "sdkwork-other".to_owned(),
            ..peer_identity()
        };
        assert!(verifier
            .verify(&signed, &wrong_peer, Some("idempotency-1"), now)
            .is_err());
        assert!(verifier
            .verify(
                &signed,
                &peer_identity(),
                Some("idempotency-1"),
                now + Duration::from_secs(91),
            )
            .is_err());
    }
}
