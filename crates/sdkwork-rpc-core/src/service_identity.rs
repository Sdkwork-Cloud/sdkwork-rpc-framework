use std::collections::BTreeSet;

use sdkwork_utils_rust::sha256_hash;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::{RpcFrameworkError, RpcFrameworkResult};

/// Canonical SPIFFE URI path used by SDKWork service certificates.
///
/// A trusted SDKWork mTLS leaf certificate must carry exactly one URI SAN in
/// this form:
/// `spiffe://{trust-domain}/sdkwork/service/{service-id}`.
pub const SPIFFE_SERVICE_PATH_PREFIX: &str = "/sdkwork/service/";

/// A service identity derived from a TLS-verified peer certificate.
///
/// This type is intentionally separate from any client-supplied RPC metadata.
/// Callers must only construct it through [`RpcServiceIdentityPolicy`] after
/// the TLS stack has validated the peer certificate chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedRpcServiceIdentity {
    pub service_id: String,
    pub trust_domain: String,
    pub spiffe_uri: String,
    pub certificate_sha256: String,
}

/// Restricts which SPIFFE service identities an internal RPC listener accepts.
///
/// An empty allow-list is invalid. Internal listeners must name their allowed
/// peers explicitly instead of treating a shared CA as blanket authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcServiceIdentityPolicy {
    trust_domain: String,
    allowed_service_ids: BTreeSet<String>,
}

impl RpcServiceIdentityPolicy {
    pub fn new<I, S>(
        trust_domain: impl Into<String>,
        allowed_service_ids: I,
    ) -> RpcFrameworkResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let trust_domain = trust_domain.into();
        validate_trust_domain(&trust_domain)?;

        let allowed_service_ids = allowed_service_ids
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();
        if allowed_service_ids.is_empty() {
            return Err(RpcFrameworkError::Configuration(
                "rpc service identity policy requires at least one allowed service id".to_owned(),
            ));
        }
        for service_id in &allowed_service_ids {
            validate_identifier("service_id", service_id)?;
        }

        Ok(Self {
            trust_domain,
            allowed_service_ids,
        })
    }

    pub fn trust_domain(&self) -> &str {
        self.trust_domain.as_str()
    }

    pub fn allows(&self, service_id: &str) -> bool {
        self.allowed_service_ids.contains(service_id)
    }

    pub fn allowed_service_ids(&self) -> &BTreeSet<String> {
        &self.allowed_service_ids
    }

    /// Parses a TLS-verified leaf certificate and derives its SPIFFE identity.
    ///
    /// Certificate chain validation is deliberately not repeated here: it is
    /// the TLS listener's responsibility. Supplying an arbitrary DER value to
    /// this method does not make it trusted.
    pub fn verify_tls_peer_certificate_der(
        &self,
        certificate_der: &[u8],
    ) -> RpcFrameworkResult<VerifiedRpcServiceIdentity> {
        let (_, certificate) = X509Certificate::from_der(certificate_der).map_err(|error| {
            RpcFrameworkError::Validation(format!(
                "mTLS peer certificate is not valid DER: {error}"
            ))
        })?;
        let subject_alt_name = certificate
            .subject_alternative_name()
            .map_err(|error| {
                RpcFrameworkError::Validation(format!(
                    "mTLS peer certificate SAN is invalid: {error}"
                ))
            })?
            .ok_or_else(|| {
                RpcFrameworkError::Validation(
                    "mTLS peer certificate must include one SPIFFE URI SAN".to_owned(),
                )
            })?;

        let spiffe_uris = subject_alt_name
            .value
            .general_names
            .iter()
            .filter_map(|name| match name {
                GeneralName::URI(uri) if uri.starts_with("spiffe://") => Some((*uri).to_owned()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if spiffe_uris.len() != 1 {
            return Err(RpcFrameworkError::Validation(format!(
                "mTLS peer certificate must include exactly one SPIFFE URI SAN, found {}",
                spiffe_uris.len()
            )));
        }

        let spiffe_uri = spiffe_uris
            .into_iter()
            .next()
            .expect("SPIFFE URI count was checked");
        let service_id = parse_spiffe_service_uri(&spiffe_uri, &self.trust_domain)?;
        if !self.allows(&service_id) {
            return Err(RpcFrameworkError::Validation(format!(
                "mTLS peer service identity `{service_id}` is not allowed"
            )));
        }

        Ok(VerifiedRpcServiceIdentity {
            service_id,
            trust_domain: self.trust_domain.clone(),
            spiffe_uri,
            certificate_sha256: sha256_hash(certificate_der),
        })
    }
}

fn parse_spiffe_service_uri(uri: &str, trust_domain: &str) -> RpcFrameworkResult<String> {
    let prefix = format!("spiffe://{trust_domain}{SPIFFE_SERVICE_PATH_PREFIX}");
    let Some(service_id) = uri.strip_prefix(prefix.as_str()) else {
        return Err(RpcFrameworkError::Validation(format!(
            "mTLS SPIFFE URI must match `{prefix}<service-id>`"
        )));
    };
    if service_id.contains('/') || service_id.contains('?') || service_id.contains('#') {
        return Err(RpcFrameworkError::Validation(
            "mTLS SPIFFE service identity must not include path, query, or fragment suffixes"
                .to_owned(),
        ));
    }
    validate_identifier("SPIFFE service_id", service_id)?;
    Ok(service_id.to_owned())
}

pub(crate) fn validate_identifier(field: &str, value: &str) -> RpcFrameworkResult<()> {
    if value.is_empty() || value.len() > 128 {
        return Err(RpcFrameworkError::Validation(format!(
            "{field} must contain 1 to 128 characters"
        )));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(RpcFrameworkError::Validation(format!(
            "{field} contains unsupported characters"
        )));
    }
    Ok(())
}

fn validate_trust_domain(trust_domain: &str) -> RpcFrameworkResult<()> {
    if trust_domain.is_empty() || trust_domain.len() > 253 {
        return Err(RpcFrameworkError::Configuration(
            "SPIFFE trust_domain must contain 1 to 253 characters".to_owned(),
        ));
    }
    if !trust_domain
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
    {
        return Err(RpcFrameworkError::Configuration(
            "SPIFFE trust_domain contains unsupported characters".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, KeyPair, SanType};

    use super::*;

    fn certificate_with_sans(sans: Vec<SanType>) -> Vec<u8> {
        let key_pair = KeyPair::generate().expect("test key pair");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("test params");
        params.subject_alt_names = sans;
        params
            .self_signed(&key_pair)
            .expect("test certificate")
            .der()
            .to_vec()
    }

    #[test]
    fn derives_allowed_spiffe_service_identity_from_certificate() {
        let der = certificate_with_sans(vec![SanType::URI(
            "spiffe://sdkwork.internal/sdkwork/service/sdkwork-knowledgebase"
                .try_into()
                .expect("valid URI SAN"),
        )]);
        let policy = RpcServiceIdentityPolicy::new("sdkwork.internal", ["sdkwork-knowledgebase"])
            .expect("policy");

        let identity = policy
            .verify_tls_peer_certificate_der(&der)
            .expect("certificate identity");
        assert_eq!(identity.service_id, "sdkwork-knowledgebase");
        assert_eq!(identity.trust_domain, "sdkwork.internal");
        assert_eq!(identity.certificate_sha256.len(), 64);
    }

    #[test]
    fn rejects_ambiguous_or_untrusted_spiffe_identity() {
        let ambiguous = certificate_with_sans(vec![
            SanType::URI(
                "spiffe://sdkwork.internal/sdkwork/service/sdkwork-knowledgebase"
                    .try_into()
                    .expect("valid URI SAN"),
            ),
            SanType::URI(
                "spiffe://sdkwork.internal/sdkwork/service/sdkwork-im"
                    .try_into()
                    .expect("valid URI SAN"),
            ),
        ]);
        let policy = RpcServiceIdentityPolicy::new("sdkwork.internal", ["sdkwork-knowledgebase"])
            .expect("policy");
        assert!(policy.verify_tls_peer_certificate_der(&ambiguous).is_err());

        let wrong_domain = certificate_with_sans(vec![SanType::URI(
            "spiffe://other.internal/sdkwork/service/sdkwork-knowledgebase"
                .try_into()
                .expect("valid URI SAN"),
        )]);
        assert!(policy
            .verify_tls_peer_certificate_der(&wrong_domain)
            .is_err());
    }
}
