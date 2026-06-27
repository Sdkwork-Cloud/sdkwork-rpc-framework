//! TLS/mTLS configuration entry points for gRPC client channels.
//!
//! Implements `RPC_FRAMEWORK_SPEC.md` §10 "Transport security | TLS/mTLS
//! termination or approved local-only exemption" and `DISCOVERY_SPEC.md` §11
//! "Production SHOULD require TLS or mTLS on discovery ingress".
//!
//! The [`RpcTlsConfig`] struct is path-based and always available so config
//! can be constructed in any build. The actual tonic `ClientTlsConfig`
//! integration is gated behind the `tls` cargo feature; when the feature is
//! disabled, supplying a TLS config returns a configuration error so callers
//! cannot silently fall back to plaintext in production.

use std::path::PathBuf;

use sdkwork_rpc_framework_core::RpcFrameworkError;

/// TLS configuration for a gRPC client channel.
///
/// When attached to [`GrpcChannelConfig`](crate::transport::GrpcChannelConfig),
/// the channel uses TLS. For mutual TLS (mTLS), set both `client_cert_path` and
/// `client_key_path` so the client presents its own certificate to the server.
///
/// The client certificate and private key are configured as separate paths
/// (rather than a combined PEM), matching the industry convention used by
/// Kubernetes TLS secrets (`tls.crt` + `tls.key`), Envoy
/// `tls_context.common_tls_context.tls_certificates`, and nginx
/// `ssl_certificate` / `ssl_certificate_key`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RpcTlsConfig {
    /// Path to a PEM-encoded CA certificate used to verify the server's
    /// certificate. When `None`, the webpki root store is used (requires the
    /// `tls` feature with `tls-webpki-roots`).
    pub server_ca_certificate_path: Option<PathBuf>,
    /// Path to the PEM-encoded client certificate chain for mTLS.
    /// Must be paired with `client_key_path`. Both must be set or both unset.
    pub client_cert_path: Option<PathBuf>,
    /// Path to the PEM-encoded client private key for mTLS.
    /// Must be paired with `client_cert_path`. Both must be set or both unset.
    pub client_key_path: Option<PathBuf>,
    /// Override the SNI domain name. When `None`, the domain is derived from
    /// the endpoint URI.
    pub domain: Option<String>,
}

impl RpcTlsConfig {
    /// Creates a minimal TLS config that verifies the server certificate
    /// against the webpki root store. Sufficient for one-way TLS.
    pub fn server_verified() -> Self {
        Self::default()
    }

    /// Adds a custom CA certificate for server verification.
    pub fn with_server_ca(mut self, path: impl Into<PathBuf>) -> Self {
        self.server_ca_certificate_path = Some(path.into());
        self
    }

    /// Adds a client identity (cert + key) for mTLS. Both paths must be
    /// provided together; pairing one without the other will fail validation.
    pub fn with_client_identity(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_cert_path = Some(cert_path.into());
        self.client_key_path = Some(key_path.into());
        self
    }

    /// Overrides the SNI domain name.
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    /// Validates that configured paths exist and that the client cert/key
    /// pair is fully specified (both set or both unset). Called before
    /// attempting to open the PEM files so failures surface as configuration
    /// errors rather than opaque IO errors during connect.
    pub fn validate(&self) -> Result<(), RpcFrameworkError> {
        if let Some(path) = &self.server_ca_certificate_path {
            if !path.exists() {
                return Err(RpcFrameworkError::Configuration(format!(
                    "tls server_ca_certificate_path does not exist: {}",
                    path.display()
                )));
            }
        }
        match (&self.client_cert_path, &self.client_key_path) {
            (Some(cert), Some(key)) => {
                if !cert.exists() {
                    return Err(RpcFrameworkError::Configuration(format!(
                        "tls client_cert_path does not exist: {}",
                        cert.display()
                    )));
                }
                if !key.exists() {
                    return Err(RpcFrameworkError::Configuration(format!(
                        "tls client_key_path does not exist: {}",
                        key.display()
                    )));
                }
            }
            (Some(cert), None) => {
                return Err(RpcFrameworkError::Configuration(format!(
                    "tls client_cert_path is set but client_key_path is missing; \
                     both must be provided for mTLS (cert={})",
                    cert.display()
                )));
            }
            (None, Some(key)) => {
                return Err(RpcFrameworkError::Configuration(format!(
                    "tls client_key_path is set but client_cert_path is missing; \
                     both must be provided for mTLS (key={})",
                    key.display()
                )));
            }
            (None, None) => {}
        }
        Ok(())
    }
}

/// Builds a tonic `ClientTlsConfig` from the SDKWork TLS config.
///
/// Reads PEM files from disk and constructs the tonic TLS config. This is
/// gated behind the `tls` feature; when disabled, callers receive a
/// configuration error directing them to enable the feature.
#[cfg(feature = "tls")]
pub(crate) fn build_client_tls_config(
    config: &RpcTlsConfig,
) -> Result<tonic::transport::ClientTlsConfig, RpcFrameworkError> {
    config.validate()?;
    let mut tls = tonic::transport::ClientTlsConfig::new();

    if let Some(domain) = &config.domain {
        tls = tls.domain_name(domain.clone());
    }

    if let Some(ca_path) = &config.server_ca_certificate_path {
        let pem = std::fs::read(ca_path).map_err(|error| RpcFrameworkError::Configuration(
            format!("failed to read tls ca certificate {}: {error}", ca_path.display()),
        ))?;
        let cert = tonic::transport::Certificate::from_pem(pem);
        tls = tls.ca_certificate(cert);
    } else {
        // No custom CA: use the webpki root store so production servers with
        // publicly-trusted certificates are verified out of the box.
        tls = tls.with_webpki_roots();
    }

    if let (Some(cert_path), Some(key_path)) =
        (&config.client_cert_path, &config.client_key_path)
    {
        let cert_pem = std::fs::read(cert_path).map_err(|error| {
            RpcFrameworkError::Configuration(format!(
                "failed to read tls client cert {}: {error}",
                cert_path.display()
            ))
        })?;
        let key_pem = std::fs::read(key_path).map_err(|error| {
            RpcFrameworkError::Configuration(format!(
                "failed to read tls client key {}: {error}",
                key_path.display()
            ))
        })?;
        let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
        tls = tls.identity(identity);
    }

    Ok(tls)
}

/// Returns an error directing the caller to enable the `tls` feature when TLS
/// config is supplied but the feature is not enabled. This prevents silent
/// fallback to plaintext in production builds.
#[cfg(not(feature = "tls"))]
pub(crate) fn build_client_tls_config(
    _config: &RpcTlsConfig,
) -> Result<tonic::transport::ClientTlsConfig, RpcFrameworkError> {
    Err(RpcFrameworkError::Configuration(
        "tls config supplied but the `tls` cargo feature is not enabled; \
         enable `sdkwork-rpc-client/tls` for production TLS/mTLS"
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_verified_config_has_no_paths() {
        let config = RpcTlsConfig::server_verified();
        assert!(config.server_ca_certificate_path.is_none());
        assert!(config.client_cert_path.is_none());
        assert!(config.client_key_path.is_none());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_nonexistent_ca_path() {
        let config = RpcTlsConfig::server_verified()
            .with_server_ca("/nonexistent/ca.pem");
        let error = config.validate().expect_err("nonexistent path should fail");
        assert!(matches!(error, RpcFrameworkError::Configuration(_)));
    }

    #[test]
    fn validate_rejects_nonexistent_client_cert_path() {
        let config = RpcTlsConfig::server_verified()
            .with_client_identity("/nonexistent/client.crt", "/nonexistent/client.key");
        let error = config.validate().expect_err("nonexistent path should fail");
        assert!(matches!(error, RpcFrameworkError::Configuration(_)));
    }

    #[test]
    fn validate_rejects_cert_without_key() {
        let config = RpcTlsConfig {
            client_cert_path: Some("/tmp/client.crt".into()),
            client_key_path: None,
            ..RpcTlsConfig::server_verified()
        };
        let error = config.validate().expect_err("unpaired cert should fail");
        assert!(matches!(error, RpcFrameworkError::Configuration(_)));
    }

    #[test]
    fn validate_rejects_key_without_cert() {
        let config = RpcTlsConfig {
            client_cert_path: None,
            client_key_path: Some("/tmp/client.key".into()),
            ..RpcTlsConfig::server_verified()
        };
        let error = config.validate().expect_err("unpaired key should fail");
        assert!(matches!(error, RpcFrameworkError::Configuration(_)));
    }

    #[test]
    fn builder_methods_are_chainable() {
        let config = RpcTlsConfig::server_verified()
            .with_domain("rpc.internal")
            .with_server_ca("/tmp/ca.pem")
            .with_client_identity("/tmp/client.crt", "/tmp/client.key");
        assert_eq!(config.domain.as_deref(), Some("rpc.internal"));
        assert_eq!(
            config.client_cert_path.as_deref(),
            Some(std::path::Path::new("/tmp/client.crt"))
        );
        assert_eq!(
            config.client_key_path.as_deref(),
            Some(std::path::Path::new("/tmp/client.key"))
        );
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn build_without_feature_returns_configuration_error() {
        let config = RpcTlsConfig::server_verified();
        let error = build_client_tls_config(&config).expect_err("feature disabled");
        assert!(matches!(error, RpcFrameworkError::Configuration(_)));
    }
}
