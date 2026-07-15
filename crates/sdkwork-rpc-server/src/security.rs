#[cfg(any(feature = "tls", test))]
use sdkwork_rpc_framework_core::RPC_SERVICE_IDENTITY_ASSERTION_METADATA;
use sdkwork_rpc_framework_core::{
    RpcCallerContextVerifier, RpcServiceIdentityPolicy, VerifiedRpcCallerContext,
    VerifiedRpcServiceIdentity,
};
#[cfg(feature = "tls")]
use sdkwork_rpc_framework_core::{
    RpcFrameworkError, SignedRpcCallerContext, RPC_CALLER_CONTEXT_METADATA,
    RPC_CALLER_CONTEXT_SIGNATURE_METADATA,
};
#[cfg(feature = "tls")]
use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Framework-owned internal RPC security policy.
///
/// This policy derives service identity solely from a TLS-verified peer
/// certificate. Signed caller context is optional at this layer so a host can
/// serve both service-only and user-delegated internal methods; individual
/// method admission must require the typed context when it needs user scope.
#[derive(Clone, Debug)]
pub struct RpcInternalServiceSecurity {
    service_identity_policy: RpcServiceIdentityPolicy,
    caller_context_verifier: Option<RpcCallerContextVerifier>,
}

impl RpcInternalServiceSecurity {
    pub fn new(
        service_identity_policy: RpcServiceIdentityPolicy,
        caller_context_verifier: Option<RpcCallerContextVerifier>,
    ) -> Self {
        Self {
            service_identity_policy,
            caller_context_verifier,
        }
    }

    pub fn interceptor(&self) -> RpcInternalServiceInterceptor {
        RpcInternalServiceInterceptor {
            security: self.clone(),
        }
    }

    pub fn service_identity_policy(&self) -> &RpcServiceIdentityPolicy {
        &self.service_identity_policy
    }

    pub fn has_caller_context_verifier(&self) -> bool {
        self.caller_context_verifier.is_some()
    }

    pub fn validate_mtls_listener(
        &self,
        tls_config: &crate::RpcServerTlsConfig,
    ) -> Result<(), crate::ServeError> {
        tls_config.validate_mtls()
    }

    /// Verifies and injects framework-owned identity extensions.
    ///
    /// The only accepted source of `VerifiedRpcServiceIdentity` is the mTLS
    /// peer certificate exposed by tonic after a successful handshake. A
    /// client-provided `x-sdkwork-service` is merely a consistency assertion.
    #[cfg(feature = "tls")]
    pub fn verify_and_inject<T>(&self, request: &mut Request<T>) -> Result<(), Status> {
        let certificates = request.peer_certs().ok_or_else(|| {
            Status::unauthenticated("internal RPC requires a verified mTLS peer certificate")
        })?;
        let leaf = certificates.first().ok_or_else(|| {
            Status::unauthenticated("internal RPC peer certificate chain is empty")
        })?;
        let service_identity = self
            .service_identity_policy
            .verify_tls_peer_certificate_der(leaf.as_ref())
            .map_err(map_identity_error)?;

        validate_optional_service_assertion(request.metadata(), &service_identity)?;
        let caller_context =
            self.verify_optional_caller_context(request.metadata(), &service_identity)?;

        let extensions = request.extensions_mut();
        extensions.insert(service_identity);
        if let Some(caller_context) = caller_context {
            extensions.insert(caller_context);
        }
        Ok(())
    }

    #[cfg(not(feature = "tls"))]
    pub fn verify_and_inject<T>(&self, _request: &mut Request<T>) -> Result<(), Status> {
        Err(Status::failed_precondition(
            "internal RPC mTLS identity verification requires sdkwork-rpc-server/tls",
        ))
    }

    #[cfg(feature = "tls")]
    fn verify_optional_caller_context(
        &self,
        metadata: &MetadataMap,
        service_identity: &VerifiedRpcServiceIdentity,
    ) -> Result<Option<VerifiedRpcCallerContext>, Status> {
        let encoded_context = optional_metadata(metadata, RPC_CALLER_CONTEXT_METADATA)?;
        let signature = optional_metadata(metadata, RPC_CALLER_CONTEXT_SIGNATURE_METADATA)?;
        let (Some(encoded_context), Some(signature)) =
            (encoded_context.as_deref(), signature.as_deref())
        else {
            if encoded_context.is_some() || signature.is_some() {
                return Err(Status::unauthenticated(
                    "internal RPC caller context and signature must be supplied together",
                ));
            }
            return Ok(None);
        };
        let verifier = self.caller_context_verifier.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "internal RPC caller context was supplied but no verifier is configured",
            )
        })?;
        verifier
            .verify(
                &SignedRpcCallerContext::from_transport_metadata(encoded_context, signature),
                service_identity,
                optional_metadata(metadata, "idempotency-key")?.as_deref(),
                std::time::SystemTime::now(),
            )
            .map(Some)
            .map_err(map_identity_error)
    }
}

/// Cloneable tonic interceptor that injects framework-verified context into
/// request extensions before generated service adapters execute.
#[derive(Clone, Debug)]
pub struct RpcInternalServiceInterceptor {
    security: RpcInternalServiceSecurity,
}

impl Interceptor for RpcInternalServiceInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        self.security.verify_and_inject(&mut request)?;
        Ok(request)
    }
}

pub fn require_verified_rpc_service_identity<T>(
    request: &Request<T>,
) -> Result<&VerifiedRpcServiceIdentity, Status> {
    request
        .extensions()
        .get::<VerifiedRpcServiceIdentity>()
        .ok_or_else(|| {
            Status::unauthenticated("internal RPC request has no verified mTLS service identity")
        })
}

pub fn require_verified_rpc_caller_context<T>(
    request: &Request<T>,
) -> Result<&VerifiedRpcCallerContext, Status> {
    request
        .extensions()
        .get::<VerifiedRpcCallerContext>()
        .ok_or_else(|| {
            Status::unauthenticated("internal RPC request has no verified caller context")
        })
}

#[cfg(feature = "tls")]
fn validate_optional_service_assertion(
    metadata: &MetadataMap,
    service_identity: &VerifiedRpcServiceIdentity,
) -> Result<(), Status> {
    let Some(assertion) = optional_metadata(metadata, RPC_SERVICE_IDENTITY_ASSERTION_METADATA)?
    else {
        return Ok(());
    };
    if assertion != service_identity.service_id {
        return Err(Status::unauthenticated(
            "internal RPC service assertion does not match mTLS peer identity",
        ));
    }
    Ok(())
}

#[cfg(feature = "tls")]
fn optional_metadata(metadata: &MetadataMap, key: &str) -> Result<Option<String>, Status> {
    metadata
        .get(key)
        .map(|value| {
            value.to_str().map(str::to_owned).map_err(|_| {
                Status::invalid_argument(format!("RPC metadata `{key}` must be ASCII"))
            })
        })
        .transpose()
}

#[cfg(feature = "tls")]
fn map_identity_error(error: RpcFrameworkError) -> Status {
    match error {
        RpcFrameworkError::Validation(_) => {
            Status::unauthenticated("internal RPC identity verification failed")
        }
        RpcFrameworkError::Configuration(_) => {
            Status::failed_precondition("internal RPC identity verifier is misconfigured")
        }
        RpcFrameworkError::Transport(_) | RpcFrameworkError::Discovery(_) => {
            Status::unavailable("internal RPC identity verifier is unavailable")
        }
    }
}

#[cfg(test)]
mod tests {
    use sdkwork_rpc_framework_core::RpcServiceIdentityPolicy;

    use super::*;

    fn security() -> RpcInternalServiceSecurity {
        RpcInternalServiceSecurity::new(
            RpcServiceIdentityPolicy::new("sdkwork.internal", ["sdkwork-knowledgebase"])
                .expect("policy"),
            None,
        )
    }

    #[test]
    fn fails_closed_without_a_tls_peer_certificate() {
        let mut request = Request::new(());
        let error = security()
            .verify_and_inject(&mut request)
            .expect_err("plaintext requests must be rejected");
        assert!(matches!(
            error.code(),
            tonic::Code::FailedPrecondition | tonic::Code::Unauthenticated
        ));
    }

    #[test]
    fn no_header_value_can_create_a_verified_identity() {
        let mut request = Request::new(());
        request.metadata_mut().insert(
            RPC_SERVICE_IDENTITY_ASSERTION_METADATA,
            "sdkwork-knowledgebase".parse().expect("metadata"),
        );
        assert!(security().verify_and_inject(&mut request).is_err());
        assert!(request
            .extensions()
            .get::<VerifiedRpcServiceIdentity>()
            .is_none());
    }

    #[test]
    fn internal_security_rejects_a_non_mtls_listener_config() {
        let tls = crate::RpcServerTlsConfig {
            server_cert_path: "server.pem".into(),
            server_key_path: "server.key".into(),
            client_ca_certificate_path: None,
            client_auth_optional: false,
        };
        assert!(security().validate_mtls_listener(&tls).is_err());
    }

    #[cfg(feature = "tls")]
    mod mtls_integration {
        use std::time::Duration;

        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
            KeyUsagePurpose, SanType,
        };
        use sdkwork_rpc_framework_core::{
            RpcCallerActorKind, RpcCallerContext, RpcCallerContextSigner,
            RpcCallerContextSigningKey, RpcCallerContextVerifier, RPC_CALLER_CONTEXT_METADATA,
            RPC_CALLER_CONTEXT_SIGNATURE_METADATA, RPC_IDEMPOTENCY_KEY_METADATA,
        };
        use tempfile::TempDir;
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;
        use tokio_stream::wrappers::TcpListenerStream;
        use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server};
        use tonic::Request;
        use tonic_health::pb::health_client::HealthClient;
        use tonic_health::pb::HealthCheckRequest;

        use super::super::*;

        const SIGNING_KEY: [u8; 32] = [4; 32];

        struct TestCertificates {
            _temp_dir: TempDir,
            server_tls: crate::RpcServerTlsConfig,
            ca_pem: String,
            valid_client_cert_pem: String,
            valid_client_key_pem: String,
            rejected_client_cert_pem: String,
            rejected_client_key_pem: String,
        }

        fn issue_test_certificates() -> TestCertificates {
            let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params.key_usages = vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyCertSign,
            ];
            let ca_key = KeyPair::generate().expect("CA key");
            let ca_certificate = ca_params.self_signed(&ca_key).expect("CA certificate");
            let ca_pem = ca_certificate.pem();
            let issuer = Issuer::new(ca_params, ca_key);

            let server_key = KeyPair::generate().expect("server key");
            let mut server_params =
                CertificateParams::new(vec!["localhost".to_owned()]).expect("server params");
            server_params
                .extended_key_usages
                .push(ExtendedKeyUsagePurpose::ServerAuth);
            let server_certificate = server_params
                .signed_by(&server_key, &issuer)
                .expect("server certificate");

            let (valid_client_cert_pem, valid_client_key_pem) = issue_client_certificate(
                &issuer,
                "spiffe://sdkwork.internal/sdkwork/service/sdkwork-knowledgebase",
            );
            let (rejected_client_cert_pem, rejected_client_key_pem) = issue_client_certificate(
                &issuer,
                "spiffe://sdkwork.internal/sdkwork/service/sdkwork-untrusted",
            );

            let temp_dir = tempfile::tempdir().expect("temporary certificate directory");
            let server_cert_path = temp_dir.path().join("server.pem");
            let server_key_path = temp_dir.path().join("server.key");
            let ca_path = temp_dir.path().join("ca.pem");
            std::fs::write(&server_cert_path, server_certificate.pem()).expect("server cert file");
            std::fs::write(&server_key_path, server_key.serialize_pem()).expect("server key file");
            std::fs::write(&ca_path, &ca_pem).expect("CA file");

            TestCertificates {
                _temp_dir: temp_dir,
                server_tls: crate::RpcServerTlsConfig {
                    server_cert_path,
                    server_key_path,
                    client_ca_certificate_path: Some(ca_path),
                    client_auth_optional: false,
                },
                ca_pem,
                valid_client_cert_pem,
                valid_client_key_pem,
                rejected_client_cert_pem,
                rejected_client_key_pem,
            }
        }

        fn issue_client_certificate(
            issuer: &Issuer<'_, KeyPair>,
            spiffe_uri: &str,
        ) -> (String, String) {
            let client_key = KeyPair::generate().expect("client key");
            let mut client_params =
                CertificateParams::new(Vec::<String>::new()).expect("client params");
            client_params.subject_alt_names =
                vec![SanType::URI(spiffe_uri.try_into().expect("SPIFFE URI SAN"))];
            client_params
                .extended_key_usages
                .push(ExtendedKeyUsagePurpose::ClientAuth);
            let client_certificate = client_params
                .signed_by(&client_key, issuer)
                .expect("client certificate");
            (client_certificate.pem(), client_key.serialize_pem())
        }

        fn security() -> RpcInternalServiceSecurity {
            let signing_key =
                RpcCallerContextSigningKey::from_secret_bytes(SIGNING_KEY).expect("signing key");
            RpcInternalServiceSecurity::new(
                RpcServiceIdentityPolicy::new("sdkwork.internal", ["sdkwork-knowledgebase"])
                    .expect("service identity policy"),
                Some(
                    RpcCallerContextVerifier::new(
                        "sdkwork-im",
                        [("sdkwork-knowledgebase", signing_key)],
                    )
                    .expect("caller context verifier"),
                ),
            )
        }

        async fn start_server(
            tls: &crate::RpcServerTlsConfig,
            security: RpcInternalServiceSecurity,
        ) -> (
            std::net::SocketAddr,
            oneshot::Sender<()>,
            tokio::task::JoinHandle<()>,
        ) {
            security
                .validate_mtls_listener(tls)
                .expect("strict mTLS listener config");
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
            let address = listener.local_addr().expect("listener address");
            let (_, health_service) = tonic_health::server::health_reporter();
            let wrapped = tonic::service::interceptor::InterceptedService::new(
                health_service,
                security.interceptor(),
            );
            let mut builder = crate::apply_server_tls(Server::builder(), tls).expect("TLS server");
            let router = builder.add_service(wrapped);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let server_task = tokio::spawn(async move {
                router
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .expect("mTLS test server");
            });
            (address, shutdown_tx, server_task)
        }

        async fn health_client(
            address: std::net::SocketAddr,
            ca_pem: &str,
            client_cert_pem: &str,
            client_key_pem: &str,
        ) -> HealthClient<tonic::transport::Channel> {
            let endpoint = Endpoint::from_shared(format!("https://{address}"))
                .expect("endpoint")
                .tls_config(
                    ClientTlsConfig::new()
                        .domain_name("localhost")
                        .ca_certificate(Certificate::from_pem(ca_pem))
                        .identity(Identity::from_pem(client_cert_pem, client_key_pem)),
                )
                .expect("TLS client config");
            let channel = tokio::time::timeout(Duration::from_secs(5), endpoint.connect())
                .await
                .expect("TLS client connect timeout")
                .expect("TLS client connection");
            HealthClient::new(channel)
        }

        fn signed_health_request() -> Request<HealthCheckRequest> {
            let signer = RpcCallerContextSigner::new(
                "sdkwork-knowledgebase",
                RpcCallerContextSigningKey::from_secret_bytes(SIGNING_KEY).expect("signing key"),
            )
            .expect("signer");
            let signed = signer
                .sign(
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
                    Duration::from_secs(60),
                )
                .expect("signed context");
            let mut request = Request::new(HealthCheckRequest {
                service: String::new(),
            });
            request.metadata_mut().insert(
                RPC_CALLER_CONTEXT_METADATA,
                signed
                    .encoded_context()
                    .parse()
                    .expect("caller context metadata"),
            );
            request.metadata_mut().insert(
                RPC_CALLER_CONTEXT_SIGNATURE_METADATA,
                signed.signature().parse().expect("signature metadata"),
            );
            request.metadata_mut().insert(
                RPC_IDEMPOTENCY_KEY_METADATA,
                "idempotency-1".parse().expect("idempotency metadata"),
            );
            request
        }

        #[tokio::test]
        async fn accepts_the_expected_mtls_spiffe_identity_and_signed_caller_context() {
            let certificates = issue_test_certificates();
            let (address, shutdown, task) =
                start_server(&certificates.server_tls, security()).await;
            let mut client = health_client(
                address,
                &certificates.ca_pem,
                &certificates.valid_client_cert_pem,
                &certificates.valid_client_key_pem,
            )
            .await;

            let response = client
                .check(signed_health_request())
                .await
                .expect("verified mTLS call should succeed");
            assert_eq!(
                response.into_inner().status,
                tonic_health::ServingStatus::Serving as i32
            );

            shutdown.send(()).expect("server shutdown");
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("server shutdown timeout")
                .expect("server task");
        }

        #[tokio::test]
        async fn rejects_wrong_spiffe_identity_and_spoofed_service_assertion() {
            let certificates = issue_test_certificates();
            let (address, shutdown, task) =
                start_server(&certificates.server_tls, security()).await;
            let mut rejected_client = health_client(
                address,
                &certificates.ca_pem,
                &certificates.rejected_client_cert_pem,
                &certificates.rejected_client_key_pem,
            )
            .await;
            let error = rejected_client
                .check(Request::new(HealthCheckRequest {
                    service: String::new(),
                }))
                .await
                .expect_err("untrusted SPIFFE service must be rejected");
            assert_eq!(error.code(), tonic::Code::Unauthenticated);

            let mut valid_client = health_client(
                address,
                &certificates.ca_pem,
                &certificates.valid_client_cert_pem,
                &certificates.valid_client_key_pem,
            )
            .await;
            let mut spoofed = signed_health_request();
            spoofed.metadata_mut().insert(
                RPC_SERVICE_IDENTITY_ASSERTION_METADATA,
                "sdkwork-untrusted".parse().expect("spoofed assertion"),
            );
            let error = valid_client
                .check(spoofed)
                .await
                .expect_err("spoofed service assertion must be rejected");
            assert_eq!(error.code(), tonic::Code::Unauthenticated);

            shutdown.send(()).expect("server shutdown");
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("server shutdown timeout")
                .expect("server task");
        }
    }
}
