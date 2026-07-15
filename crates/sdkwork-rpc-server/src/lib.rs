//! SDKWork RPC server lifecycle helpers.
//!
//! Provides graceful shutdown orchestration that aligns with
//! `RPC_RESILIENCE_SPEC.md` §7 and `RPC_FRAMEWORK_STANDARD.md` §8:
//!
//! 1. Drain in-flight RPC traffic against the configured `drain_timeout`.
//! 2. Deregister the discovery instance so clients stop receiving new picks.
//! 3. Abort the lease renew loop only after deregister completes.
//!
//! The previous implementation aborted the renew loop before deregister,
//! creating a window in which the discovery control plane could expire the
//! lease before the explicit deregister RPC completed, surfacing as
//! `NOT_FOUND` noise and stale-instance picks on the client side.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use sdkwork_rpc_discovery::DiscoveryInstanceHandle;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;
use tracing::{error, info, warn};

mod security;

pub use security::{
    require_verified_rpc_caller_context, require_verified_rpc_service_identity,
    RpcInternalServiceInterceptor, RpcInternalServiceSecurity,
};

/// Error returned by the server lifecycle helpers.
///
/// Typed variants (rather than `Box<dyn Error>`) let callers distinguish
/// bind failures, serve failures, and discovery deregister failures so they
/// can retry or surface the right operational signal.
#[derive(Debug)]
pub enum ServeError {
    /// Failed to bind the TCP listener or serve the tonic router.
    Transport(String),
    /// Failed to deregister the discovery instance during shutdown.
    DiscoveryDeregister(String),
    /// Failed to install the Ctrl-C signal handler.
    Signal(String),
}

impl fmt::Display for ServeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(message) => write!(f, "rpc server transport error: {message}"),
            Self::DiscoveryDeregister(message) => {
                write!(f, "rpc server discovery deregister error: {message}")
            }
            Self::Signal(message) => write!(f, "rpc server signal handler error: {message}"),
        }
    }
}

impl Error for ServeError {}

/// Serves the tonic router with a caller-provided shutdown signal.
///
/// The `shutdown_signal` future may produce any output type; only its
/// completion triggers graceful shutdown. The caller is responsible for
/// emitting the signal (typically via [`wait_for_ctrl_c`]). Use
/// [`serve_with_discovery_lifecycle`] when the server is registered with the
/// SDKWork discovery control plane so the deregister/renew ordering is
/// honored automatically.
pub async fn serve_with_graceful_shutdown<F>(
    router: Router,
    bind_addr: &str,
    shutdown_signal: F,
) -> Result<(), ServeError>
where
    F: std::future::Future + Send + 'static,
{
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|error| ServeError::Transport(error.to_string()))?;
    let incoming = TcpListenerStream::new(listener);
    router
        .serve_with_incoming_shutdown(incoming, async {
            let _ = shutdown_signal.await;
        })
        .await
        .map_err(|error| ServeError::Transport(error.to_string()))
}

/// Serves the tonic router with discovery lifecycle management.
///
/// The caller provides the `shutdown_signal` future, allowing flexible
/// shutdown triggers (Ctrl-C, SIGTERM, watch channels, etc.).
///
/// Shutdown order (per `RPC_RESILIENCE_SPEC.md` §7 and
/// `RPC_FRAMEWORK_STANDARD.md` §8):
///
/// 1. Serve traffic concurrently while waiting for the shutdown signal.
/// 2. On shutdown signal, drain in-flight RPCs against `drain_timeout`; if
///    the timeout fires, in-flight RPCs are cancelled and a
///    `forced_shutdown` warning is logged so operations can distinguish
///    graceful drain from forced teardown.
/// 3. Deregister the discovery instance (so clients stop receiving new picks).
/// 4. Abort the lease renew loop only after deregister completes.
pub async fn serve_with_discovery_lifecycle<F>(
    router: Router,
    bind_addr: &str,
    discovery_handle: Arc<DiscoveryInstanceHandle>,
    shutdown_signal: F,
    drain_timeout: Option<Duration>,
) -> Result<(), ServeError>
where
    F: std::future::Future + Send + 'static,
{
    let renew_task = discovery_handle.spawn_renew_loop();
    serve_with_discovery_lifecycle_and_renew_task(
        router,
        bind_addr,
        discovery_handle,
        shutdown_signal,
        drain_timeout,
        renew_task,
    )
    .await
}

/// Same as [`serve_with_discovery_lifecycle`] but accepts an externally
/// spawned renew task, enabling tests to drive the renew loop without an
/// actual discovery control plane.
pub async fn serve_with_discovery_lifecycle_and_renew_task<F>(
    router: Router,
    bind_addr: &str,
    discovery_handle: Arc<DiscoveryInstanceHandle>,
    shutdown_signal: F,
    drain_timeout: Option<Duration>,
    renew_task: JoinHandle<()>,
) -> Result<(), ServeError>
where
    F: std::future::Future + Send + 'static,
{
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|error| ServeError::Transport(error.to_string()))?;
    let incoming = TcpListenerStream::new(listener);

    // Use a oneshot channel so we can trigger graceful shutdown after the
    // caller-provided signal fires, while the serve future runs concurrently.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = router.serve_with_incoming_shutdown(incoming, async move {
        let _ = shutdown_rx.await;
    });

    tokio::pin!(serve);

    // Run the server and the shutdown signal concurrently. If the server
    // stops on its own (fatal error), skip the drain phase. If the shutdown
    // signal fires first, trigger graceful shutdown and drain in-flight RPCs.
    tokio::select! {
        result = &mut serve => {
            result.map_err(|error| ServeError::Transport(error.to_string()))?;
        }
        _ = shutdown_signal => {
            info!(
                target: "sdkwork.rpc.server.shutdown",
                "shutdown signal received, draining rpc server"
            );

            // Trigger graceful shutdown so tonic emits HTTP/2 GOAWAY and
            // clients can retry against healthy instances.
            let _ = shutdown_tx.send(());

            match drain_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, &mut serve).await {
                    Ok(Ok(())) => info!(
                        target: "sdkwork.rpc.server.drain",
                        "rpc server drained within timeout"
                    ),
                    Ok(Err(error)) => {
                        return Err(ServeError::Transport(error.to_string()));
                    }
                    Err(_) => {
                        warn!(
                            target: "sdkwork.rpc.server.drain",
                            drain_timeout_ms = timeout.as_millis() as u64,
                            "rpc server drain exceeded timeout; in-flight RPCs will be cancelled"
                        );
                    }
                },
                None => {
                    (&mut serve)
                        .await
                        .map_err(|error| ServeError::Transport(error.to_string()))?;
                }
            }
        }
    }

    // Deregister discovery instance FIRST so the control plane stops
    // advertising this server to resolvers while we tear down locally.
    if let Err(error) = discovery_handle.deregister().await {
        error!(
            target: "sdkwork.rpc.server.discovery",
            error = %error,
            "discovery deregister failed during shutdown"
        );
        // Still abort the renew task so the process can exit; surface the
        // deregister failure to the caller so it can be retried or alerted.
        renew_task.abort();
        return Err(ServeError::DiscoveryDeregister(error.to_string()));
    }
    info!(
        target: "sdkwork.rpc.server.discovery",
        "discovery instance deregistered cleanly"
    );

    // Abort the renew loop only AFTER deregister completes. Aborting
    // earlier would let the lease expire during a slow deregister RPC,
    // causing the control plane to evict the instance before the explicit
    // deregister lands.
    renew_task.abort();

    Ok(())
}

/// Waits for the Ctrl-C signal, returning an error if the signal handler
/// could not be installed (e.g., when running outside a TTY).
///
/// Previously this swallowed the error silently, causing the server to appear
/// alive when no shutdown signal would ever arrive. Now the caller can decide
/// how to fall back (e.g., bind a Unix signal directly or exit immediately).
pub async fn wait_for_ctrl_c() -> Result<(), ServeError> {
    tokio::signal::ctrl_c()
        .await
        .map_err(|error| ServeError::Signal(error.to_string()))
}

/// TLS/mTLS configuration for the gRPC server.
///
/// Implements `RPC_FRAMEWORK_SPEC.md` §10 "Transport security | TLS/mTLS
/// termination or approved local-only exemption" and `DISCOVERY_SPEC.md` §11
/// "mTLS client verification SHOULD be enabled for service-to-service
/// production".
///
/// Callers apply this to a `tonic::transport::Server` builder via
/// [`apply_server_tls`] before constructing the `Router` passed to the
/// `serve_with_*` functions. TLS is configured on the builder, not the router,
/// because tonic bakes the TLS acceptor into the router at construction time.
///
/// The server certificate and private key are configured as separate paths
/// (rather than a combined PEM), matching the industry convention used by
/// Kubernetes TLS secrets (`tls.crt` + `tls.key`), Envoy
/// `tls_context.common_tls_context.tls_certificates`, and nginx
/// `ssl_certificate` / `ssl_certificate_key`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RpcServerTlsConfig {
    /// Path to the PEM-encoded server certificate chain.
    pub server_cert_path: std::path::PathBuf,
    /// Path to the PEM-encoded server private key.
    pub server_key_path: std::path::PathBuf,
    /// Path to a PEM-encoded CA certificate used to verify client certificates
    /// for mTLS. When `None`, the server does not request client certificates
    /// (one-way TLS only).
    pub client_ca_certificate_path: Option<std::path::PathBuf>,
    /// When `true`, client certificate verification is optional (mTLS soft
    /// mode). Defaults to `false` (hard mTLS: clients without a valid cert are
    /// rejected). Ignored when `client_ca_certificate_path` is `None`.
    pub client_auth_optional: bool,
}

impl RpcServerTlsConfig {
    /// Validates that configured paths exist, returning a typed error if not.
    pub fn validate(&self) -> Result<(), ServeError> {
        if !self.server_cert_path.exists() {
            return Err(ServeError::Transport(format!(
                "tls server_cert_path does not exist: {}",
                self.server_cert_path.display()
            )));
        }
        if !self.server_key_path.exists() {
            return Err(ServeError::Transport(format!(
                "tls server_key_path does not exist: {}",
                self.server_key_path.display()
            )));
        }
        if let Some(path) = &self.client_ca_certificate_path {
            if !path.exists() {
                return Err(ServeError::Transport(format!(
                    "tls client_ca_certificate_path does not exist: {}",
                    path.display()
                )));
            }
        }
        Ok(())
    }

    /// Validates the strict mTLS profile required by internal production RPC.
    ///
    /// One-way TLS and optional client authentication are valid for other
    /// listener types, but they cannot establish a trustworthy service
    /// identity and therefore must not back an internal service interceptor.
    pub fn validate_mtls(&self) -> Result<(), ServeError> {
        if self.client_ca_certificate_path.is_none() {
            return Err(ServeError::Transport(
                "strict mTLS requires client_ca_certificate_path".to_owned(),
            ));
        }
        if self.client_auth_optional {
            return Err(ServeError::Transport(
                "strict mTLS requires client_auth_optional=false".to_owned(),
            ));
        }
        self.validate()
    }
}

/// Applies TLS configuration to a tonic `Server` builder.
///
/// Callers build their `Router` from the returned server:
///
/// ```ignore
/// let tls = RpcServerTlsConfig {
///     server_cert_path: "server.crt".into(),
///     server_key_path: "server.key".into(),
///     ..Default::default()
/// };
/// let server = apply_server_tls(tonic::transport::Server::builder(), &tls)?;
/// let router = server.add_service(my_service);
/// serve_with_graceful_shutdown(router, "0.0.0.0:50051", wait_for_ctrl_c()).await?;
/// ```
///
/// When the `tls` cargo feature is disabled, this returns a `ServeError`
/// directing the caller to enable the feature.
#[cfg(feature = "tls")]
pub fn apply_server_tls(
    builder: tonic::transport::Server,
    config: &RpcServerTlsConfig,
) -> Result<tonic::transport::Server, ServeError> {
    config.validate()?;
    let server_cert = std::fs::read(&config.server_cert_path).map_err(|error| {
        ServeError::Transport(format!(
            "failed to read tls server cert {}: {error}",
            config.server_cert_path.display()
        ))
    })?;
    let server_key = std::fs::read(&config.server_key_path).map_err(|error| {
        ServeError::Transport(format!(
            "failed to read tls server key {}: {error}",
            config.server_key_path.display()
        ))
    })?;
    let identity = tonic::transport::Identity::from_pem(server_cert, server_key);
    let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);

    if let Some(client_ca_path) = &config.client_ca_certificate_path {
        let ca_pem = std::fs::read(client_ca_path).map_err(|error| {
            ServeError::Transport(format!(
                "failed to read tls client ca {}: {error}",
                client_ca_path.display()
            ))
        })?;
        let cert = tonic::transport::Certificate::from_pem(ca_pem);
        tls = tls
            .client_ca_root(cert)
            .client_auth_optional(config.client_auth_optional);
    }

    builder
        .tls_config(tls)
        .map_err(|error| ServeError::Transport(error.to_string()))
}

/// Returns an error directing the caller to enable the `tls` feature.
#[cfg(not(feature = "tls"))]
pub fn apply_server_tls(
    _builder: tonic::transport::Server,
    _config: &RpcServerTlsConfig,
) -> Result<tonic::transport::Server, ServeError> {
    Err(ServeError::Transport(
        "tls config supplied but the `tls` cargo feature is not enabled; \
         enable `sdkwork-rpc-server/tls` for production TLS/mTLS"
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_error_display_distinguishes_variants() {
        assert!(format!("{}", ServeError::Transport("bind failed".into())).contains("transport"));
        assert!(
            format!("{}", ServeError::DiscoveryDeregister("not found".into()))
                .contains("deregister")
        );
        assert!(format!("{}", ServeError::Signal("no tty".into())).contains("signal"));
    }

    #[test]
    fn serve_error_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ServeError>();
    }
}
