//! SDKWork RPC server lifecycle helpers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sdkwork_rpc_discovery::DiscoveryInstanceHandle;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;
use tracing::{info, warn};

pub async fn serve_router_with_incoming(
    router: Router,
    bind_addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(bind_addr).await?;
    router
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

pub async fn serve_with_graceful_shutdown<F>(
    router: Router,
    bind_addr: &str,
    shutdown: F,
    on_shutdown: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    drain_timeout: Option<Duration>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind(bind_addr).await?;
    info!(bind_addr = %bind_addr, "sdkwork rpc server listening");

    let serve = router.serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown);
    match drain_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, serve).await {
            Ok(result) => result?,
            Err(_) => {
                warn!(
                    drain_timeout_ms = timeout.as_millis(),
                    "rpc server drain exceeded timeout; proceeding with shutdown hooks"
                );
            }
        },
        None => serve.await?,
    }

    if let Some(callback) = on_shutdown {
        callback.await;
    }

    Ok(())
}

pub async fn serve_with_discovery_lifecycle<F>(
    router: Router,
    bind_addr: &str,
    discovery: Arc<DiscoveryInstanceHandle>,
    shutdown: F,
    drain_timeout: Option<Duration>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: Future<Output = ()> + Send,
{
    let renew_task = discovery.spawn_renew_loop();
    let discovery_for_shutdown = Arc::clone(&discovery);
    let on_shutdown = Box::pin(async move {
        renew_task.abort();
        if let Err(error) = discovery_for_shutdown.deregister().await {
            tracing::warn!(error = %error, "discovery deregister failed during shutdown");
        }
    });

    serve_with_graceful_shutdown(router, bind_addr, shutdown, Some(on_shutdown), drain_timeout)
        .await
}

pub async fn wait_for_ctrl_c() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
