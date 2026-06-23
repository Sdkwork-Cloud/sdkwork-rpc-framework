use sdkwork_im_rpc_service_rust::{
    initialize_im_rpc_framework_from_env, register_im_discovery_instance,
    ImRpcServerConfig, IM_DISCOVERY_ENDPOINT_ENV, IM_DISCOVERY_SERVICE_NAME_ENV,
    IM_RPC_RESOLVER_PROFILE_ENV,
};
use sdkwork_discovery_core::{ConfigPolicy, DiscoveryControlPlane, RegistryPolicy};
use sdkwork_discovery_rpc::{
    DiscoveryRpcRuntime, DiscoveryRpcRuntimeConfig, DiscoveryRpcServerConfig,
    DiscoveryRpcServerHandle, DiscoveryRpcServices, RuntimeResilienceConfig,
};
use sdkwork_discovery_storage_memory::MemoryDiscoveryStore;
use sdkwork_rpc_framework_core::ResolverProfile;
use tokio::net::TcpListener;

#[tokio::test]
async fn im_bootstrap_resolves_registered_rpc_instance() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let discovery_endpoint = format!("http://{addr}");
    let advertised_endpoint = "grpc://127.0.0.1:55051".to_string();
    let service_name = "sdkwork-communication-app-rpc".to_string();

    let server = start_local_discovery_server(listener).await;

    unsafe {
        std::env::set_var(IM_DISCOVERY_ENDPOINT_ENV, &discovery_endpoint);
        std::env::set_var(IM_DISCOVERY_SERVICE_NAME_ENV, &service_name);
        std::env::set_var(IM_RPC_RESOLVER_PROFILE_ENV, "discovery");
    }

    let server_config = ImRpcServerConfig {
        bind_addr: "127.0.0.1:55051".to_string(),
        public_endpoint: Some(advertised_endpoint.clone()),
        enable_health: true,
        ..ImRpcServerConfig::local_default()
    };

    let discovery_handle = register_im_discovery_instance(&server_config)
        .await
        .expect("register im instance")
        .expect("discovery enabled");

    let framework = initialize_im_rpc_framework_from_env().expect("framework bootstrap");
    assert_eq!(framework.resolver_profile, ResolverProfile::Discovery);
    framework
        .verify_client_resolution()
        .await
        .expect("verify client resolution");

    let resolved = framework
        .client_resolver
        .as_ref()
        .expect("client resolver inventory")
        .resolve_primary_endpoint()
        .await
        .expect("resolve endpoint");
    assert_eq!(resolved, advertised_endpoint);

    discovery_handle.deregister().await.expect("deregister");
    server.shutdown().await;

    unsafe {
        std::env::remove_var(IM_DISCOVERY_ENDPOINT_ENV);
        std::env::remove_var(IM_DISCOVERY_SERVICE_NAME_ENV);
        std::env::remove_var(IM_RPC_RESOLVER_PROFILE_ENV);
    }
}

async fn start_local_discovery_server(
    listener: tokio::net::TcpListener,
) -> DiscoveryRpcServerHandle {
    let addr = listener.local_addr().expect("local addr");
    let runtime = DiscoveryRpcRuntime::with_config(
        DiscoveryControlPlane::new(
            MemoryDiscoveryStore::new(),
            ConfigPolicy {
                enabled: true,
                require_publish_for_reads: true,
                allow_secret_values: false,
                allow_secret_refs: true,
                max_config_body_bytes: 1024,
            },
            RegistryPolicy::default(),
        ),
        DiscoveryRpcRuntimeConfig {
            registry_expiry_scan_interval_ms: 0,
            registry_expiry_scan_batch_size: 1_000,
            allow_unsigned_local_context: true,
            service_token_verifier: None,
            event_gc_interval_ms: 0,
            event_gc_retention_count: 10_000,
            event_gc_batch_size: 1_000,
            resilience: RuntimeResilienceConfig::default(),
            health_check_scan_interval_ms: 0,
        },
    );

    DiscoveryRpcServerHandle::serve_with_listener(
        DiscoveryRpcServerConfig {
            bind_addr: addr.to_string(),
            enable_health: true,
            enable_reflection: false,
            default_deadline_ms: 5_000,
            watch_enabled: true,
            watch_max_streams: 1_000,
            watch_event_buffer_size: 256,
            watch_heartbeat_interval_ms: 15_000,
            watch_durable_poll_interval_ms: 1_000,
            watch_durable_replay_batch_size: 1_000,
            require_tls: false,
            tls_identity: None,
            client_ca_certificate_pem: None,
        },
        DiscoveryRpcServices::new(runtime),
        listener,
    )
    .await
    .expect("serve discovery")
}
