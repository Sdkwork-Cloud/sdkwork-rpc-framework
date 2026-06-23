use std::sync::Arc;

use sdkwork_discovery_core::{ConfigPolicy, DiscoveryControlPlane, RegistryPolicy};
use sdkwork_discovery_rpc::{
    DiscoveryRpcRuntime, DiscoveryRpcRuntimeConfig, DiscoveryRpcServerConfig,
    DiscoveryRpcServerHandle, DiscoveryRpcServices, RuntimeResilienceConfig,
};
use sdkwork_discovery_storage_memory::MemoryDiscoveryStore;
use sdkwork_rpc_client::{DiscoveryNameResolver, DiscoveryNameResolverConfig, NameResolver};
use sdkwork_rpc_discovery::{
    build_registration_metadata, DiscoveryInstanceConfig, DiscoveryInstanceLifecycle,
    RegistrationMetadataInput,
};
use tokio::net::TcpListener;

#[tokio::test]
async fn framework_register_and_resolve_round_trip_against_local_discovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let discovery_endpoint = format!("http://{addr}");
    let advertised_endpoint = "grpc://127.0.0.1:51051".to_string();
    let service_name = "sdkwork-commerce-app-rpc".to_string();

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

    let server = DiscoveryRpcServerHandle::serve_with_listener(
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
    .expect("serve discovery");

    let metadata = build_registration_metadata(RegistrationMetadataInput {
        rpc_surface: "app",
        sdk_family: "sdkwork-commerce-rpc-sdk",
        domain: "commerce",
        proto_packages: &["sdkwork.commerce.app.v3"],
        operation_manifest_ref:
            "sdks/sdkwork-commerce-rpc-sdk/rpc/sdkwork-commerce-rpc.manifest.json",
        deployment_profile: None,
        runtime_target: None,
    });

    let handle = Arc::new(
        DiscoveryInstanceLifecycle::register(DiscoveryInstanceConfig {
            discovery_endpoint: discovery_endpoint.clone(),
            namespace: "sdkwork".to_string(),
            environment: "development".to_string(),
            service_name: service_name.clone(),
            instance_id: "commerce-rpc-e2e-1".to_string(),
            advertised_endpoint: advertised_endpoint.clone(),
            protocol: "grpc".to_string(),
            version: "0.1.0".to_string(),
            region: "local".to_string(),
            zone: "local".to_string(),
            lease_ttl_seconds: 30,
            subject_id: "sdkwork-commerce-service-host".to_string(),
            metadata,
            revision_cas_on_register: true,
            expected_revision: None,
        })
        .await
        .expect("register commerce rpc instance"),
    );

    let resolver = DiscoveryNameResolver::new(DiscoveryNameResolverConfig {
        discovery_endpoint,
        namespace: "sdkwork".to_string(),
        environment: "development".to_string(),
        subject_id: "sdkwork-commerce-rpc-client".to_string(),
        healthy_only: true,
        protocol: "grpc".to_string(),
    })
    .expect("resolver config");

    let endpoints = resolver
        .resolve(&service_name)
        .await
        .expect("resolve registered instance");

    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].endpoint, advertised_endpoint);
    assert!(endpoints[0].healthy);

    handle.deregister().await.expect("deregister");
    server.shutdown().await;
}

#[tokio::test]
async fn watching_resolver_refreshes_after_registration() {
    use sdkwork_rpc_client::WatchingDiscoveryNameResolver;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let discovery_endpoint = format!("http://{addr}");
    let advertised_endpoint = "grpc://127.0.0.1:52051".to_string();
    let service_name = "sdkwork-commerce-app-rpc".to_string();

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

    let server = DiscoveryRpcServerHandle::serve_with_listener(
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
    .expect("serve discovery");

    let resolver = Arc::new(
        WatchingDiscoveryNameResolver::new(DiscoveryNameResolverConfig {
            discovery_endpoint: discovery_endpoint.clone(),
            namespace: "sdkwork".to_string(),
            environment: "development".to_string(),
            subject_id: "sdkwork-commerce-rpc-client".to_string(),
            healthy_only: true,
            protocol: "grpc".to_string(),
        })
        .expect("watching resolver"),
    );
    let _watch_task = resolver.spawn_watch_loop(service_name.clone());

    let metadata = build_registration_metadata(RegistrationMetadataInput {
        rpc_surface: "app",
        sdk_family: "sdkwork-commerce-rpc-sdk",
        domain: "commerce",
        proto_packages: &["sdkwork.commerce.app.v3"],
        operation_manifest_ref:
            "sdks/sdkwork-commerce-rpc-sdk/rpc/sdkwork-commerce-rpc.manifest.json",
        deployment_profile: None,
        runtime_target: None,
    });

    let handle = Arc::new(
        DiscoveryInstanceLifecycle::register(DiscoveryInstanceConfig {
            discovery_endpoint,
            namespace: "sdkwork".to_string(),
            environment: "development".to_string(),
            service_name: service_name.clone(),
            instance_id: "commerce-rpc-watch-e2e-1".to_string(),
            advertised_endpoint: advertised_endpoint.clone(),
            protocol: "grpc".to_string(),
            version: "0.1.0".to_string(),
            region: "local".to_string(),
            zone: "local".to_string(),
            lease_ttl_seconds: 30,
            subject_id: "sdkwork-commerce-service-host".to_string(),
            metadata,
            revision_cas_on_register: true,
            expected_revision: None,
        })
        .await
        .expect("register instance"),
    );

    let mut resolved = None;
    for _ in 0..20 {
        if let Ok(endpoints) = resolver.resolve(&service_name).await {
            if endpoints
                .iter()
                .any(|endpoint| endpoint.endpoint == advertised_endpoint)
            {
                resolved = Some(endpoints);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let endpoints = resolved.expect("watching resolver should observe registered instance");
    assert_eq!(endpoints[0].endpoint, advertised_endpoint);

    handle.deregister().await.expect("deregister");
    server.shutdown().await;
}

#[tokio::test]
async fn revision_cas_allows_safe_reregister_with_same_instance_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let discovery_endpoint = format!("http://{addr}");
    let service_name = "sdkwork-commerce-app-rpc".to_string();
    let instance_id = "commerce-rpc-cas-e2e-1".to_string();

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

    let server = DiscoveryRpcServerHandle::serve_with_listener(
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
    .expect("serve discovery");

    let metadata = build_registration_metadata(RegistrationMetadataInput {
        rpc_surface: "app",
        sdk_family: "sdkwork-commerce-rpc-sdk",
        domain: "commerce",
        proto_packages: &["sdkwork.commerce.app.v3"],
        operation_manifest_ref:
            "sdks/sdkwork-commerce-rpc-sdk/rpc/sdkwork-commerce-rpc.manifest.json",
        deployment_profile: None,
        runtime_target: None,
    });

    let base_config = || DiscoveryInstanceConfig {
        discovery_endpoint: discovery_endpoint.clone(),
        namespace: "sdkwork".to_string(),
        environment: "development".to_string(),
        service_name: service_name.clone(),
        instance_id: instance_id.clone(),
        advertised_endpoint: String::new(),
        protocol: "grpc".to_string(),
        version: "0.1.0".to_string(),
        region: "local".to_string(),
        zone: "local".to_string(),
        lease_ttl_seconds: 30,
        subject_id: "sdkwork-commerce-service-host".to_string(),
        metadata: metadata.clone(),
        revision_cas_on_register: true,
        expected_revision: None,
    };

    let first_endpoint = "grpc://127.0.0.1:53051".to_string();
    let second_endpoint = "grpc://127.0.0.1:53052".to_string();

    let first_handle = Arc::new(
        DiscoveryInstanceLifecycle::register(DiscoveryInstanceConfig {
            advertised_endpoint: first_endpoint.clone(),
            ..base_config()
        })
        .await
        .expect("first register"),
    );

    let second_handle = Arc::new(
        DiscoveryInstanceLifecycle::register(DiscoveryInstanceConfig {
            advertised_endpoint: second_endpoint.clone(),
            ..base_config()
        })
        .await
        .expect("cas-safe re-register"),
    );

    let resolver = DiscoveryNameResolver::new(DiscoveryNameResolverConfig {
        discovery_endpoint: discovery_endpoint.clone(),
        namespace: "sdkwork".to_string(),
        environment: "development".to_string(),
        subject_id: "sdkwork-commerce-rpc-client".to_string(),
        healthy_only: true,
        protocol: "grpc".to_string(),
    })
    .expect("resolver config");

    let endpoints = resolver
        .resolve(&service_name)
        .await
        .expect("resolve after re-register");
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].endpoint, second_endpoint);

    second_handle.deregister().await.expect("deregister");
    let _ = first_handle;
    server.shutdown().await;
}
