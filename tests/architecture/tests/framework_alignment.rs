use sdkwork_rpc_client::{
    CompositeNameResolver, DiscoveryNameResolver, DiscoveryNameResolverConfig, NameResolver,
    StaticNameResolver,
};
use sdkwork_rpc_discovery::{
    build_registration_metadata, grpc_advertised_endpoint, normalize_http_endpoint,
    RegistrationMetadataInput,
};
use sdkwork_rpc_framework_core::{
    build_rpc_identity_uri, ResilienceProfile, ResolverProfile, RpcIdentityParts,
};
use sdkwork_rpc_resilience::RetryPolicy;

#[test]
fn framework_exports_identity_and_profiles() {
    let uri = build_rpc_identity_uri(&RpcIdentityParts {
        namespace: "sdkwork".into(),
        environment: "development".into(),
        rpc_surface: "app".into(),
        proto_package: "sdkwork.commerce.app.v3".into(),
        service: "WalletService".into(),
        method: "RetrieveWalletOverview".into(),
        operation_id: "wallet.overview.retrieve".into(),
    });
    assert!(uri.contains("wallet.overview.retrieve"));
    assert_eq!(
        ResolverProfile::parse("discovery"),
        Some(ResolverProfile::Discovery)
    );
    assert_eq!(
        ResilienceProfile::parse("rpc-default"),
        Some(ResilienceProfile::RpcDefault)
    );
}

#[test]
fn registration_metadata_and_retry_policy_align_with_specs() {
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
    assert_eq!(metadata.get("rpc_surface").map(String::as_str), Some("app"));

    let policy = RetryPolicy::for_profile(ResilienceProfile::RpcDefault);
    assert!(policy.allows_retry(tonic::Code::Unavailable, 1));

    assert_eq!(
        grpc_advertised_endpoint("127.0.0.1:50051"),
        "grpc://127.0.0.1:50051"
    );
    assert_eq!(
        normalize_http_endpoint("127.0.0.1:19090"),
        "http://127.0.0.1:19090"
    );
}

#[tokio::test]
async fn static_resolver_is_usable_from_client_crate() {
    let resolver = StaticNameResolver::single("grpc://127.0.0.1:50051");
    let endpoints = resolver.resolve("sdkwork-commerce-app-rpc").await.unwrap();
    assert_eq!(endpoints[0].endpoint, "grpc://127.0.0.1:50051");
}

#[tokio::test]
async fn composite_resolver_falls_back_to_static_endpoints() {
    use std::sync::Arc;

    struct FailingResolver;

    #[async_trait::async_trait]
    impl NameResolver for FailingResolver {
        async fn resolve(
            &self,
            _service_name: &str,
        ) -> Result<
            Vec<sdkwork_rpc_client::ResolvedEndpoint>,
            sdkwork_rpc_framework_core::RpcFrameworkError,
        > {
            Err(
                sdkwork_rpc_framework_core::RpcFrameworkError::Configuration(
                    "discovery unavailable".to_string(),
                ),
            )
        }
    }

    let composite = CompositeNameResolver::new(
        Arc::new(FailingResolver),
        StaticNameResolver::single("grpc://127.0.0.1:51052"),
    );

    let endpoints = composite
        .resolve("sdkwork-commerce-app-rpc")
        .await
        .expect("fallback resolve");

    assert_eq!(endpoints[0].endpoint, "grpc://127.0.0.1:51052");
}

#[test]
fn discovery_resolver_is_exported_from_client_crate() {
    assert!(DiscoveryNameResolver::new(DiscoveryNameResolverConfig {
        discovery_endpoint: "http://127.0.0.1:50051".to_string(),
        namespace: "sdkwork".to_string(),
        environment: "development".to_string(),
        subject_id: "resolver-1".to_string(),
        healthy_only: true,
        protocol: "grpc".to_string(),
    })
    .is_ok());
}
