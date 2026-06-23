use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistrationMetadataInput<'a> {
    pub rpc_surface: &'a str,
    pub sdk_family: &'a str,
    pub domain: &'a str,
    pub proto_packages: &'a [&'a str],
    pub operation_manifest_ref: &'a str,
    pub deployment_profile: Option<&'a str>,
    pub runtime_target: Option<&'a str>,
}

pub fn build_registration_metadata(
    input: RegistrationMetadataInput<'_>,
) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::from([
        ("rpc_surface".to_string(), input.rpc_surface.to_string()),
        ("sdk_family".to_string(), input.sdk_family.to_string()),
        ("domain".to_string(), input.domain.to_string()),
        ("proto_packages".to_string(), input.proto_packages.join(",")),
        (
            "operation_manifest_ref".to_string(),
            input.operation_manifest_ref.to_string(),
        ),
    ]);

    if let Some(deployment_profile) = input.deployment_profile {
        metadata.insert(
            "deployment_profile".to_string(),
            deployment_profile.to_string(),
        );
    }

    if let Some(runtime_target) = input.runtime_target {
        metadata.insert("runtime_target".to_string(), runtime_target.to_string());
    }

    metadata
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn includes_required_discovery_metadata_keys() {
        let metadata = build_registration_metadata(RegistrationMetadataInput {
            rpc_surface: "internal",
            sdk_family: "sdkwork-commerce-rpc-sdk",
            domain: "commerce",
            proto_packages: &["sdkwork.commerce.app.v3", "sdkwork.commerce.backend.v3"],
            operation_manifest_ref:
                "sdks/sdkwork-commerce-rpc-sdk/rpc/sdkwork-commerce-rpc.manifest.json",
            deployment_profile: Some("standalone"),
            runtime_target: Some("server"),
        });

        assert_eq!(
            metadata.get("rpc_surface").map(String::as_str),
            Some("internal")
        );
        assert_eq!(
            metadata.get("sdk_family").map(String::as_str),
            Some("sdkwork-commerce-rpc-sdk")
        );
        assert!(metadata
            .get("proto_packages")
            .unwrap()
            .contains("sdkwork.commerce.app.v3"));
    }
}
