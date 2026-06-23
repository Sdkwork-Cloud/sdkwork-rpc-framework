#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcIdentityParts {
    pub namespace: String,
    pub environment: String,
    pub rpc_surface: String,
    pub proto_package: String,
    pub service: String,
    pub method: String,
    pub operation_id: String,
}

pub fn build_rpc_identity_uri(parts: &RpcIdentityParts) -> String {
    format!(
        "sdkwork-rpc://{}/{}/{}/{}/{}/{}?operationId={}",
        parts.namespace,
        parts.environment,
        parts.rpc_surface,
        parts.proto_package,
        parts.service,
        parts.method,
        parts.operation_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_canonical_identity_uri() {
        let uri = build_rpc_identity_uri(&RpcIdentityParts {
            namespace: "acme".to_string(),
            environment: "production".to_string(),
            rpc_surface: "internal".to_string(),
            proto_package: "sdkwork.communication.internal.v1".to_string(),
            service: "RoomOrchestrationService".to_string(),
            method: "CreateRoom".to_string(),
            operation_id: "rooms.create".to_string(),
        });

        assert_eq!(
            uri,
            "sdkwork-rpc://acme/production/internal/sdkwork.communication.internal.v1/RoomOrchestrationService/CreateRoom?operationId=rooms.create"
        );
    }
}
