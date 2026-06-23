//! Bootstrap and shutdown stage names per `RPC_FRAMEWORK_SPEC.md` section 8.

pub const STAGE_VALIDATE_RPC_CONTRACTS: &str = "validate-rpc-contracts";
pub const STAGE_INITIALIZE_RPC_FRAMEWORK: &str = "initialize-rpc-framework";
pub const STAGE_BIND_RPC_SERVICES: &str = "bind-rpc-services";
pub const STAGE_REGISTER_DISCOVERY_INSTANCE: &str = "register-discovery-instance";
pub const STAGE_START_DISCOVERY_RENEW_LOOP: &str = "start-discovery-renew-loop";

pub const STAGE_DRAIN_RPC_SERVERS: &str = "drain-rpc-servers";
pub const STAGE_DEREGISTER_DISCOVERY_INSTANCE: &str = "deregister-discovery-instance";
pub const STAGE_STOP_DISCOVERY_RENEW_LOOP: &str = "stop-discovery-renew-loop";

pub const BOOTSTRAP_STAGES: &[&str] = &[
    STAGE_VALIDATE_RPC_CONTRACTS,
    STAGE_INITIALIZE_RPC_FRAMEWORK,
    STAGE_BIND_RPC_SERVICES,
    STAGE_REGISTER_DISCOVERY_INSTANCE,
    STAGE_START_DISCOVERY_RENEW_LOOP,
];

pub const SHUTDOWN_STAGES: &[&str] = &[
    STAGE_DRAIN_RPC_SERVERS,
    STAGE_DEREGISTER_DISCOVERY_INSTANCE,
    STAGE_STOP_DISCOVERY_RENEW_LOOP,
];
