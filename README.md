# sdkwork-rpc-framework

SDKWork canonical gRPC integration framework: discovery registration, name resolution, resilience profiles, transport helpers, TLS/mTLS, and server lifecycle.

## Standards

- L0: `../sdkwork-specs/RPC_FRAMEWORK_SPEC.md`, `DISCOVERY_SPEC.md`, `RPC_RESILIENCE_SPEC.md`
- L1: `specs/RPC_FRAMEWORK_STANDARD.md`
- Agent entry: `AGENTS.md`

## Crates

| Crate | Library name | Responsibility |
| --- | --- | --- |
| `sdkwork-rpc-core` | `sdkwork_rpc_framework_core` | Identity URI, profiles, bootstrap stages, RPC surface, `RpcFrameworkError` |
| `sdkwork-rpc-resilience` | `sdkwork_rpc_resilience` | Retry policy, backoff, circuit breaker, idempotency admission, gRPC pushback parsing |
| `sdkwork-rpc-discovery` | `sdkwork_rpc_discovery` | Registration metadata, discovery lifecycle, renew loop with backoff |
| `sdkwork-rpc-client` | `sdkwork_rpc_client` | Resolvers, load balancing, transport, TLS/mTLS config, call metadata, watch resolver, cross-call retry budget registry |
| `sdkwork-rpc-server` | `sdkwork_rpc_server` | Graceful shutdown, discovery-aware serve lifecycle, TLS/mTLS termination |

## Production integration checklist

- Register RPC instances only after listeners are ready (`sdkwork-rpc-discovery`)
- Use `WatchingDiscoveryNameResolver` or `CompositeNameResolver` in production
- Resolve and connect through `resolve_and_connect`, not ad hoc tonic channels
- Apply `RpcCallMetadata` for auth, trace, and idempotency keys
- Select resilience profiles with `ResilienceProfile::is_production_safe()`
- Use `serve_with_discovery_lifecycle` with drain timeout for shutdown ordering
- Enable TLS/mTLS in production: enable the `tls` cargo feature and supply `RpcTlsConfig` (client) / `RpcServerTlsConfig` (server) with separate cert and key paths
- Configure `RenewLoopConfig` with appropriate backoff for discovery lease renewal resilience
- Use typed `RpcFrameworkError` variants (`Transport`, `Discovery`) for retry decisions
- Use `should_retry_call_with_pushback` for retry decisions so `RESOURCE_EXHAUSTED` retries only when the server signals `grpc-retry-pushback-ms`
- Instantiate `RetryBudgetRegistry` at client bootstrap and share it across RPC client factories to bound the cross-call retry rate per service

## Verify

```powershell
.\scripts\verify.ps1
```

```bash
cargo test --workspace
```

## Documentation Canon

- [docs/README.md](docs/README.md)
- [docs/product/prd/PRD.md](docs/product/prd/PRD.md)
- [docs/architecture/tech/TECH_ARCHITECTURE.md](docs/architecture/tech/TECH_ARCHITECTURE.md)

