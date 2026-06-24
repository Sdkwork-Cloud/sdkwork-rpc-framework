# sdkwork-rpc-framework

SDKWork canonical gRPC integration framework: discovery registration, name resolution, resilience profiles, transport helpers, and server lifecycle.

## Standards

- L0: `../sdkwork-specs/RPC_FRAMEWORK_SPEC.md`, `DISCOVERY_SPEC.md`, `RPC_RESILIENCE_SPEC.md`
- L1: `specs/RPC_FRAMEWORK_STANDARD.md`
- Agent entry: `AGENTS.md`

## Crates

| Crate | Library name | Responsibility |
| --- | --- | --- |
| `sdkwork-rpc-core` | `sdkwork_rpc_framework_core` | Identity URI, profiles, bootstrap stages, RPC surface |
| `sdkwork-rpc-resilience` | `sdkwork_rpc_resilience` | Retry policy, backoff, circuit breaker, idempotency admission |
| `sdkwork-rpc-discovery` | `sdkwork_rpc_discovery` | Registration metadata and discovery lifecycle |
| `sdkwork-rpc-client` | `sdkwork_rpc_client` | Resolvers, load balancing, transport, call metadata |
| `sdkwork-rpc-server` | `sdkwork_rpc_server` | Graceful shutdown and discovery-aware serve lifecycle |

## Production integration checklist

- Register RPC instances only after listeners are ready (`sdkwork-rpc-discovery`)
- Use `WatchingDiscoveryNameResolver` or `CompositeNameResolver` in production
- Resolve and connect through `resolve_and_connect`, not ad hoc tonic channels
- Apply `RpcCallMetadata` for auth, trace, and idempotency keys
- Select resilience profiles with `ResilienceProfile::is_production_safe()`
- Use `serve_with_discovery_lifecycle` with drain timeout for shutdown ordering

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

