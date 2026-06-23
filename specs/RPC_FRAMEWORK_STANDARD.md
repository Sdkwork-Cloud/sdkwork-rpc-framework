# SDKWork RPC Framework Standard

- Version: 1.0
- Scope: `sdkwork-rpc-framework` repository — gRPC server/client integration framework for SDKWork RPC-capable repositories
- Status: stable
- Authority: narrows `../sdkwork-specs/RPC_FRAMEWORK_SPEC.md`, `DISCOVERY_SPEC.md`, `RPC_RESILIENCE_SPEC.md`, and `RUST_RPC_SPEC.md`; does not contradict root specs
- Related: `specs/component.spec.json`, `tests/architecture/`

## 1. Purpose

This standard defines the executable profile for SDKWork RPC runtime integration: crate boundaries, resolver and discovery lifecycle APIs, resilience profiles, and verification expectations. Business repositories register RPC services and consume generated RPC SDKs through these crates.

## 2. Dependency Rule

- RPC-enabled business repositories `MUST` depend on `sdkwork-rpc-framework` workspace crates.
- `sdkwork-rpc-framework` `MUST NOT` depend on any business repository, business RPC server crate, or application-owned proto authority.

## 3. Crate Matrix

| Crate | Responsibility |
| --- | --- |
| `sdkwork-rpc-core` (`sdkwork_rpc_framework_core`) | RPC identity URI, resolver/resilience profile enums, re-export of `sdkwork-rpc-core-rust` manifest primitives |
| `sdkwork-rpc-resilience` | Retry policy and retry-budget tracking per resilience profile |
| `sdkwork-rpc-discovery` | Discovery registration metadata builder and register/renew/deregister lifecycle |
| `sdkwork-rpc-client` | `NameResolver` trait, resolvers, load balancing, `GrpcChannelConfig`, `resolve_and_connect`, `RpcCallMetadata` |
| `sdkwork-rpc-server` | Graceful shutdown helpers and discovery-aware serve lifecycle |

## 4. RPC Identity

`build_rpc_identity_uri` and `RpcIdentityParts` in `sdkwork-rpc-core` `MUST` produce:

```text
sdkwork-rpc://{namespace}/{environment}/{rpc_surface}/{proto_package}/{Service}/{Method}?operationId={dotted.id}
```

## 5. Resolver Profiles

| Profile | Implementation |
| --- | --- |
| `static` | `StaticNameResolver` |
| `static-composite` | Business composite tables wrapping `StaticNameResolver` |
| `discovery` | `DiscoveryNameResolver` via `DiscoverInstances` with registry read metadata |
| `composite` | `CompositeNameResolver` — discovery primary with `StaticNameResolver` fallback |

`WatchingDiscoveryNameResolver` `SHOULD` be used in production paths to refresh endpoint snapshots through `WatchService` instead of polling `DiscoverInstances`.

`DiscoveryNameResolver` `MUST`:

- call `DiscoverInstances` with `healthy_only` and `protocol = grpc` unless a method documents otherwise
- map `SERVING` and `DEGRADED` instances to resolver output
- fail when no healthy endpoint is returned

## 6. Discovery Registration

`sdkwork-rpc-discovery` `MUST` expose:

- `build_registration_metadata` with required keys: `rpc_surface`, `sdk_family`, `domain`, `proto_packages`, `operation_manifest_ref`
- `DiscoveryInstanceLifecycle::register` with lease renew loop
- `grpc_advertised_endpoint` for advertised gRPC URIs

Service hosts `MUST` call registration only after RPC listeners are ready and deregister during graceful shutdown.

## 7. Resilience Profiles

`ResilienceProfile` names `MUST` match `RPC_RESILIENCE_SPEC.md`:

- `rpc-default`
- `rpc-read-only`
- `rpc-idempotent-write`
- `rpc-critical-write`
- `rpc-stream`
- `rpc-local-dev`

`sdkwork-rpc-resilience` `MUST` enforce profile-specific retry whitelists and budgets.

`sdkwork-rpc-resilience` `MUST` expose:

- `retry_backoff_ms` with exponential delay and jitter
- `RetryPolicy::should_retry` combining attempt limits, status whitelist, and retry budget
- `CircuitBreaker` with closed/open/half-open states for per-target fail-fast

`sdkwork-rpc-client` `MUST` expose `pick_endpoint` with `pick_first`, `round_robin`, and `weighted` algorithms over healthy resolver snapshots.

`sdkwork-rpc-client` `MUST` expose:

- `GrpcChannelConfig` with connect timeout and HTTP/2 keepalive defaults
- `resolve_and_connect` combining resolver output, load balancing, and channel creation
- `RpcCallMetadata` applying standard metadata keys from `sdkwork-rpc-core-rust`

`sdkwork-rpc-resilience` `MUST` expose `should_retry_call` with idempotency admission rules.

`sdkwork-rpc-core` `MUST` expose bootstrap/shutdown stage constants and `RpcSurface` validation.

## 8. Server Bootstrap

`serve_with_graceful_shutdown` and `serve_with_discovery_lifecycle` in `sdkwork-rpc-server` `MUST` be used by service hosts instead of ad hoc tonic serve loops when discovery or drain ordering is required.

Both helpers `MAY` accept an optional drain timeout; when exceeded the framework `MUST` log the event and continue shutdown hooks.

Shutdown order:

1. drain RPC servers
2. deregister discovery instance
3. stop renew loop

## 9. Verification

Repository verification `MUST` include:

- `cargo test --workspace`
- `cargo test -p sdkwork-rpc-framework-architecture-tests`
- `node ../sdkwork-specs/tools/check-rpc-framework-standard.mjs` when run from parent workspace verify pipelines

Architecture tests `MUST` prove identity URI construction, registration metadata keys, static resolver usability, resilience profile alignment, load balancing, and retry budget behavior.

## 10. Business Integration Checklist

- [ ] RPC manifest declares `discoveryServiceName` and `defaultResilienceProfile` when dynamic resolution is used
- [ ] Service host registers with discovery through `sdkwork-rpc-discovery`
- [ ] Clients resolve through framework resolvers, not raw tonic channels
- [ ] Metadata keys use `sdkwork-rpc-core-rust` constants
- [ ] Graceful shutdown deregisters discovery instances
