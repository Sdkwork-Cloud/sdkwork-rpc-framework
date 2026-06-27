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
| `sdkwork-rpc-core` (`sdkwork_rpc_framework_core`) | RPC identity URI, resolver/resilience profile enums, `RpcFrameworkError` variants, re-export of `sdkwork-rpc-core-rust` manifest primitives |
| `sdkwork-rpc-resilience` | Retry policy and retry-budget tracking per resilience profile |
| `sdkwork-rpc-discovery` | Discovery registration metadata builder, register/renew/deregister lifecycle, `RenewLoopConfig` with exponential backoff |
| `sdkwork-rpc-client` | `NameResolver` trait, resolvers, load balancing, `GrpcChannelConfig` with TLS, `resolve_and_connect`, `RpcCallMetadata`, `WatchingDiscoveryNameResolver`, `RpcTlsConfig` |
| `sdkwork-rpc-server` | Graceful shutdown helpers, discovery-aware serve lifecycle, `RpcServerTlsConfig` |

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

`DiscoveryNameResolver` `MUST` cache the discovery control-plane channel via `Arc<OnceCell<Channel>>` so discover RPCs and watch sessions share a single TCP+HTTP2 connection. Tonic channels auto-reconnect on transient failures.

`DiscoveryNameResolver` `MUST`:

- call `DiscoverInstances` with `healthy_only` and `protocol = grpc` unless a method documents otherwise
- map `SERVING` and `DEGRADED` instances to resolver output
- fail when no healthy endpoint is returned

## 6. Discovery Registration

`sdkwork-rpc-discovery` `MUST` expose:

- `build_registration_metadata` with required keys: `rpc_surface`, `sdk_family`, `domain`, `proto_packages`, `operation_manifest_ref`
- `DiscoveryInstanceLifecycle::register` with lease renew loop
- `RenewLoopConfig` with `initial_backoff`, `max_backoff`, and `max_consecutive_failures` for exponential backoff on renewal failure
- `spawn_renew_loop_with_config` accepting a `RenewLoopConfig` for tunable backoff
- `grpc_advertised_endpoint` for advertised gRPC URIs

The renew loop `MUST`:

- In steady state, sleep for `renew_interval` (derived from `lease_ttl_seconds / 3`)
- On failure, switch to exponential backoff with Full Jitter (Google SRE)
- After `max_consecutive_failures`, log a critical error indicating the lease has likely expired
- On recovery, log an info event with the previous failure count

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
- `RetryPolicy::should_retry_with_deadline` combining attempt limits, status whitelist, retry budget, and remaining deadline
- `CircuitBreaker` with closed/open/half-open states for per-target fail-fast
- `should_retry_call_with_pushback` enforcing idempotency admission, deadline, and server pushback (`grpc-retry-pushback-ms`) per `RPC_RESILIENCE_SPEC.md` §4.1
- `extract_retry_pushback_ms` and `effective_retry_backoff_ms` for call sites that resolve backoff separately from the decision

`sdkwork-rpc-client` `MUST` expose `pick_endpoint` with `pick_first`, `round_robin`, and `weighted` algorithms over healthy resolver snapshots.

`sdkwork-rpc-client` `MUST` expose:

- `GrpcChannelConfig` with connect timeout and HTTP/2 keepalive defaults
- `resolve_and_connect` combining resolver output, load balancing, and channel creation
- `RpcCallMetadata` applying standard metadata keys from `sdkwork-rpc-core-rust`
- `RetryBudgetRegistry` with per-`service_name` token bucket enforcing the cross-call retry budget per `RPC_RESILIENCE_SPEC.md` §4.2; exhaustion `MUST` fail fast and log `service_name` + `operationId`

`sdkwork-rpc-resilience` `MUST` expose `should_retry_call` and `should_retry_call_with_deadline` with idempotency admission rules. New call sites `SHOULD` prefer the deadline-aware variant.

`sdkwork-rpc-core` `MUST` expose bootstrap/shutdown stage constants and `RpcSurface` validation.

## 8. Server Bootstrap

`serve_with_graceful_shutdown` and `serve_with_discovery_lifecycle` in `sdkwork-rpc-server` `MUST` be used by service hosts instead of ad hoc tonic serve loops when discovery or drain ordering is required.

Both helpers `MAY` accept an optional drain timeout; when exceeded the framework `MUST` log the event and continue shutdown hooks.

Shutdown order:

1. drain RPC servers
2. deregister discovery instance
3. stop renew loop

## 9. Pipeline Stage Coverage

`RPC_FRAMEWORK_SPEC.md` §5 (server) and §6 (client) define the standard pipeline stages. This framework owns the cross-cutting stages and exposes composable primitives for them; business repositories own the domain-specific stages and assemble the full pipeline through tonic interceptors at bootstrap. This split keeps the framework high-cohesion/low-coupling and honors the open-closed principle: adding a business auth scheme does not require editing framework code.

### Server Pipeline (`RPC_FRAMEWORK_SPEC.md` §5)

| Stage | Owner | Framework primitive |
| --- | --- | --- |
| 1. Transport security | Framework | `RpcServerTlsConfig` + `apply_server_tls` |
| 2. Metadata normalization | Tonic | tonic built-in lowercase header handling |
| 3. Deadline and cancellation | Tonic | `grpc-timeout` honored by tonic runtime |
| 4. Trace propagation | Framework | `rpc_metadata` W3C `traceparent` generation in `sdkwork-rpc-discovery` |
| 5. Auth and context resolution | Business | injected via interceptor; framework provides `RpcCallMetadata` keys |
| 6. Idempotency admission | Business | framework exposes `RetryAdmission` for the client-side mirror |
| 7. Authorization | Business | `MUST NOT` be implemented only in client SDKs (spec §5) |
| 8. Proto validation | Tonic/prost | generated message types enforce wire-level validation |
| 9. Operation dispatch | Business | maps to `operationId` in domain runtime |
| 10. Error mapping | Framework | `RpcFrameworkError` variants; business maps domain errors to gRPC status |
| 11. Audit and metrics | Framework | structured tracing at `sdkwork.rpc.*` targets |

### Client Pipeline (`RPC_FRAMEWORK_SPEC.md` §6)

| Stage | Owner | Framework primitive |
| --- | --- | --- |
| 1. Invocation context | Framework | `RpcInvocationContext`, `RpcCallMetadata`, `RetryBudgetRegistry` |
| 2. Name resolution | Framework | `StaticNameResolver`, `DiscoveryNameResolver`, `WatchingDiscoveryNameResolver`, `CompositeNameResolver` |
| 3. Load balancing | Framework | `pick_endpoint` (`pick_first`, `round_robin`, `weighted`) |
| 4. Transport | Framework | `GrpcChannelConfig`, `connect_grpc_channel_with_config`, `resolve_and_connect`, `RpcTlsConfig` |
| 5. Call execution | Business | generated tonic stubs over the framework channel |
| 6. Resilience | Framework | `RetryPolicy`, `should_retry_call_with_pushback`, `CircuitBreaker`, `RetryBudgetTracker`, `RetryBudgetRegistry` |
| 7. Observability | Framework | tracing events at `sdkwork.rpc.*` targets |

Rules:

- Business repositories `MUST` assemble the server pipeline through tonic interceptors in the order above; the framework `MUST NOT` own business auth/authorization/dispatch.
- Framework-owned stages `MUST` be reused via their primitives rather than reimplemented in business code.
- When a framework primitive is missing for a declared stage, the gap `MUST` be filed against this standard before adding a local implementation.

## 10. Transport Security (TLS/mTLS)

`sdkwork-rpc-client` and `sdkwork-rpc-server` `MUST` expose TLS/mTLS configuration entry points gated behind the `tls` cargo feature. When the feature is disabled, supplying a TLS config `MUST` return a configuration error so production builds cannot silently fall back to plaintext.

### Client TLS

`RpcTlsConfig` in `sdkwork-rpc-client` `MUST`:

- Use separate paths for client certificate (`client_cert_path`) and private key (`client_key_path`), matching Kubernetes TLS secret (`tls.crt` + `tls.key`) and Envoy/nginx conventions
- Enforce that `client_cert_path` and `client_key_path` are both set or both unset via `validate()`
- Support custom CA certificates via `server_ca_certificate_path`; when `None`, use the webpki root store
- Support SNI domain override via `domain`
- Attach to `GrpcChannelConfig.tls` and apply via `connect_grpc_channel_with_config`

### Server TLS

`RpcServerTlsConfig` in `sdkwork-rpc-server` `MUST`:

- Use separate paths for server certificate (`server_cert_path`) and private key (`server_key_path`)
- Support mTLS client verification via `client_ca_certificate_path` with `client_auth_optional` flag
- Apply to a tonic `Server` builder via `apply_server_tls` before constructing the `Router`

## 11. Error Handling

`RpcFrameworkError` in `sdkwork-rpc-core` `MUST` expose four typed variants:

| Variant | Semantics | Retry guidance |
| --- | --- | --- |
| `Validation` | Caller input is invalid (blank fields, malformed URIs) | Not retryable without correcting input |
| `Configuration` | Framework/deployment config is invalid (missing TLS feature, unreadable cert paths) | Not retryable without correcting config |
| `Transport` | Transport-level failure (TCP refused, TLS handshake, HTTP/2 protocol) | Often transient, retryable with backoff |
| `Discovery` | Service discovery failure (no instances, discover RPC error) | Retryable after backoff if control plane recovers |

All `map_err` call sites `MUST` use the most specific variant, not default to `Configuration`.

## 12. Observability

The framework `MUST` emit structured tracing events at operational boundaries:

| Target | Events |
| --- | --- |
| `sdkwork.rpc.transport.connect` | channel connect attempt (debug), connect failure (warn) |
| `sdkwork.rpc.discovery.connect` | control plane connect attempt (debug), connect failure (warn) |
| `sdkwork.rpc.discovery.resolve` | discover attempt (debug), empty results (warn), success with count (debug) |
| `sdkwork.rpc.discovery.watch` | watch session start (info), stream error (warn), clean end (info) |
| `sdkwork.rpc.discovery.renew` | renewal failure (warn), recovery (info), threshold exceeded (error) |
| `sdkwork.rpc.server.shutdown` | shutdown signal (info) |
| `sdkwork.rpc.server.drain` | drain complete (info), drain timeout exceeded (warn) |
| `sdkwork.rpc.server.discovery` | deregister success (info), deregister failure (error) |
| `sdkwork.rpc.retry.deadline` | retry skipped due to insufficient deadline (warn) |
| `sdkwork.rpc.retry.budget` | retry skipped due to budget exhaustion (warn) |

## 13. Verification

Repository verification `MUST` include:

- `cargo test --workspace`
- `cargo test -p sdkwork-rpc-framework-architecture-tests`
- `cargo check --features tls` on `sdkwork-rpc-client` and `sdkwork-rpc-server` (TLS feature path compilation)
- `cargo clippy --workspace --all-targets` with zero warnings

Architecture tests `MUST` prove identity URI construction, registration metadata keys, static resolver usability, resilience profile alignment, load balancing, retry budget behavior, pushback parsing, and cross-call retry budget enforcement.

## 14. Business Integration Checklist

- [ ] RPC manifest declares `discoveryServiceName` and `defaultResilienceProfile` when dynamic resolution is used
- [ ] Service host registers with discovery through `sdkwork-rpc-discovery`
- [ ] Clients resolve through framework resolvers, not raw tonic channels
- [ ] Metadata keys use `sdkwork-rpc-core-rust` constants
- [ ] Graceful shutdown deregisters discovery instances
- [ ] TLS/mTLS enabled in production via `tls` cargo feature
- [ ] `RenewLoopConfig` tuned for the deployment's lease TTL
- [ ] Error handling uses typed `RpcFrameworkError` variants for retry decisions
- [ ] Retry decisions use `should_retry_call_with_pushback` (or the deadline-aware variant) so `RESOURCE_EXHAUSTED` retries only when the server signals pushback
- [ ] `RetryBudgetRegistry` instantiated at client bootstrap and shared across RPC client factories to bound cross-call retry rate per service
- [ ] Server pipeline assembled through tonic interceptors in spec §5 order, reusing framework primitives for framework-owned stages
