# Rpc Framework Technical Architecture

Status: stable
Owner: SDKWork maintainers
Updated: 2026-06-26
Specs: ARCHITECTURE_DECISION_SPEC.md, DOCUMENTATION_SPEC.md

## Document Map

- Add `TECH-<topic>.md` shards in this directory when the architecture grows beyond one reviewable screen.

## 1. Architecture Overview

The SDKWork RPC framework is a layered gRPC integration framework built on Tonic 0.14.6. It provides discovery-aware service registration, name resolution with watch-driven caching, resilience profiles with retry/circuit-breaker/budget policies, TLS/mTLS transport security, and graceful server lifecycle management.

**Design principles:**

- **High cohesion, low coupling**: each crate owns a single concern (identity, resilience, discovery, client, server)
- **Open-closed**: resilience profiles and resolver strategies are extensible via traits without modifying existing code
- **Spec-driven**: all behaviors align with `sdkwork-specs/RPC_FRAMEWORK_SPEC.md`, `DISCOVERY_SPEC.md`, and `RPC_RESILIENCE_SPEC.md`
- **Production-safe defaults**: plaintext is the dev default; TLS/mTLS is gated behind a cargo feature that returns errors when misconfigured

## 2. Technology Choices

| Concern | Choice | Rationale |
| --- | --- | --- |
| gRPC framework | Tonic 0.14.6 | Production-grade async Rust gRPC with TLS, keepalive, and streaming support |
| Async runtime | Tokio | Industry standard for Rust async; workspace-shared |
| TLS | rustls (via tonic `tls-ring` + `tls-webpki-roots`) | No OpenSSL dependency; webpki roots for public CA verification |
| Tracing | `tracing` crate with structured fields | Operational observability at all boundary events |
| Error handling | `thiserror` with typed variants | Callers can match on error kind for retry decisions |
| Identity | UUID v4 + canonical URI | Globally unique instance IDs; W3C-compliant traceparent |
| Backoff | Full Jitter (Google SRE) | Prevents thundering-herd on reconnect/retry |

## 3. System Boundaries And Modules

```
┌─────────────────────────────────────────────────────────┐
│                   Business RPC Service                   │
│  (registers via sdkwork-rpc-discovery, serves via Tonic) │
└───────────────┬──────────────────────────┬──────────────┘
                │                          │
    ┌───────────▼──────────┐   ┌──────────▼──────────┐
    │  sdkwork-rpc-server   │   │  sdkwork-rpc-client  │
    │  - graceful shutdown  │   │  - name resolution   │
    │  - drain + deregister │   │  - load balancing    │
    │  - TLS termination    │   │  - TLS/mTLS channel  │
    │  - renew lifecycle    │   │  - watch resolver    │
    │                      │   │  - RetryBudgetRegistry│
    └───────────┬──────────┘   └──────────┬──────────┘
                │                          │
    ┌───────────▼──────────────────────────▼──────────┐
    │              sdkwork-rpc-discovery                 │
    │  - register/renew/deregister lifecycle            │
    │  - RenewLoopConfig with exponential backoff       │
    │  - SubjectTokenProvider for auth metadata         │
    │  - W3C traceparent generation                     │
    └───────────────────────┬──────────────────────────┘
                            │
    ┌───────────────────────▼──────────────────────────┐
    │              sdkwork-rpc-resilience               │
    │  - RetryPolicy (6 profiles)                       │
    │  - should_retry_call_with_pushback (gRPC A6)     │
    │  - CircuitBreaker (closed/open/half-open)        │
    │  - RetryBudgetTracker (per-call)                 │
    │  - IdempotencyAdmission (deadline-aware)         │
    │  - retry_backoff_ms (Full Jitter)                │
    └───────────────────────┬──────────────────────────┘
                            │
    ┌───────────────────────▼──────────────────────────┐
    │              sdkwork-rpc-core                     │
    │  - RpcFrameworkError (4 variants)                │
    │  - RpcIdentityParts / build_rpc_identity_uri     │
    │  - ResilienceProfile / ResolverProfile enums     │
    │  - Bootstrap/shutdown stage constants            │
    │  - RpcSurface validation                        │
    └──────────────────────────────────────────────────┘
```

## 4. Directory And Package Layout

```
sdkwork-rpc-framework/
├── crates/
│   ├── sdkwork-rpc-core/          # Identity, profiles, errors, bootstrap
│   ├── sdkwork-rpc-resilience/    # Retry, circuit breaker, idempotency
│   ├── sdkwork-rpc-discovery/     # Registration, renew, metadata, TLS
│   ├── sdkwork-rpc-client/        # Resolvers, transport, TLS, watch
│   └── sdkwork-rpc-server/        # Shutdown, drain, TLS termination
├── specs/
│   └── RPC_FRAMEWORK_STANDARD.md  # L1 executable standard
├── tests/architecture/            # Architecture conformance tests
├── docs/                          # Architecture, PRD, runbooks
└── Cargo.toml                     # Workspace manifest
```

## 5. API, SDK, And Data Ownership

- **RPC identity**: `sdkwork-rpc://{namespace}/{environment}/{surface}/{package}/{Service}/{Method}?operationId={id}`
- **Discovery metadata**: `rpc_surface`, `sdk_family`, `domain`, `proto_packages`, `operation_manifest_ref`
- **W3C traceparent**: `00-{trace_id(32hex)}-{parent_id(16hex)}-01`
- **Error variants**: `Validation`, `Configuration`, `Transport`, `Discovery`
- **TLS paths**: separate cert + key files (Kubernetes `tls.crt` + `tls.key` convention)
- **Retry pushback**: `grpc-retry-pushback-ms` trailer governs `RESOURCE_EXHAUSTED` retryability (gRPC A6)
- **Retry budgets**: per-call `RetryBudgetTracker` (resilience) + cross-call `RetryBudgetRegistry` per `service_name` (client)

## 6. Security, Privacy, And Observability

### Security
- TLS/mTLS gated behind `tls` cargo feature; plaintext returns configuration error
- mTLS client verification via `client_ca_certificate_path` + `client_auth_optional`
- Subject token providers for discovery auth metadata
- Cert/key validation enforces paired configuration

### Observability
Structured tracing events at all operational boundaries (see `RPC_FRAMEWORK_STANDARD.md` §12 for the full target/event table).

## 7. Deployment And Runtime Topology

- **Server**: bind → register with discovery → serve → drain → deregister → abort renew
- **Client**: resolve (cache or RPC) → load balance → connect (cached channel) → invoke with retry/circuit-breaker
- **Watch loop**: connect (shared channel) → stream events → cache update → reconnect on failure with backoff
- **Renew loop**: steady-state interval → failure backoff → threshold alert → recovery log

## 8. Architecture Decision Index

- ADR-001: Use `Arc<OnceCell<Channel>>` for discovery channel reuse (avoids per-call TCP handshakes)
- ADR-002: Separate cert/key paths instead of combined PEM (matches K8s/Envoy/nginx convention)
- ADR-003: Full Jitter backoff for both retry and reconnect (Google SRE)
- ADR-004: 4-variant `RpcFrameworkError` for typed retry decisions
- ADR-005: `RenewLoopConfig` with exponential backoff and threshold alerting
- ADR-006: gRPC A6 pushback governs `RESOURCE_EXHAUSTED` retryability; absence ⇒ no retry
- ADR-007: Cross-call retry budget via monotonic-clock token bucket (`RetryBudgetRegistry`) keyed by `service_name`, distinct from per-call `RetryBudgetTracker`
- ADR-008: Framework owns cross-cutting pipeline stages; business owns auth/authorization/dispatch (open-closed principle, see `RPC_FRAMEWORK_STANDARD.md` §9)

## 9. Verification

- `cargo test --workspace` — 89 tests across all crates
- `cargo check --features tls` — TLS feature path compilation
- Architecture tests: identity, metadata, resolver, resilience, load balancing, retry budget, pushback, cross-call budget
- E2E: register → resolve → watch round-trip against local discovery
