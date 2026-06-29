# Repository Guidelines

Domain: `platform`  
Capability: `rpc-framework`  
Type: **基础底层框架仓库**（非业务产品）  
Status: `stable`

SDKWork **所有带 gRPC/RPC 能力仓库**所依赖的 RPC 基础框架：tonic 服务生命周期、discovery 注册/解析、韧性策略、调用上下文。

## SDKWORK Soul

Read `../sdkwork-specs/SOUL.md` before executing repository tasks.

## SDKWORK Standards

Canonical entrypoint: `../sdkwork-specs/README.md`. Do not copy root standards into this repository.

## Application Identity

This repository is a platform framework workspace, not an SDKWork application root.

## Local Dictionary Structure

- `AGENTS.md` — agent execution rules (this file).
- `specs/` — framework L1 standard and `component.spec.json`.
- `crates/` — Rust framework crates.
- `tests/architecture/` — cross-crate alignment verification.
- `scripts/` — thin verification entrypoints.

## Documentation Canon

- [docs/README.md](docs/README.md)
- [docs/product/prd/PRD.md](docs/product/prd/PRD.md)
- [docs/architecture/tech/TECH_ARCHITECTURE.md](docs/architecture/tech/TECH_ARCHITECTURE.md)

## Spec Resolution Order

1. This `AGENTS.md`.
2. `specs/component.spec.json` and `specs/RPC_FRAMEWORK_STANDARD.md`.
3. `../sdkwork-specs/README.md` and task-specific root specs.
4. Implementation files.

## Required Specs By Task Type

| Task | Required specs |
| --- | --- |
| Agent/workflow rules | `../sdkwork-specs/SOUL.md`, `../sdkwork-specs/AGENTS_SPEC.md`, `../sdkwork-specs/SDKWORK_WORKSPACE_SPEC.md` |
| Any code change | `../sdkwork-specs/CODE_STYLE_SPEC.md`, `../sdkwork-specs/NAMING_SPEC.md`, `../sdkwork-specs/RUST_CODE_SPEC.md` |
| RPC framework/runtime | `../sdkwork-specs/RPC_FRAMEWORK_SPEC.md`, `specs/RPC_FRAMEWORK_STANDARD.md`, `../sdkwork-specs/RPC_SPEC.md` |
| Discovery integration | `../sdkwork-specs/DISCOVERY_SPEC.md`, `../sdkwork-specs/ENVIRONMENT_SPEC.md` §16 |
| Resilience | `../sdkwork-specs/RPC_RESILIENCE_SPEC.md` |
| Rust RPC adapters | `../sdkwork-specs/RUST_RPC_SPEC.md` |
| Verification | `../sdkwork-specs/TEST_SPEC.md` §2.2.1–2.2.3, `../sdkwork-specs/QUALITY_GATE_SPEC.md` |
Build scripts, dev runners, and `pnpm clean` must follow `CODE_STYLE_SPEC.md` §7 (Build Source Integrity And Self-Healing). Git-tracked build-critical source files must be verified before builds and self-healed from git when missing; `clean` must not delete them.


## 定位

- **是**：RPC identity、resolver、discovery lifecycle、resilience profile、server graceful shutdown
- **不是**：业务 proto、业务 RPC adapter、discovery 控制面服务实现
- **依赖**：`sdkwork-appbase` (`sdkwork-rpc-core-rust`)、`sdkwork-discovery` (`sdkwork-discovery-rpc-proto`)、`sdkwork-utils-rust`（单向，本仓库 **不** 依赖业务仓库）

## Verification

From repository root:

```bash
cargo test --workspace
```

On Windows:

```powershell
.\scripts\verify.ps1
```

## HTTP API Response Envelope

All L2+ `app-api`, `backend-api`, and SDKWork-owned business `open-api` HTTP contracts `MUST` follow `API_SPEC.md` section 4.5, section 14, and section 15:

- **Input:** typed request bodies, section 14.1 list/search/command input, `SdkWorkListQuery`, and `q` for free-text search.
- **Success output:** `SdkWorkApiResponse` with `{ "code": 0, "data": <payload>, "traceId": "<server-uuid>" }`.
- **Error output:** HTTP 4xx/5xx `application/problem+json` (`ProblemDetail`) with numeric `code` and `traceId`.
- Success `code` is numeric `int32`; HTTP 2xx JSON bodies `MUST` use `0` only. REST semantics remain on HTTP status (`201`, `202`, etc.).
- Platform error codes are numeric non-zero values per section 15.3 (`40001`, `40101`, `40401`, …).
- Single resource: `data.item`
- Lists: `data.items` + `data.pageInfo` (`PageInfo.mode` is `offset` or `cursor`)
- Commands: `data.accepted` plus optional `resourceId` / `status`
- Async accept (`202`): `data.operationId`, `data.status`, optional `pollUrl`

Vendor compatibility `open-api` routes that mirror upstream tool or provider wire (for example OpenAI `/v1/*`, Claude Code, Codex) `MAY` opt out only when every exempt operation declares `x-sdkwork-wire-protocol: external` and `x-sdkwork-external-protocol-id` per `API_SPEC.md` section 4.5.2. SDKWork-owned business `open-api` operations `MUST NOT` opt out.

Errors `MUST` use HTTP 4xx/5xx with `application/problem+json` (`ProblemDetail`) including required numeric `code` and `traceId`. Business failures `MUST NOT` use HTTP 2xx with non-zero `code`, string wire codes, `success`, or human `message`.

Forbidden legacy envelopes and fields: `PlusApiResult`, `AppbaseApiResult`, `StoreApiResult`, `SdkWorkResponse`, per-domain `*ApiResult`, wire field `requestId`, bare domain DTOs at the HTTP root, and top-level `{ items, pageInfo, traceId }` without `data`.

Handlers `MUST` serialize success and map errors through `sdkwork-web-framework` response mapping. Generated HTTP SDKs (`--standard-profile sdkwork-v3`) unwrap `data` by default and expose typed numeric `ProblemDetail.code` / `traceId` on errors; use `.raw` when the full envelope is required.

Before completing API contract, SDK generation, or frontend service work, run:

```bash
node <sdkwork-specs>/tools/check-api-response-envelope.mjs --workspace <workspace-root>
```

Authority: `sdkwork-specs/API_SPEC.md` section 4.5 and sections 14–16, `SDK_SPEC.md` section 4.2, `FRONTEND_SPEC.md`, `MIGRATION_SPEC.md` section 4.2.

## HTTP API Response Envelope

All L2+ `app-api`, `backend-api`, and SDKWork-owned `open-api` success JSON bodies `MUST` use `SdkWorkResponse` from `API_SPEC.md` §15:

- Envelope: `{ "data": <payload>, "requestId": "<server-uuid>" }`
- Single resource: `data.item`
- Lists: `data.items` + `data.pageInfo` (`PageInfo.mode` is `offset` or `cursor`)
- Commands: `data.accepted` plus optional `resourceId` / `status`
- Async accept (`202`): `data.operationId`, `data.status`, optional `pollUrl`

Errors `MUST` use HTTP 4xx/5xx with `application/problem+json` (`ProblemDetail`). Business failures `MUST NOT` use HTTP 2xx with `success`, `code`, or `message`.

Forbidden legacy envelopes: `PlusApiResult`, `AppbaseApiResult`, `StoreApiResult`, per-domain `*ApiResult`, bare domain DTOs at the HTTP root, and top-level `{ items, pageInfo, requestId }` without `data`.

Handlers `MUST` serialize success and map errors through `sdkwork-web-framework` response mapping. Do not hand-build envelopes in controllers or route handlers.

Generated HTTP SDKs (`--standard-profile sdkwork-v3`) unwrap `data` by default; use `.raw` only when correlation headers or the full envelope are required.

Before completing API contract or handler work, run:

```bash
node <sdkwork-specs>/tools/check-api-response-envelope.mjs --workspace <workspace-root>
```

Authority: `sdkwork-specs/API_SPEC.md` §15–§16, `WEB_FRAMEWORK_SPEC.md`, `SDK_SPEC.md` §4.1, `MIGRATION_SPEC.md` §API Response Envelope Migration.
