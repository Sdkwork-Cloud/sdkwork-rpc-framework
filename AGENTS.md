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
