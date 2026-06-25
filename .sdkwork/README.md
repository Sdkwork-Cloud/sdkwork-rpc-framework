# SDKWork Workspace

This `.sdkwork/` directory is source-controlled workspace metadata for `sdkwork-rpc-framework`.

Owner: sdkwork-rpc-framework repository maintainers.

It is governed by `../sdkwork-specs/SDKWORK_WORKSPACE_SPEC.md` and follows the standards entrypoint at `../sdkwork-specs/README.md`.

Authoritative local entries:

- `README.md`: purpose and ownership for this workspace metadata directory.
- `skills/README.md`: repository skill contribution guidance.
- `plugins/README.md`: repository plugin contribution guidance.
- `manifests/`: optional non-secret workspace manifests when tooling needs them.

This directory is not runtime state. Do not store generated SDK transport output, secrets, local credentials, runtime databases, cache, logs, or user-private files here.

## Verification

- Agent entrypoint: `AGENTS.md`
- Shared execution soul: `../sdkwork-specs/SOUL.md`
- Repository baseline: `../sdkwork-specs/REPOSITORY_BASELINE_SPEC.md`