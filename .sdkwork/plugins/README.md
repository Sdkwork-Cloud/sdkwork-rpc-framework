# Repository Plugins

Repository-local agent plugins may be added here when sdkwork-rpc-framework needs checked-in plugin bundles.

An installable plugin must live at `.sdkwork/plugins/<plugin-name>/.codex-plugin/plugin.json`, use lowercase kebab-case, and document contributed skills, tools, apps, scripts, or MCP servers.

Do not vendor unrelated toolchains, generated SDK transport output, secrets, runtime databases, caches, logs, or user-private files in plugins.