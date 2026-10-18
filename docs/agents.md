# Use with coding agents

## CLI integration

Install GPU Queue once in an environment reachable from your local agents. Each agent can call `gpu-queue` directly or use an absolute path to the installed executable. An isolated or remote agent must have access to the local worker host to submit jobs; installing a snippet does not bridge machines.

Merge the appropriate snippet into the project's instruction file:

| Client | Snippet | Official instruction documentation |
| --- | --- | --- |
| Codex | [AGENTS.md](../integrations/codex/AGENTS.md) | [Custom instructions](https://developers.openai.com/codex/guides/agents-md) |
| Claude Code | [CLAUDE.md](../integrations/claude-code/CLAUDE.md) | [Project memory](https://code.claude.com/docs/en/memory) |
| Gemini CLI | [GEMINI.md](../integrations/gemini-cli/GEMINI.md) | [Context files](https://geminicli.com/docs/cli/gemini-md/) |
| Other local agents | [Generic instructions](../integrations/generic/INSTRUCTIONS.md) | Use that agent's project instruction mechanism. |

Instruction filenames were checked against the linked official documentation on October 18, 2026. Client configuration and policy can affect instruction loading. The snippets describe the same CLI contract; they do not claim that each client has undergone end-to-end integration testing.

Use project scope initially. If you want every project to participate, merge the instructions into your agent's supported user-wide instruction file yourself. Preserve existing rules. Configure the same `GPU_QUEUE_STATE_DIR` wherever you override the default, including desktop-launched agents whose environment may differ from a terminal.

## Optional local MCP adapter

The stdio adapter offers structured access for clients supporting the implemented MCP protocol version. The universal fallback is the CLI. Start it with:

```sh
gpu-queue-mcp
```

By default, only read tools are exposed. To allow job submission, cancellation, reordering and cooperative pause/resume through an appropriately trusted local client:

```sh
gpu-queue-mcp --allow-submit
```

`--state-dir ABSOLUTE_PATH` selects an explicit shared queue. The adapter does not start a worker. Its protocol messages use stdin/stdout, so launch it as a client-managed stdio server rather than expecting an interactive terminal UI.

| Tool | Arguments | Purpose |
| --- | --- | --- |
| `gpu_queue_status` | None | Read queue state. |
| `gpu_queue_report` | `job_id` integer | Read one job's details. |
| `gpu_queue_submit` | `project`, `vram_gb`, `minutes`, `cwd`, `command` string array; optional `max_minutes` | Submit once; requires `--allow-submit`. |
| `gpu_queue_reorder` | `job_ids` array; optional `expected_order` | Reorder every waiting ID once; requires `--allow-submit`. |
| `gpu_queue_pause` | `job_id` integer | Hold queued work or request checkpoint pause; requires `--allow-submit`. |
| `gpu_queue_resume` | `job_id` integer | Queue a linked checkpoint resume or release a held job; requires `--allow-submit`. |
| `gpu_queue_cancel` | `job_id` integer | Request cancellation; requires `--allow-submit`. |

The adapter implements the MCP `2025-11-25` initialization and tools workflow over stdio. Compatibility depends on client protocol support; client-specific configuration syntax can change. In your client's MCP server settings, configure these values:

```json
{
  "command": "/absolute/path/to/gpu-queue-mcp",
  "args": [],
  "env": {
    "GPU_QUEUE_STATE_DIR": "/absolute/path/to/shared-state"
  }
}
```

This is a server-entry template, not a complete configuration file for any particular client. On Windows, use an absolute `.exe` path and escaped backslashes in JSON. Consult your client's current MCP configuration documentation for the enclosing structure. Do not add `--allow-submit` unless the client is meant to execute local jobs. Submission executes arbitrary local commands with the worker account's permissions; it is not sandboxed.

## Sample structured job

```json
{
  "project": "demo-training",
  "vram_gb": 8,
  "minutes": 20,
  "max_minutes": 45,
  "cwd": "/home/you/projects/demo",
  "command": ["/home/you/projects/demo/.venv/bin/python", "train.py", "--epochs", "3"]
}
```

Replace the paths and estimates for your workload. Give each array element one argument; do not combine a shell command into one string. Save the returned job ID and inspect its report before deciding whether another submission is needed.

## Suggested agent task

> Submit the training script through our shared GPU queue using this project's Python environment. Estimate VRAM and runtime from the configuration, record the job ID, and report its status. If it waits, explain why without starting another copy. When it finishes, inspect the report and log before declaring success.

See [operating notes](operations.md) for environment inheritance and process cleanup limits.


Before submitting, follow the [required checkpoint contract](pause-resume.md) and
[reporting/review contract](review-runs.md). The project snippets include both requirements.
