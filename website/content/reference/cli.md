---
title: CLI
weight: 2
---

Optional binary (`--features cli`). Files and stdin become opaque batches—
useful for black-box tests, not a Kafka client.

```bash
cargo install object-log --features cli
object-log --help
```

## Commands

| Command | Purpose |
|---------|---------|
| `produce` | Append batches from files / stdin |
| `consume` | Read batches to stdout or `--out-dir` |
| `roundtrip` | One-shot produce+consume (`--memory` for in-process) |
| `list` | List object keys under a prefix |
| `inspect` | Print ManifestSequencer index (optional `--json`) |
| `orphans` | Dry-run or `--delete` unreferenced data objects |
| `fetch` | Low-level fetch with hex/text preview |

## Framing modes

| Mode | Produce | Consume |
|------|---------|---------|
| `file` | each path = one batch | — |
| `lines` | newline-split | payload + `\n` |
| `nul` | NUL-split | payload + `NUL` |
| `framed` | u64 BE length + bytes | same |
| `raw` | — | concatenate payloads |

## Store backends

- Local: `--root DIR`
- S3-compatible: build with `cli,s3` and `--s3-endpoint` / `--s3-bucket` /
  `OBJECT_LOG_S3_KEY_ID` / `OBJECT_LOG_S3_SECRET`

## Examples

```bash
printf 'a\nb\nc\n' | object-log produce --root /tmp/olog --partition demo --lines
object-log consume --root /tmp/olog --partition demo --lines

object-log inspect --root /tmp/olog --summary
object-log list --root /tmp/olog --prefix log/
```
