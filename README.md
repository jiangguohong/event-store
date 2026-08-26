# Event Store for AI Agents

**Give your AI agent a memory of what it's supposed to be doing — across sessions, across devices, across tool calls.**

Event Store is a tiny, zero-dependency, single-file event/task tracker designed for **AI agents** (not humans). It solves the classic "AI forgot what it was doing yesterday" problem by persisting every task through a full lifecycle:

```
intake → in_progress → waiting → done → closed
```

## Why it exists

AI agents are great at doing tasks, terrible at remembering them. When a session ends, the context dies with it. Event Store is the **single source of truth** for everything an agent is tracking:

- Tasks that need follow-up ("waiting for the user to reply")
- Things that must not be dropped ("remind me on Monday")
- Cross-session recovery ("what was I working on?")

## Features

- **Lifecycle state machine** — legal transitions enforced by a state-machine guard (no illegal jumps)
- **Audit trail** — every create/update/status change is logged, fully traceable
- **Stale & overdue scans** — built-in sweeper finds forgotten tasks (stale >3d, overdue reminders, backlog tags)
- **Cross-session memory** — new session = `list --status in_progress`, you're instantly back in context
- **Chinese-first search** — multi-keyword AND fuzzy search (LIKE-based, friendly to CJK)
- **Zero dependencies** — pure Python stdlib (`sqlite3`), single file, runs anywhere Python runs
- **Backup-friendly** — SQLite file, copy it anywhere

## Quick start

```bash
python scripts/event_store.py init

# add an event
python scripts/event_store.py in --title "Follow up with user on X" --tag 待查 --reminder 2026-08-30T09:00

# update progress
python scripts/event_store.py update EVT260826-001 --progress "draft sent, waiting reply" --status waiting

# sweep for forgotten tasks
python scripts/event_store.py overdue --notify

# list what's active
python scripts/event_store.py list --status in_progress
```

Database defaults to `~/.eventstore/eventstore.db`; override with `EVENTSTORE_DB` env var (great for tests/CI).

## Commands

| Command | Purpose |
|---|---|
| `init` | create the database |
| `in` | intake a new event |
| `update <id>` | progress note + optional status change |
| `status <id> <new>` | state-machine-guarded transition |
| `done <id> --conclusion "..."` | mark done with conclusion |
| `close <id>` | archive with conclusion |
| `reopen <id>` | reopen an archived event |
| `tag <id> --add 待查` | add/remove tags (待查 = needs-follow-up) |
| `list` | filter by status/tag/date |
| `search "<kw>"` | multi-keyword AND search |
| `show <id>` | full detail + audit trail |
| `overdue` | sweep for stale/overdue/backlog |
| `export` | dump to JSON |

All list commands support `--json` for machine-readable output.

## Tests

```bash
EVENTSTORE_DB=/tmp/evt_test.db python tests/run_tests.py
```

## License

MIT
