# 事件仓 Event Store —— 给 AI 装上"记事本"

**解决 AI 最经典的痛点：昨天说的事，今天全忘了。**

事件仓（Event Store）是一个**为零依赖、单文件**的事件/任务跟踪系统，专门给 AI 助手用（不是给人用）。把 AI 该记的事按完整生命周期管理，跨会话、跨工具、跨设备都不丢：

```
进仓(intake) → 进行中(in_progress) → 等外部(waiting) → 已完成(done) → 归档(closed)
```

## 为什么需要它

AI 干活很强，记性很差。会话一关，上下文跟着蒸发。事件仓 = AI 所有待办的**单一事实源**：

- 需要跟进的事（"等客户回复"）
- 不能忘的事（"周一提醒我"）
- 跨会话恢复（"我上次干到哪了？"——一条命令找回上下文）

## 核心能力

- **生命周期状态机** —— 状态流转有 guard 约束，非法跳转直接拒绝（如 done 不能直接变 waiting）
- **审计溯源** —— 每次进仓/改状态/加标签全部留痕，`show <id>` 可查完整变更链
- **滞销/逾期巡检** —— 内置扫描：进行中/等外部超过 3 天没动静 = 滞销提醒；设置了提醒时间的逾期 = 红色警报；带"待查"标签超过 7 天 = 积压预警
- **跨会话记忆** —— 新开会话一条 `list --status in_progress` 立刻恢复上下文
- **中文友好搜索** —— 多关键词 AND 模糊检索（LIKE 实现，对中文友好）
- **零依赖** —— 纯 Python 标准库（sqlite3），单文件，任何能跑 Python 的地方都能跑
- **好备份** —— 就是一个 SQLite 文件，拷走即是备份

## 快速开始

```bash
python scripts/event_store.py init

# 进仓一个事件
python scripts/event_store.py in --title "跟进客户 X 的需求确认" --tag 待查 --reminder 2026-08-30T09:00

# 更新进度
python scripts/event_store.py update EVT260826-001 --progress "方案已发，等回复" --status waiting

# 巡检有没有忘事
python scripts/event_store.py overdue --notify

# 看看现在都在干嘛
python scripts/event_store.py list --status in_progress
```

数据库默认存 `~/.eventstore/eventstore.db`，可用环境变量 `EVENTSTORE_DB` 覆盖（方便测试/CI）。

## 命令速查

| 命令 | 用途 |
|---|---|
| `init` | 初始化数据库 |
| `in` | 进仓新事件 |
| `update <id>` | 记进度 + 可选改状态 |
| `status <id> <新状态>` | 状态机 guard 约束的流转 |
| `done <id> --conclusion "..."` | 完成并写结论 |
| `close <id>` | 归档 |
| `reopen <id>` | 重新打开归档事件 |
| `tag <id> --add 待查` | 加/减标签（待查=需跟进） |
| `list` | 按状态/标签/日期过滤 |
| `search "关键词"` | 多关键词 AND 搜索 |
| `show <id>` | 详情 + 审计链 |
| `overdue` | 巡检滞销/逾期/积压 |
| `export` | 导出 JSON |

所有列表命令支持 `--json` 机器可读输出。

## 测试

```bash
EVENTSTORE_DB=/tmp/evt_test.db python tests/run_tests.py
```

## 许可证

MIT
