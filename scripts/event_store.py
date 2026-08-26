#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞影事件仓 (Event Store) 引擎  v1.3
====================================
单一事实源: 默认 ~/.eventstore/eventstore.db
            (可用环境变量 EVENTSTORE_DB 覆盖, 便于隔离测试)
生命周期:  intake(进仓) -> in_progress(进度) -> waiting(等外部) -> done(完成) -> closed(结论归档)
配套:      待查标签(tags 含 '待查') + 时间提醒(reminder_at) + 滞销/逾期/临近扫描
增强(v1.1): 状态机流转 guard + audit_log 审计溯源 + reopen/audit 命令 + 临近提醒(提前1天) + 待查分级
增强(v1.3): 登录启动钩子(FeiYing_EventStore_LoginCheck/LogonTrigger) + UTF-8编码修复 + 前序v1.2(巡检心跳last_check/漏跑补报/自动化多时间点)
检索:      多词 AND 模糊检索(LIKE,中文友好) + 按 status/tag/date 精确过滤

用法:
  event_store.py init
  event_store.py in --title "..." [--tag 待查] [--reminder 2026-08-20T09:00] [--owner feiying] [--dep EVT...] [--next "..."]
  event_store.py update <id> --progress "..." [--status in_progress] [--force]
  event_store.py status <id> <new_status> [--force]
  event_store.py done <id> --conclusion "..." [--force]
  event_store.py close <id> [--conclusion "..."] [--force]
  event_store.py reopen <id> [--to in_progress] [--force]
  event_store.py audit <id>                 # 查看某事件审计溯源记录
  event_store.py tag <id> --add 待查 | --remove 待查
  event_store.py list [--status in_progress] [--tag 待查] [--stale 3]
  event_store.py search "<关键词>"
  event_store.py show <id>                   # 含审计记录
  event_store.py overdue [--stale-days 3] [--pending-days 7] [--notify]
  event_store.py export [--output path.json]
所有列表类命令支持 --json 输出机器可读。
"""

import sqlite3, json, os, sys, shutil, argparse, glob
from datetime import datetime, timedelta, timezone

DB = os.environ.get("EVENTSTORE_DB", os.path.expanduser("~/.eventstore/eventstore.db"))
TZ = timezone(timedelta(hours=8))  # 北京时间

VALID_STATUS = {"intake", "in_progress", "waiting", "done", "closed"}
STATUS_CN = {
    "intake": "进仓/待启动",
    "in_progress": "进度中",
    "waiting": "等外部",
    "done": "已完成",
    "closed": "已归档(结论)",
}
# 合法状态流转 (状态机 guard 依据). from -> 允许到达的集合
TRANSITIONS = {
    "intake":      {"in_progress", "waiting", "closed"},
    "in_progress": {"waiting", "done", "closed"},
    "waiting":     {"in_progress", "done", "closed"},
    "done":        {"closed", "in_progress"},   # done 可 reopen 回 in_progress
    "closed":      {"in_progress"},              # closed 仅可显式 reopen
}
STALE_DEFAULT = 3      # 进度中/等外部 超过 N 天无更新 = 滞销
PENDING_DEFAULT = 7    # 待查标签 超过 N 天未处理 = 积压
PENDING_SEVERE = 14    # 待查超过 N 天 = 严重积压
UPCOMING_DAYS = 1      # reminder_at 在 N 天内将到期 = 临近提醒
CATCHUP_THRESHOLD_H = 30  # 距上次成功巡检超过 N 小时视为漏跑(当天WD未启动), 触发补报


def now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")


def parse_dt(s):
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # 兼容 2026-08-20 09:00 这种无 T 写法
        dt = datetime.fromisoformat(s.replace(" ", "T"))
    # 强制时区: 无时区的时间戳一律按北京时间解释, 避免 aware-naive 相减抛 TypeError
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt


def conn():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    # busy_timeout: WD/DSH 同用户同路径并发写时避免 SQLITE_BUSY 直接报错
    c = sqlite3.connect(DB, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def init_db(c):
    c.execute(
        """CREATE TABLE IF NOT EXISTS events(
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'intake',
            progress TEXT DEFAULT '',
            owner TEXT DEFAULT 'feiying',
            tags TEXT DEFAULT '[]',
            deps TEXT DEFAULT '[]',
            next_step TEXT DEFAULT '',
            reminder_at TEXT,
            conclusion TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            evidence TEXT DEFAULT '[]'
        )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS audit_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            action TEXT NOT NULL,
            from_status TEXT,
            to_status TEXT,
            note TEXT DEFAULT ''
        )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY,
            value TEXT
        )"""
    )
    c.commit()


def backup():
    """写操作后调用: 滚动本地备份 + 异地副本(D盘, 与河图同容灾目录), 防单文件损坏/误删。"""
    try:
        if not os.path.exists(DB):
            return
        # 1) 最近1份覆盖式快照
        shutil.copy(DB, DB + ".bak")
        # 2) 滚动时间戳备份 (保留近 14 份)
        daily_dir = os.path.abspath(os.path.join(os.path.dirname(DB), "..", "backups", "daily"))
        os.makedirs(daily_dir, exist_ok=True)
        ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        shutil.copy(DB, os.path.join(daily_dir, f"eventstore_{ts}.db"))
        olds = sorted(glob.glob(os.path.join(daily_dir, "eventstore_*.db")))
        for old in olds[:-14]:
            try:
                os.remove(old)
            except Exception:
                pass
        # 3) 异地副本 (与河图 hetu_v1.db 同容灾目录 D:/hetu_backup)
        dst_dir = r"D:/hetu_backup"
        os.makedirs(dst_dir, exist_ok=True)
        shutil.copy(DB, os.path.join(dst_dir, "eventstore.db"))
    except Exception as e:
        print(f"[warn] backup failed: {e}", file=sys.stderr)


def write_audit(c, event_id, action, from_status=None, to_status=None, note=""):
    c.execute(
        "INSERT INTO audit_log(event_id,ts,action,from_status,to_status,note) VALUES(?,?,?,?,?,?)",
        (event_id, now_iso(), action, from_status, to_status, note or ""),
    )


def get_meta(c, key, default=None):
    r = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def set_meta(c, key, value):
    c.execute(
        "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=?",
        (key, value, value),
    )


def check_transition(from_s, to_s, force=False):
    """状态机 guard: 返回是否允许 from_s -> to_s。force 可绕开(记录审计)。"""
    if from_s == to_s:
        return True
    allowed = TRANSITIONS.get(from_s, set())
    if to_s in allowed:
        return True
    if force:
        return True
    return False


def gen_id(c):
    d = datetime.now(TZ).strftime("%y%m%d")
    like = f"EVT{d}%"
    row = c.execute("SELECT COUNT(*) AS n FROM events WHERE id LIKE ?", (like,)).fetchone()
    n = row["n"] + 1
    return f"EVT{d}-{n:03d}"


def jload(s):
    try:
        v = json.loads(s)
        return v if isinstance(v, list) else []
    except Exception:
        return []


def jdump(v):
    return json.dumps(v, ensure_ascii=False)


def cmd_init(args):
    c = conn()
    init_db(c)
    c.close()
    print(f"事件仓已初始化: {DB}")


def cmd_in(args):
    c = conn()
    init_db(c)
    if not args.title:
        print("错误: --title 必填", file=sys.stderr)
        sys.exit(2)
    eid = gen_id(c)
    tags = list(args.tag) if args.tag else []
    deps = list(args.dep) if args.dep else []
    rt = args.reminder
    if rt:
        parse_dt(rt)  # 校验格式
    t = now_iso()
    ct = parse_dt(args.created).isoformat() if args.created else t
    ut = parse_dt(args.updated).isoformat() if args.updated else t
    init_status = args.status if args.status else "intake"
    if init_status not in VALID_STATUS:
        print(f"错误: 非法状态 {init_status}", file=sys.stderr)
        sys.exit(2)
    ev = jdump([x.strip() for x in (args.evidence or "").split(",") if x.strip()])
    c.execute(
        """INSERT INTO events(id,title,status,progress,owner,tags,deps,next_step,reminder_at,conclusion,created_at,updated_at,evidence)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (eid, args.title, init_status, args.progress or "", args.owner, jdump(tags), jdump(deps),
         args.next or "", rt, args.conclusion or "", ct, ut, ev),
    )
    write_audit(c, eid, "created", None, init_status, f"进仓 status={init_status}")
    c.commit()
    backup()
    c.close()
    print(f"进仓成功: {eid} | {args.title}")
    print(f"  状态={init_status}({STATUS_CN.get(init_status,'')})  标签={tags}  提醒={rt or '无'}")


def cmd_update(args):
    c = conn()
    init_db(c)
    row = c.execute("SELECT * FROM events WHERE id=?", (args.id,)).fetchone()
    if not row:
        print(f"错误: 事件 {args.id} 不存在", file=sys.stderr)
        sys.exit(2)
    from_status = row["status"]
    target_status = args.status or from_status
    changed = []
    if args.status and target_status != from_status:
        if target_status not in VALID_STATUS:
            print(f"错误: 非法状态 {target_status}", file=sys.stderr)
            sys.exit(2)
        if not check_transition(from_status, target_status, args.force):
            legal = sorted(TRANSITIONS.get(from_status, set()))
            print(f"错误: 非法状态流转 {from_status} -> {target_status} (从 {from_status} 合法去向: {legal}); 加 --force 可强制", file=sys.stderr)
            sys.exit(2)
        c.execute("UPDATE events SET status=?, updated_at=? WHERE id=?", (target_status, now_iso(), args.id))
        write_audit(c, args.id, "status_change", from_status, target_status, "update 命令")
        changed.append(f"状态={target_status}({STATUS_CN.get(target_status,'')})")
    if args.progress is not None:
        c.execute("UPDATE events SET progress=?, updated_at=? WHERE id=?", (args.progress, now_iso(), args.id))
        write_audit(c, args.id, "progress_update", from_status, target_status, (args.progress or "")[:200])
        changed.append("进度已更新")
    if not changed:
        # 无实质变更也刷新 updated_at, 便于巡检判断活跃度
        c.execute("UPDATE events SET updated_at=? WHERE id=?", (now_iso(), args.id))
    c.commit()
    backup()
    c.close()
    print(f"已更新: {args.id} | " + ("; ".join(changed) or "无变化(仅刷新时间)"))
    if args.progress:
        print(f"  进度: {args.progress}")


def cmd_status(args):
    c = conn()
    init_db(c)
    row = c.execute("SELECT * FROM events WHERE id=?", (args.id,)).fetchone()
    if not row:
        print(f"错误: 事件 {args.id} 不存在", file=sys.stderr)
        sys.exit(2)
    from_status = row["status"]
    to_status = args.new_status
    if to_status not in VALID_STATUS:
        print(f"错误: 非法状态 {to_status} (可选: {sorted(VALID_STATUS)})", file=sys.stderr)
        sys.exit(2)
    if to_status != from_status and not check_transition(from_status, to_status, args.force):
        legal = sorted(TRANSITIONS.get(from_status, set()))
        print(f"错误: 非法状态流转 {from_status} -> {to_status} (合法去向: {legal}); 加 --force 可强制", file=sys.stderr)
        sys.exit(2)
    c.execute("UPDATE events SET status=?, updated_at=? WHERE id=?", (to_status, now_iso(), args.id))
    write_audit(c, args.id, "status_change", from_status, to_status, "status 命令")
    c.commit()
    backup()
    c.close()
    print(f"状态变更: {args.id} -> {to_status}({STATUS_CN.get(to_status,'')})")


def _guard_done_close(c, args, target, verb):
    """done/close 的流转 guard: 当前状态必须能到达 target, 否则 --force。"""
    row = c.execute("SELECT * FROM events WHERE id=?", (args.id,)).fetchone()
    if not row:
        print(f"错误: 事件 {args.id} 不存在", file=sys.stderr)
        sys.exit(2)
    from_status = row["status"]
    if from_status != target and target not in TRANSITIONS.get(from_status, set()) and not args.force:
        legal = sorted(TRANSITIONS.get(from_status, set()))
        print(f"错误: 当前 {from_status} 不能直接 {verb} (合法去向: {legal}); 先转 in_progress 或加 --force", file=sys.stderr)
        sys.exit(2)
    return row, from_status


def cmd_done(args):
    c = conn()
    init_db(c)
    row, from_status = _guard_done_close(c, args, "done", "done")
    concl = args.conclusion or row["conclusion"]
    c.execute("UPDATE events SET status='done', conclusion=?, updated_at=? WHERE id=?", (concl, now_iso(), args.id))
    write_audit(c, args.id, "done", from_status, "done", "标记完成")
    c.commit()
    backup()
    c.close()
    print(f"完成: {args.id} | 结论: {concl or '(未填)'}")


def cmd_close(args):
    c = conn()
    init_db(c)
    row, from_status = _guard_done_close(c, args, "closed", "close")
    concl = args.conclusion or row["conclusion"]
    c.execute("UPDATE events SET status='closed', conclusion=?, updated_at=? WHERE id=?", (concl, now_iso(), args.id))
    write_audit(c, args.id, "closed", from_status, "closed", "归档")
    c.commit()
    backup()
    c.close()
    print(f"归档: {args.id} | 结论: {concl or '(未填)'}")


def cmd_reopen(args):
    c = conn()
    init_db(c)
    row = c.execute("SELECT * FROM events WHERE id=?", (args.id,)).fetchone()
    if not row:
        print(f"错误: 事件 {args.id} 不存在", file=sys.stderr)
        sys.exit(2)
    from_status = row["status"]
    to_status = args.to
    if to_status not in VALID_STATUS:
        print(f"错误: 非法状态 {to_status}", file=sys.stderr)
        sys.exit(2)
    if not check_transition(from_status, to_status, args.force):
        legal = sorted(TRANSITIONS.get(from_status, set()))
        print(f"错误: 非法流转 {from_status} -> {to_status} (合法: {legal}); 加 --force 可强制", file=sys.stderr)
        sys.exit(2)
    c.execute("UPDATE events SET status=?, updated_at=? WHERE id=?", (to_status, now_iso(), args.id))
    write_audit(c, args.id, "reopened", from_status, to_status, f"重新打开 -> {to_status}")
    c.commit()
    backup()
    c.close()
    print(f"重新打开: {args.id} -> {to_status}({STATUS_CN.get(to_status,'')})")


def cmd_audit(args):
    c = conn()
    init_db(c)
    rows = c.execute("SELECT * FROM audit_log WHERE event_id=? ORDER BY id", (args.id,)).fetchall()
    c.close()
    if not rows:
        print(f"(事件 {args.id} 无审计记录)")
        return
    print(f"事件 {args.id} 审计溯源 ({len(rows)} 条):")
    for r in rows:
        print(f"  [{r['ts']}] {r['action']:<14} {r['from_status'] or '-'} -> {r['to_status'] or '-'}  {r['note']}")


def cmd_tag(args):
    c = conn()
    init_db(c)
    row = c.execute("SELECT * FROM events WHERE id=?", (args.id,)).fetchone()
    if not row:
        print(f"错误: 事件 {args.id} 不存在", file=sys.stderr)
        sys.exit(2)
    tags = jload(row["tags"])
    changed = False
    if args.add:
        for t in args.add:
            if t not in tags:
                tags.append(t)
                changed = True
    if args.remove:
        for t in args.remove:
            if t in tags:
                tags.remove(t)
                changed = True
    if changed:
        c.execute("UPDATE events SET tags=?, updated_at=? WHERE id=?", (jdump(tags), now_iso(), args.id))
        write_audit(c, args.id, "tag_change", None, None, f"tags={tags}")
        c.commit()
        backup()
    c.close()
    print(f"标签更新: {args.id} -> {tags}")


def fmt_rows(rows, as_json):
    out = []
    for r in rows:
        d = dict(r)
        d["tags"] = jload(d.get("tags", "[]"))
        d["deps"] = jload(d.get("deps", "[]"))
        d["evidence"] = jload(d.get("evidence", "[]"))
        out.append(d)
    if as_json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        if not out:
            print("(无事件)")
            return
        print(f"{'ID':<14} {'状态':<10} {'标题':<30} {'更新于':<20} 标签")
        print("-" * 100)
        for d in out:
            print(f"{d['id']:<14} {STATUS_CN.get(d['status'],d['status']):<10} {d['title'][:28]:<30} {str(d['updated_at']):<20} {','.join(d['tags'])}")


def cmd_list(args):
    c = conn()
    init_db(c)
    sql = "SELECT * FROM events WHERE 1=1"
    params = []
    if args.status:
        sql += " AND status=?"
        params.append(args.status)
    if args.tag:
        # tags 是 JSON 数组文本, 用 LIKE 粗匹配
        sql += " AND tags LIKE ?"
        params.append(f'%{args.tag}%')
    if args.stale:
        cutoff = (datetime.now(TZ) - timedelta(days=args.stale)).isoformat(timespec="seconds")
        sql += " AND updated_at < ? AND status IN ('in_progress','waiting')"
        params.append(cutoff)
    sql += " ORDER BY updated_at DESC"
    rows = c.execute(sql, params).fetchall()
    c.close()
    fmt_rows(rows, args.json)


def cmd_search(args):
    c = conn()
    init_db(c)
    # 多词 AND 检索 (LIKE), 中文友好; 匹配 title/progress/conclusion/tags
    terms = args.query.split()
    sql = "SELECT * FROM events WHERE 1=1"
    params = []
    for t in terms:
        sql += " AND (title LIKE ? OR progress LIKE ? OR conclusion LIKE ? OR tags LIKE ?)"
        params += [f"%{t}%"] * 4
    sql += " ORDER BY updated_at DESC"
    rows = c.execute(sql, params).fetchall()
    c.close()
    fmt_rows(rows, args.json)


def cmd_show(args):
    c = conn()
    init_db(c)
    row = c.execute("SELECT * FROM events WHERE id=?", (args.id,)).fetchone()
    if not row:
        c.close()
        print(f"错误: 事件 {args.id} 不存在", file=sys.stderr)
        sys.exit(2)
    d = dict(row)
    print(f"事件: {d['id']}")
    print(f"标题: {d['title']}")
    print(f"状态: {d['status']} ({STATUS_CN.get(d['status'],'')})")
    print(f"负责: {d['owner']}")
    print(f"标签: {jload(d['tags'])}")
    print(f"依赖: {jload(d['deps'])}")
    print(f"下一步: {d['next_step']}")
    print(f"提醒: {d['reminder_at'] or '无'}")
    print(f"进度: {d['progress'] or '(空)'}")
    print(f"结论: {d['conclusion'] or '(空)'}")
    print(f"证据: {jload(d['evidence'])}")
    print(f"创建: {d['created_at']}  更新: {d['updated_at']}")
    print("审计记录:")
    aud = c.execute("SELECT * FROM audit_log WHERE event_id=? ORDER BY id", (d['id'],)).fetchall()
    if not aud:
        print("  (无)")
    else:
        for r in aud:
            print(f"  [{r['ts']}] {r['action']:<14} {r['from_status'] or '-'} -> {r['to_status'] or '-'}  {r['note']}")
    c.close()


def cmd_overdue(args):
    c = conn()
    init_db(c)
    now = datetime.now(TZ)
    now_iso_s = now.isoformat(timespec="seconds")

    # 巡检心跳: 读上次成功巡检时间, 判定是否漏跑(补报)
    # 解决 P0④: 当日 WD 未启动则 automation 静默丢失, 下次开机需能感知并补报
    catchup = False
    gap_h = None
    last_s = get_meta(c, "last_check")
    if last_s:
        last_ts = parse_dt(last_s)
        if last_ts:
            gap_h = (now - last_ts).total_seconds() / 3600.0
            if gap_h > CATCHUP_THRESHOLD_H:
                catchup = True

    stale_cut = (now - timedelta(days=args.stale_days)).isoformat(timespec="seconds")
    pending_cut = (now - timedelta(days=args.pending_days)).isoformat(timespec="seconds")
    soon_cut = (now + timedelta(days=UPCOMING_DAYS)).isoformat(timespec="seconds")

    # 1) 滞销: 进度中/等外部 超过 stale_days 无更新
    stale = c.execute(
        "SELECT * FROM events WHERE status IN ('in_progress','waiting') AND updated_at < ? ORDER BY updated_at ASC",
        (stale_cut,),
    ).fetchall()
    # 2) 逾期: reminder_at 已过且未完成
    overdue = c.execute(
        "SELECT * FROM events WHERE reminder_at IS NOT NULL AND reminder_at < ? AND status NOT IN ('done','closed') ORDER BY reminder_at ASC",
        (now_iso_s,),
    ).fetchall()
    # 3) 待查积压: 含 '待查' 标签且未完成且超过 pending_days 未动
    pending = c.execute(
        "SELECT * FROM events WHERE tags LIKE ? AND status NOT IN ('done','closed') AND updated_at < ? ORDER BY updated_at ASC",
        (f"%待查%", pending_cut),
    ).fetchall()
    # 4) 临近: reminder_at 在 [now, now+UPCOMING_DAYS] 内且未完成 (提前预警)
    upcoming = c.execute(
        "SELECT * FROM events WHERE reminder_at IS NOT NULL AND reminder_at >= ? AND reminder_at <= ? AND status NOT IN ('done','closed') ORDER BY reminder_at ASC",
        (now_iso_s, soon_cut),
    ).fetchall()

    def block(title, rows, kind):
        if not rows:
            return None
        lines = [f"### {title} ({len(rows)})"]
        for r in rows:
            if kind == "stale":
                days = (now - parse_dt(r["updated_at"])).days
                extra = f" | 滞销{days}天"
            elif kind == "overdue":
                days = (now - parse_dt(r["reminder_at"])).days
                extra = f" | 已逾期{days}天 (reminder {r['reminder_at']})"
            elif kind == "pending":
                days = (now - parse_dt(r["updated_at"])).days
                sev = " 🔴严重" if days > PENDING_SEVERE else ""
                extra = f" | 积压{days}天{sev}"
            elif kind == "upcoming":
                rdays = (parse_dt(r["reminder_at"]) - now).days
                extra = f" | 将于{rdays}天内到期 (reminder {r['reminder_at']})"
            else:
                extra = ""
            lines.append(f"  - [{r['id']}] {r['title']} (状态:{r['status']}{extra})")
            if r["next_step"]:
                lines.append(f"      下一步: {r['next_step']}")
        return "\n".join(lines)

    b1 = block(f"🟠 滞销事件 (>{args.stale_days}天无更新)", stale, "stale")
    b2 = block("🔴 逾期提醒 (reminder_at 已过)", overdue, "overdue")
    b3 = block(f"🟡 待查积压 (含'待查'标签 >{args.pending_days}天)", pending, "pending")
    b4 = block(f"🔵 临近提醒 (reminder_at 在{UPCOMING_DAYS}天内将到期)", upcoming, "upcoming")

    blocks = [x for x in (b1, b2, b3, b4) if x]

    # 写回巡检心跳 (无论有无异常都打点, 供下次漏跑判定)
    set_meta(c, "last_check", now_iso_s)
    c.commit()
    c.close()

    catchup_note = ""
    if catchup:
        gap_d = gap_h / 24.0
        catchup_note = (
            f"\n\n⚠️ **巡检补报**：距上次成功巡检已 {gap_h:.1f} 小时（≈{gap_d:.1f} 天），"
            f"期间疑似漏跑（某日 WorkBuddy 未启动 / 自动化未触发）。本次为补报扫描，请重点核查上述待办是否仍有效。"
        )

    if not blocks:
        msg = "✅ 事件仓巡检：无滞销 / 逾期 / 待查积压 / 临近，全部健康。"
        if catchup:
            msg += catchup_note
        print(msg)
        if args.notify:
            print("\n[notify] 无异常。" + ("（含补报提示）" if catchup else ""))
        return

    report = "## 📋 事件仓巡检提醒" + catchup_note + "\n\n" + "\n\n".join(blocks)
    report += f"\n\n_生成时间: {now_iso_s} (北京时间)_"
    print(report)
    if args.notify:
        # 把异常事件 id 清单也输出一份机器可读, 供上层自动化推送
        alert_ids = [r["id"] for r in (list(stale) + list(overdue) + list(pending) + list(upcoming))]
        print("\n[notify-ids] " + json.dumps(alert_ids, ensure_ascii=False))


def cmd_export(args):
    c = conn()
    init_db(c)
    rows = c.execute("SELECT * FROM events ORDER BY created_at").fetchall()
    aud = c.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
    c.close()
    data = {
        "events": [dict(r) for r in rows],
        "audit_log": [dict(r) for r in aud],
    }
    out = args.output or (DB + ".export.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"已导出 {len(rows)} 条事件 + {len(aud)} 条审计 -> {out}")


def build_parser():
    p = argparse.ArgumentParser(description="飞影事件仓引擎 v1.3")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init").set_defaults(func=cmd_init)

    pi = sub.add_parser("in", help="事件进仓")
    pi.add_argument("--title", required=True)
    pi.add_argument("--tag", action="append", help="可多次: --tag 待查")
    pi.add_argument("--status", default="intake", choices=sorted(VALID_STATUS), help="初始状态, 默认 intake")
    pi.add_argument("--progress", help="初始进度摘要")
    pi.add_argument("--conclusion", help="初始结论(用于直接进 done/closed)")
    pi.add_argument("--reminder", help="提醒时间 ISO, 如 2026-08-20T09:00")
    pi.add_argument("--created", help="回溯创建时间 ISO (迁移历史用)")
    pi.add_argument("--updated", help="回溯更新时间 ISO (迁移历史用)")
    pi.add_argument("--evidence", help="证据链接/ID, 逗号分隔 (如 url1,id2)")
    pi.add_argument("--owner", default="feiying")
    pi.add_argument("--dep", action="append", help="依赖事件ID, 可多次")
    pi.add_argument("--next", help="下一步动作")
    pi.set_defaults(func=cmd_in)

    pu = sub.add_parser("update", help="更新进度/状态")
    pu.add_argument("id")
    pu.add_argument("--progress")
    pu.add_argument("--status")
    pu.add_argument("--force", action="store_true", help="强制绕过状态机 guard (仍记录审计)")
    pu.set_defaults(func=cmd_update)

    ps = sub.add_parser("status", help="改状态")
    ps.add_argument("id")
    ps.add_argument("new_status", choices=sorted(VALID_STATUS))
    ps.add_argument("--force", action="store_true", help="强制绕过状态机 guard")
    ps.set_defaults(func=cmd_status)

    pd = sub.add_parser("done", help="标记完成+结论")
    pd.add_argument("id")
    pd.add_argument("--conclusion")
    pd.add_argument("--force", action="store_true")
    pd.set_defaults(func=cmd_done)

    pc = sub.add_parser("close", help="归档+结论")
    pc.add_argument("id")
    pc.add_argument("--conclusion")
    pc.add_argument("--force", action="store_true")
    pc.set_defaults(func=cmd_close)

    pr = sub.add_parser("reopen", help="重新打开(closed/in_progress 互转)")
    pr.add_argument("id")
    pr.add_argument("--to", default="in_progress", choices=sorted(VALID_STATUS))
    pr.add_argument("--force", action="store_true")
    pr.set_defaults(func=cmd_reopen)

    pa = sub.add_parser("audit", help="查看某事件审计溯源记录")
    pa.add_argument("id")
    pa.set_defaults(func=cmd_audit)

    pt = sub.add_parser("tag", help="标签增删")
    pt.add_argument("id")
    pt.add_argument("--add", action="append")
    pt.add_argument("--remove", action="append")
    pt.set_defaults(func=cmd_tag)

    pl = sub.add_parser("list", help="列表")
    pl.add_argument("--status")
    pl.add_argument("--tag")
    pl.add_argument("--stale", type=int, help="仅显示超N天无更新的进行中事件")
    pl.add_argument("--json", action="store_true")
    pl.set_defaults(func=cmd_list)

    pse = sub.add_parser("search", help="全文检索")
    pse.add_argument("query")
    pse.add_argument("--json", action="store_true")
    pse.set_defaults(func=cmd_search)

    psh = sub.add_parser("show", help="详情(含审计)")
    psh.add_argument("id")
    psh.set_defaults(func=cmd_show)

    po = sub.add_parser("overdue", help="滞销/逾期/待查/临近扫描")
    po.add_argument("--stale-days", type=int, default=STALE_DEFAULT)
    po.add_argument("--pending-days", type=int, default=PENDING_DEFAULT)
    po.add_argument("--notify", action="store_true", help="附带可推送的异常id清单")
    po.set_defaults(func=cmd_overdue)

    pe = sub.add_parser("export", help="导出全部事件+审计为JSON(便于迁移/溯源)")
    pe.add_argument("--output", help="输出路径, 默认 eventstore.db.export.json")
    pe.set_defaults(func=cmd_export)

    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    args.func(args)
