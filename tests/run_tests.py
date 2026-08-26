#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""run_tests.py -- minimal smoke tests for Event Store (release sanity check).
Usage: EVENTSTORE_DB=<tmp> python tests/run_tests.py
"""
import os
import sys
import tempfile
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "event_store.py"
PY = sys.executable

DB = os.environ.get(
    "EVENTSTORE_DB",
    os.path.join(tempfile.gettempdir(), "evt_test_%s.db" % os.getpid()),
)


def run(*args):
    env = dict(os.environ, EVENTSTORE_DB=DB)
    r = subprocess.run([PY, str(SCRIPT), *args], capture_output=True, text=True, env=env, encoding="utf-8")
    return r.returncode, (r.stdout or "") + (r.stderr or "")


def main():
    fails = []
    rc, out = run("init")
    if rc != 0:
        fails.append("init failed: %s" % out[:200])
    rc, out = run("in", "--title", "smoke event", "--tag", "待查")
    if rc != 0 or "EVT" not in out:
        fails.append("in failed: %s" % out[:200])
    rc, out = run("list", "--status", "intake")
    if rc != 0 or "smoke event" not in out:
        fails.append("list failed: %s" % out[:200])
    rc, out = run("status", "EVT260826-001", "in_progress")
    if rc != 0 or "in_progress" not in out:
        fails.append("status transition failed: %s" % out[:200])
    # illegal transition done -> waiting must be refused
    rc, out = run("status", "EVT260826-001", "done")
    rc2, out2 = run("status", "EVT260826-001", "waiting")
    if "拒绝" not in out2 and "BLOCKED" not in out2 and rc2 == 0:
        fails.append("guard should refuse done->waiting: %s" % out2[:200])
    rc, out = run("search", "smoke")
    if rc != 0 or "smoke event" not in out:
        fails.append("search failed: %s" % out[:200])
    rc, out = run("export")
    if rc != 0:
        fails.append("export failed: %s" % out[:200])
    try:
        os.remove(DB)
    except OSError:
        pass
    print("PASS" if not fails else "FAIL=%d" % len(fails))
    for f in fails:
        print("  -", f)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
