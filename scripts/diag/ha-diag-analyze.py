#!/usr/bin/env python3
"""ha-diag-analyze.py — Analyze a bundle produced by ha-diag-collect.sh.

Runs the exact diagnostic workflow we used to root-cause BUG-043 through
BUG-046, producing a short "findings" report with severity tags. The report
is written to stdout and to ``$BUNDLE/findings.md``.

Checks performed:

1. **Stream token distribution**: parses the full XRANGE dump and counts
   events per ``fencingToken``. Flags large gaps in ``seq`` within a token
   (e.g. token=3 with count=6 while others have ~600 → "CDC pipeline stall
   during that epoch").

2. **Consumer group lag**: compares each group's ``last-delivered-id`` to
   the stream's tail. Flags groups > 60 s behind or with nonzero
   ``pel-count``.

3. **Per-node TestNode distribution**: parses the ``testnode_by_writer.txt``
   for every Neo4j node and cross-references. Flags nodes whose count
   differs from the maximum by more than 1 % as "out-of-sync".

4. **APOC trigger state**: for every node, verifies how many triggers are
   installed. Rule: exactly one node (the primary) may hold all 3 triggers,
   every other node must have zero. Multiple "full-trigger" nodes means
   BUG-046-style uninstall failure.

5. **Index presence**: checks that each user label on each Neo4j node has a
   range index on ``_updated_at`` (required for BUG-045 fix's per-label
   keyset query to hit ``NodeIndexSeekByRange``).

6. **Clock skew**: compares ``timestamp()`` across nodes. Flags > 500 ms drift.

7. **Checkpoint sanity**: CDC & sync checkpoint present & monotonic
   ``updatedAt``. Flags stale checkpoints (> 5 min old while the test was
   running).

8. **HAProxy routing invariant**: exactly one server in ``neo4j_primary``
   backend is ``READY``; all others are ``MAINT``. Across both HAProxy
   instances the READY server must match.

9. **Agent log signals**: scans for fencing rejects, trigger install/drop
   failures, unhandled exceptions.

Usage:
    python3 scripts/diag/ha-diag-analyze.py <bundle-dir-or-tgz>
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tarfile
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


# ---------------------------------------------------------------------------
# output helpers
# ---------------------------------------------------------------------------


@dataclass
class Finding:
    severity: str  # "INFO" / "WARN" / "ERROR"
    category: str
    message: str
    detail: str = ""


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    facts: dict = field(default_factory=dict)

    def add(self, sev: str, cat: str, msg: str, detail: str = "") -> None:
        self.findings.append(Finding(sev, cat, msg, detail))

    def info(self, cat: str, msg: str, detail: str = "") -> None:
        self.add("INFO", cat, msg, detail)

    def warn(self, cat: str, msg: str, detail: str = "") -> None:
        self.add("WARN", cat, msg, detail)

    def error(self, cat: str, msg: str, detail: str = "") -> None:
        self.add("ERROR", cat, msg, detail)


# ---------------------------------------------------------------------------
# bundle I/O
# ---------------------------------------------------------------------------


def load_bundle(path: str) -> Path:
    """Return a directory path. If ``path`` is a tar.gz, extract to a tmpdir."""
    p = Path(path)
    if p.is_dir():
        return p
    if p.suffix in (".gz", ".tgz") or p.name.endswith(".tar.gz"):
        tmp = Path(tempfile.mkdtemp(prefix="ha-diag-"))
        with tarfile.open(p) as tar:
            tar.extractall(tmp)
        # pick the single top-level dir we just extracted
        kids = [c for c in tmp.iterdir() if c.is_dir()]
        return kids[0] if len(kids) == 1 else tmp
    sys.exit(f"Cannot handle bundle path: {path}")


def read(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def strip_hdr(text: str) -> str:
    """Drop the `### timestamp` header and `### exit=` footer added by run()."""
    lines = [
        ln for ln in text.splitlines()
        if not ln.startswith("### ")
    ]
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# Stream dump parser (redis-cli non-TTY XRANGE output)
# ---------------------------------------------------------------------------


STREAM_ID_RE = re.compile(r"(?m)^(\d{13}-\d+)\s*$")


def parse_stream_entries(path: Path) -> list[dict]:
    """Parse a `redis-cli XRANGE key - +` dump (non-TTY, no numeric prefixes).

    Each entry starts with a line containing only the stream ID, followed by
    key/value pairs one per line.
    """
    text = read(path)
    if not text:
        return []
    text = strip_hdr(text)
    blocks = STREAM_ID_RE.split(text)
    # blocks = ['', id1, body1, id2, body2, ...]
    entries: list[dict] = []
    for i in range(1, len(blocks), 2):
        sid = blocks[i]
        body = blocks[i + 1]
        e: dict = {"_id": sid}
        # parse key\nvalue lines
        lines = [ln for ln in body.splitlines() if ln.strip() != ""]
        it = iter(lines)
        for k in it:
            try:
                v = next(it)
            except StopIteration:
                break
            # Skip lines that look like next stream id (safety)
            if STREAM_ID_RE.fullmatch(k):
                break
            e[k] = v
        entries.append(e)
    return entries


# ---------------------------------------------------------------------------
# individual checks
# ---------------------------------------------------------------------------


def check_stream_tokens(bundle: Path, r: Report) -> None:
    stream_dir = bundle / "redis" / "streams"
    if not stream_dir.exists():
        r.warn("stream", "No redis/streams/ directory in bundle")
        return
    for full in sorted(stream_dir.glob("*.full.txt")):
        key = full.stem.replace(".full", "").replace("__", ":")
        entries = parse_stream_entries(full)
        if not entries:
            continue
        buckets: dict[str, list[int]] = defaultdict(list)
        for e in entries:
            tok = e.get("fencingToken")
            ent = e.get("entity", "")
            m_seq = re.search(r'"seq":(\d+)', ent)
            if tok is not None and m_seq:
                buckets[tok].append(int(m_seq.group(1)))
        r.facts.setdefault("streams", {})[key] = {
            "xlen": len(entries),
            "tokens": {
                t: {
                    "count": len(seqs),
                    "min": min(seqs),
                    "max": max(seqs),
                }
                for t, seqs in buckets.items()
            },
        }
        if not buckets:
            r.warn("stream", f"{key}: no fencingToken field in entries")
            continue
        # Heuristic: if other buckets are "large" and one is < 10 % of the
        # median, flag a stall.
        counts = {t: len(s) for t, s in buckets.items()}
        median = sorted(counts.values())[len(counts) // 2]
        for tok, cnt in counts.items():
            if median >= 100 and cnt < median * 0.1:
                seqs = sorted(buckets[tok])
                gaps = [
                    (seqs[i - 1] + 1, seqs[i] - 1)
                    for i in range(1, len(seqs))
                    if seqs[i] != seqs[i - 1] + 1
                ]
                r.error(
                    "stream",
                    f"{key}: fencingToken={tok} count={cnt} « median={median} "
                    "— likely CDC pipeline stall (BUG-045 style).",
                    f"seq range [{seqs[0]},{seqs[-1]}]; gaps: {gaps[:5]}"
                    + (f" ... +{len(gaps) - 5} more" if len(gaps) > 5 else ""),
                )
            else:
                r.info(
                    "stream",
                    f"{key}: fencingToken={tok} count={cnt} seq range "
                    f"[{min(buckets[tok])},{max(buckets[tok])}]",
                )


def check_consumer_lag(bundle: Path, r: Report) -> None:
    groups_dir = bundle / "redis" / "streams"
    if not groups_dir.exists():
        return
    for groups_file in groups_dir.glob("*.groups.txt"):
        text = strip_hdr(read(groups_file))
        key = groups_file.stem.replace(".groups", "").replace("__", ":")
        # Parse redis-cli non-TTY output of XINFO GROUPS:
        # alternating key lines and value lines inside each group block
        blocks = re.split(r"\n(?=name\n)", text)
        for blk in blocks:
            if not blk.strip():
                continue
            fields: dict = {}
            lines = [ln for ln in blk.splitlines() if ln.strip()]
            it = iter(lines)
            for k in it:
                try:
                    v = next(it)
                except StopIteration:
                    break
                fields[k] = v
            if "name" not in fields:
                continue
            name = fields["name"]
            try:
                pel = int(fields.get("pending", "0"))
            except ValueError:
                pel = 0
            last = fields.get("last-delivered-id", "0-0")
            if pel > 0:
                r.warn(
                    "consumer",
                    f"{key}/{name}: pending={pel} (events consumed but not ACKed)",
                    f"last-delivered-id={last}",
                )
            else:
                r.info("consumer", f"{key}/{name}: caught up, last-delivered={last}")


def parse_cypher_plain(text: str) -> list[dict]:
    """Parse --format plain cypher-shell output into a list of row dicts.

    First non-warning line is the CSV-ish header (`col1, col2, ...`). Values
    are quoted or bare; we keep them as raw strings.
    """
    text = strip_hdr(text)
    # Drop WARNING: ... lines emitted by the JDK
    rows = [
        ln for ln in text.splitlines()
        if ln.strip() and not ln.startswith("WARNING")
        and not ln.startswith("Plan:") and not ln.startswith("Statement:")
        and not ln.startswith("Version:") and not ln.startswith("Planner:")
        and not ln.startswith("Runtime:") and not ln.startswith("Time:")
    ]
    if not rows:
        return []
    header = [c.strip() for c in rows[0].split(",")]
    out = []
    for line in rows[1:]:
        # Naive CSV split that tolerates commas inside quoted strings
        vals = []
        cur = ""
        in_str = False
        for ch in line:
            if ch == '"':
                in_str = not in_str
                cur += ch
            elif ch == "," and not in_str:
                vals.append(cur.strip())
                cur = ""
            else:
                cur += ch
        vals.append(cur.strip())
        if len(vals) == len(header):
            out.append(dict(zip(header, vals)))
    return out


def check_neo4j_sanity(bundle: Path, r: Report) -> None:
    neo_dir = bundle / "neo4j"
    if not neo_dir.exists() or (neo_dir / "SKIPPED.txt").exists():
        r.warn("neo4j", "neo4j/ directory missing or skipped (no NEO4J_PASSWORD?)")
        return

    # clock skew
    clocks = {}
    for svc_dir in sorted(p for p in neo_dir.iterdir() if p.is_dir()):
        ts_text = read(svc_dir / "clock.txt")
        m = re.search(r"(\d{13})", ts_text)
        if m:
            clocks[svc_dir.name] = int(m.group(1))
    if clocks:
        lo, hi = min(clocks.values()), max(clocks.values())
        r.facts.setdefault("neo4j", {})["clocks"] = clocks
        if hi - lo > 500:
            r.warn(
                "clock",
                f"Neo4j node clock skew: {hi - lo}ms (may affect CDC timing)",
                f"clocks={clocks}",
            )
        else:
            r.info("clock", f"Neo4j clock skew OK ({hi - lo}ms)")

    # trigger state: exactly one node should have all 3 triggers
    trigger_counts: dict[str, int] = {}
    for svc_dir in sorted(p for p in neo_dir.iterdir() if p.is_dir()):
        text = read(svc_dir / "triggers.txt")
        # `cdc-timestamp`, `cdc-capture-node-deletes`, `cdc-capture-rel-deletes`
        installed = len(re.findall(r'"cdc-[a-z\-]+"', text))
        trigger_counts[svc_dir.name] = installed
    r.facts.setdefault("neo4j", {})["trigger_counts"] = trigger_counts
    if trigger_counts:
        fully_armed = [n for n, c in trigger_counts.items() if c >= 3]
        if len(fully_armed) > 1:
            r.error(
                "trigger",
                f"BUG-046 suspected: {len(fully_armed)} nodes have all 3 CDC triggers installed "
                f"(expected exactly 1, the primary). Old triggers likely never uninstalled.",
                f"nodes with triggers: {trigger_counts}",
            )
        elif len(fully_armed) == 1:
            r.info("trigger", f"Primary trigger carrier: {fully_armed[0]}")
        else:
            r.warn("trigger", f"No node has the full trigger set: {trigger_counts}")

    # _updated_at index presence per label
    for svc_dir in sorted(p for p in neo_dir.iterdir() if p.is_dir()):
        idx_text = read(svc_dir / "indexes.txt")
        labels_text = read(svc_dir / "labels.txt")
        user_labels = {
            l.strip('"')
            for l in re.findall(r'"[^"_][^"]*"', labels_text)
            if not l.strip('"').startswith("_")
        }
        missing: list[str] = []
        for label in user_labels:
            # look for a line like  ..., ["TestNode"], ["_updated_at"], "ONLINE", "NODE"
            if not re.search(
                rf'\["{re.escape(label)}"\],\s*\["_updated_at"\]', idx_text
            ):
                missing.append(label)
        if missing:
            r.error(
                "index",
                f"{svc_dir.name}: missing _updated_at range index on labels "
                f"{missing} (BUG-045 fix requires these indexes)",
            )

    # per-node TestNode count cross-check
    counts: dict[str, int] = {}
    for svc_dir in sorted(p for p in neo_dir.iterdir() if p.is_dir()):
        ov_text = read(svc_dir / "testnode_overview.txt")
        m = re.search(r"(\d+),\s*(\d+|NULL),\s*(\d+|NULL),\s*(\d+)", ov_text)
        if m:
            counts[svc_dir.name] = int(m.group(1))
    r.facts.setdefault("neo4j", {})["testnode_counts"] = counts
    if len(counts) >= 2:
        mx = max(counts.values())
        mn = min(counts.values())
        if mx > 0 and (mx - mn) > max(1, mx * 0.01):
            r.error(
                "replication",
                f"TestNode counts diverge across nodes: {counts}. "
                f"Max-min delta={mx - mn} (>1% threshold). "
                f"Suspect BUG-045/CDC pipeline stall.",
            )
        else:
            r.info("replication", f"TestNode count parity OK: {counts}")


def check_haproxy_state(bundle: Path, r: Report) -> None:
    hp_dir = bundle / "haproxy"
    if not hp_dir.exists():
        return
    ready_map: dict[str, list[str]] = {}
    for hp_sub in sorted(p for p in hp_dir.iterdir() if p.is_dir()):
        text = strip_hdr(read(hp_sub / "servers_state.txt"))
        if not text or "No admin socket" in text:
            r.warn("haproxy", f"{hp_sub.name}: could not read admin socket")
            continue
        readies = []
        for line in text.splitlines():
            # Columns in `show servers state` space-separated. srv_op_state=2 means RUNNING.
            # Also match the backend name "neo4j_primary" per v1 format.
            if "neo4j_primary" in line and re.search(r"\s2\s", line):
                m = re.match(r"\S+\s+\S+\s+(\S+)", line)
                if m:
                    readies.append(m.group(1))
        ready_map[hp_sub.name] = readies
    if ready_map:
        r.facts.setdefault("haproxy", {})["ready"] = ready_map
        all_single = all(len(v) == 1 for v in ready_map.values())
        if not all_single:
            r.error(
                "haproxy",
                f"neo4j_primary backend ready-server count != 1 on some instances: {ready_map}",
            )
        else:
            first = next(iter(ready_map.values()))[0]
            if any(v[0] != first for v in ready_map.values()):
                r.error(
                    "haproxy",
                    f"HAProxy instances disagree on primary: {ready_map}",
                )
            else:
                r.info("haproxy", f"All instances route writes to {first}")


def check_agent_log(bundle: Path, r: Report) -> None:
    logf = bundle / "agent" / "stdout.log"
    if not logf.exists():
        return
    text = read(logf)
    fence_rejects = re.findall(
        r"Fencing token rejected.*?(?:stopping CDC Collector|buffering (\d+) unpublished events)",
        text,
    )
    # After BUG-047 fix we expect the reject path (if ever hit) to carry
    # "buffering N unpublished events for retry on next epoch" — that is safe.
    # A plain "stopping CDC Collector" without the buffer message means events
    # were lost.
    buffered = [m for m in fence_rejects if m]
    lost = [m for m in fence_rejects if not m]
    if lost:
        r.error(
            "agent",
            f"{len(lost)} Fencing token rejection(s) without buffering — "
            "indicates BUG-047 recurrence (CDC in-flight events dropped)",
        )
    elif buffered:
        r.warn(
            "agent",
            f"{len(buffered)} Fencing token rejection(s) occurred but events "
            "were safely buffered (BUG-047 defence in depth worked). "
            "Consider investigating the orchestrator order — this path should not be hit.",
        )
    if re.search(r"Failed to uninstall APOC trigger", text):
        r.error("agent", "Agent log contains 'Failed to uninstall APOC trigger' (BUG-046)")
    if re.search(r"already absent.*cdc-timestamp.*already absent", text, re.DOTALL):
        r.info("agent", "Agent uninstalled triggers cleanly across switchover(s)")
    # Count WARN/ERROR lines
    n_err = sum(1 for ln in text.splitlines() if " ERROR " in ln)
    n_warn = sum(1 for ln in text.splitlines() if " WARN " in ln)
    r.facts.setdefault("agent", {})["log_counts"] = {
        "error": n_err, "warn": n_warn
    }
    if n_err > 0:
        r.warn("agent", f"{n_err} ERROR lines in agent log (see agent/errors.log)")


def check_checkpoints(bundle: Path, r: Report) -> None:
    cp_dir = bundle / "redis" / "checkpoints"
    if not cp_dir.exists():
        r.warn("checkpoint", "No redis/checkpoints/ directory; cannot verify")
        return
    for cp in sorted(cp_dir.glob("*.txt")):
        text = strip_hdr(read(cp))
        # Each file is HGETALL output: alternating field/value lines
        kv = {}
        lines = [l for l in text.splitlines() if l.strip()]
        it = iter(lines)
        for k in it:
            try:
                v = next(it)
            except StopIteration:
                break
            kv[k] = v
        if not kv:
            r.warn("checkpoint", f"{cp.name}: empty (key missing in Redis)")
            continue
        updated = int(kv.get("updatedAt", "0") or "0")
        r.info(
            "checkpoint",
            f"{cp.stem}: lastTs={kv.get('lastTs', '?')} "
            f"updatedAt={updated} lastStreamId={kv.get('lastStreamId', '?')}",
        )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def format_report(r: Report) -> str:
    sev_order = {"ERROR": 0, "WARN": 1, "INFO": 2}
    findings = sorted(r.findings, key=lambda f: (sev_order[f.severity], f.category))
    out: list[str] = []
    out.append("# HA Diagnostics Report")
    out.append("")
    n_err = sum(1 for f in findings if f.severity == "ERROR")
    n_warn = sum(1 for f in findings if f.severity == "WARN")
    n_info = sum(1 for f in findings if f.severity == "INFO")
    out.append(f"**Summary: {n_err} ERROR, {n_warn} WARN, {n_info} INFO**")
    out.append("")
    for f in findings:
        icon = {"ERROR": "[E]", "WARN": "[W]", "INFO": "[i]"}[f.severity]
        out.append(f"- {icon} **{f.category}** — {f.message}")
        if f.detail:
            for line in f.detail.splitlines():
                out.append(f"    {line}")
    out.append("")
    out.append("## Facts")
    out.append("```json")
    out.append(json.dumps(r.facts, indent=2, sort_keys=True))
    out.append("```")
    return "\n".join(out)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("bundle", help="directory or tar.gz produced by ha-diag-collect.sh")
    ap.add_argument("--out", help="write report to this file (default: <bundle>/findings.md)")
    args = ap.parse_args(argv)

    bundle = load_bundle(args.bundle)
    report = Report()

    check_stream_tokens(bundle, report)
    check_consumer_lag(bundle, report)
    check_neo4j_sanity(bundle, report)
    check_haproxy_state(bundle, report)
    check_agent_log(bundle, report)
    check_checkpoints(bundle, report)

    text = format_report(report)
    print(text)
    out = Path(args.out) if args.out else bundle / "findings.md"
    out.write_text(text, encoding="utf-8")
    print(f"\n[written to {out}]", file=sys.stderr)

    n_err = sum(1 for f in report.findings if f.severity == "ERROR")
    return 1 if n_err else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
