# HA Diagnostics Toolkit

One-shot collector + analyzer that reproduces the exact diagnostic workflow used
to root-cause BUG-037, BUG-042, BUG-043, BUG-044, BUG-045, BUG-046.

## Files

| File                      | What it does                                                                                |
|---------------------------|---------------------------------------------------------------------------------------------|
| `ha-diag-collect.sh`      | Dumps Redis / Neo4j / HAProxy / ha-agent state into a bundle (tar.gz).                      |
| `ha-diag-analyze.py`      | Reads a bundle, emits a findings report (severity-tagged).                                  |
| `ha-diag-cdc-gap.sh`      | Targeted CDC gap finder: given a failed load-test `writer_id`, locates the missing batch, classifies it as producer-side vs consumer-side, and emits `SUMMARY.md`. |

## Quick start

```bash
# 1. Collect (from the VM running the compose stack)
scripts/diag/ha-diag-collect.sh
# → produces /tmp/ha-diag-<UTC-ts>/ and /tmp/ha-diag-<UTC-ts>.tar.gz

# 2. Analyze (anywhere with python3)
python3 scripts/diag/ha-diag-analyze.py /tmp/ha-diag-<UTC-ts>
# → prints markdown report to stdout + writes bundle/findings.md
# → exit code = 1 if any ERROR finding, else 0
```

## When to run

- **Right after a failed integrity check** (e.g.
  `ha-load-switchover-test.py --clean-before-run` reports `missing=N`)
- **After a manual switchover** that looked abnormal (client errors,
  HAProxy flapping, agent log spikes)
- **Before opening an issue**: always attach the tar.gz to the ticket so the
  reviewer can replay without needing live access

## What the collector captures

| Source   | Items                                                                                                                                      |
|----------|--------------------------------------------------------------------------------------------------------------------------------------------|
| Redis    | `PING`, `INFO {server,memory,stats,persistence}`, keyspace scan `neo4j:ha:*` + `neo4j:cdc:*`, every checkpoint (`HGETALL`), `fencing-token`, `leader-lock`, `node-registry` |
| Streams  | `XLEN`, `XINFO GROUPS`, `XINFO STREAM`, first/last 5 entries, **full XRANGE dump** up to 500k entries (else sampled head+tail 10k), `XPENDING` per consumer group |
| Neo4j    | per-container: `RETURN timestamp()`, `SHOW INDEXES`, `CALL apoc.trigger.list()`, `CALL db.labels()`, TestNode summary + per-writer breakdown, per-label counts, `_CDCDeleteEvent` residue |
| HAProxy  | `show servers state`, `show stat` via admin socket; last 500 lines of container logs                                                       |
| ha-agent | full `docker logs`, plus filtered highlights (`fencing/reject/failover/trigger/ERROR/WARN`) and errors-only file                             |

No passwords are written to disk — only whether env vars are set.

## What the analyzer checks

| Check                          | Diagnoses                                                     |
|--------------------------------|---------------------------------------------------------------|
| Stream token distribution      | CDC pipeline stall in a specific epoch (BUG-045)             |
| Consumer group lag / PEL       | SyncApplier stuck / consumer not making progress             |
| Per-node TestNode parity       | Replication divergence (BUG-037, BUG-045)                    |
| Trigger install count per node | Triggers never uninstalled on old primary (BUG-046)          |
| `_updated_at` range index      | Required for BUG-045 fix; missing → AllNodesScan degradation  |
| Clock skew across Neo4j nodes  | APOC `timestamp()` drift affecting keyset cursor              |
| Checkpoint presence + freshness| CheckpointManager silent failure                              |
| HAProxy ready-server invariant | Exactly one READY in `neo4j_primary`; agreement across instances (BUG-032, BUG-036) |
| Agent log scan                 | Fencing rejects, trigger failures, unhandled exceptions       |

## Report format

```
# HA Diagnostics Report

**Summary: 2 ERROR, 1 WARN, 17 INFO**

- [E] **trigger** — BUG-046 suspected: 3 nodes have all 3 CDC triggers installed ...
- [E] **stream** — neo4j:cdc:neo4j:changes: fencingToken=3 count=6 « median=599 — likely CDC pipeline stall (BUG-045 style).
      seq range [1834,2451]; gaps: [(1835, 2446)]
- [W] **agent** — 3 ERROR lines in agent log (see agent/errors.log)
- [i] **haproxy** — All instances route writes to neo4j-primary
- [i] **clock** — Neo4j clock skew OK (87ms)
...

## Facts
```json
{
  "neo4j": {
    "testnode_counts": {"neo4j-primary": 1803, "neo4j-standby-1": 1805, "neo4j-standby-2": 2406},
    "trigger_counts":  {"neo4j-primary": 3, "neo4j-standby-1": 3, "neo4j-standby-2": 3},
    "clocks":          {"neo4j-primary": 1776430630301, ...}
  },
  "streams": {
    "neo4j:cdc:neo4j:changes": {
      "xlen": 1804,
      "tokens": {"0": {"count": 601, "min": 1, "max": 601}, ...}
    }
  }
}
```

## Examples

### A) Suspect "missing data after rotation"

```bash
# VM
scripts/diag/ha-diag-collect.sh --out /tmp/rot-fail
tar -C /tmp -czf rot-fail.tar.gz rot-fail

# laptop
python3 scripts/diag/ha-diag-analyze.py rot-fail.tar.gz
```

A "BUG-045 style" finding will point at which epoch dropped events and how many.

### B) After upgrade, verify triggers clean across switchover

Run the collector before and after a planned switchover, then diff the trigger
counts: pre-switchover `{primary:3, standbys:0}`, post-switchover after one
cycle should be the same with roles swapped.

### C) Pinpoint which batch of writes a failed rotation dropped

```bash
# VM — run immediately after ha-load-switchover-test.py reports
#      "miss=70" on one or more standbys; take writer_id from its output.
scripts/diag/ha-diag-cdc-gap.sh --writer-id w-245386
# → prints SUMMARY.md to stdout + writes /tmp/cdc-gap-<UTC-ts>/
```

The verdict lines classify the gap as:

- **producer-side** (missing elementIds are NOT in the Redis stream) —
  the old primary never published them (BUG-061 class: `afterAsync` tail
  lost, or new-primary collector baseline skipped the tail).
- **consumer-side** (missing elementIds ARE in the Redis stream but not
  on the standby) — SyncApplier skipped / failed the batch; inspect
  XPENDING and ha-agent applier logs.
- **mixed** — both paths leaked; fix both.

The script also writes a `_created_at` histogram bucketed by second, so
you can align the missing window with a specific switchover timestamp
from the load-test report.

### D) CI: block release if production bundle still has ERROR findings

```bash
python3 scripts/diag/ha-diag-analyze.py /mnt/prod-snapshot/latest-bundle \
    --out /mnt/prod-snapshot/findings.md || { echo "release blocked"; exit 1; }
```

## Adding new checks

Edit `ha-diag-analyze.py` and add a `check_*` function that takes
`(bundle: Path, r: Report)`. Call it from `main()`. Use
`r.error / r.warn / r.info` for findings and `r.facts[category][key] = ...`
for structured data that ends up in the JSON appendix.
