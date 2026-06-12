#!/usr/bin/env python3
"""
HA cluster load + rotating-switchover integrity test.

Scenario:
  1. Connect to the cluster via HAProxy (write via write-backend, read via read-backend)
  2. Start continuous concurrent workload that does NOT stop for the duration of
     the test:
       - MERGE TestNodes at --write-rate Hz (per-writer monotonic seq)
       - MERGE :RELATED_TO relationships between adjacent TestNodes
         at --rel-rate Hz (exercises relationship CDC + delete events)
       - DETACH DELETE random TestNodes at --delete-rate Hz
         (exercises delete CDC + community-property side effects)
       - Optional periodic GDS community-detection (Louvain) writes at
         --gds-interval seconds (exercises bulk property writes from GDS,
         confirms _updated_at trigger fires for procedure-driven mutations)
  3. After an initial steady-state period, rotate the primary role through every
     node, then back to the initial primary (--rotations). While the agent
     performs each switchover, the workload threads pause so HAProxy does not
     return ``defunct connection`` errors and the write error budget stays meaningful.
  4. Continue a trailing steady-state period.
  5. Stop the workload, drain (``--post-quiet-seconds``), optionally poll until
     every instance reports the same ``TestNode`` count (``--replication-sync-timeout``,
     default 120s when switchovers are enabled), then query EVERY Neo4j node directly
     (bypassing HAProxy) and verify ALL of:
       - Every successfully-written, NOT-deleted TestNode seq is present
       - No deleted seq is present (delete propagation)
       - Relationship count matches (entity-relationship sync)
       - GDS community-id distribution matches across all nodes
         (bulk property write sync)

Data model:
  (:TestNode {id, seq, writtenAt, writer, community?})
    - id         string, format "probe-<uuid>"  (unique, used for MERGE)
    - seq        int, monotonic per writer
    - writtenAt  ms-epoch
    - writer     writer thread name (for debugging)
    - community  long, written by GDS Louvain (only when --gds-interval > 0)
  (:TestNode)-[:RELATED_TO {createdAt}]->(:TestNode)
    - relationship between adjacent TestNodes (seq i ↔ seq i+1)

Exit codes:
  0 — workload completed AND every node holds every successfully-written seq
  1 — integrity check failed (missing or duplicate data)
  2 — workload could not be sustained (switchover broke writes beyond tolerance)
  3 — environment / precondition problem

Usage:
  pip install -r scripts/deploy/requirements-load-test.txt

  # Run with Python (do NOT use `bash` on this file — it is Python source):
  python3 scripts/deploy/ha-load-switchover-test.py
  # or: bash scripts/deploy/ha-load-switchover-test.sh

  # credentials are read from docker/.env if present, or the environment

  # explicit override still works:
  NEO4J_PASSWORD=xxx ADMIN_TOKEN=xxx \
    python3 scripts/deploy/ha-load-switchover-test.py
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import signal
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

import requests
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable, SessionExpired, TransientError

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

DEFAULT_WRITE_URI = "bolt://localhost:17687"      # HAProxy-1 write backend
DEFAULT_READ_URI  = "bolt://localhost:17688"      # HAProxy-1 read backend
DEFAULT_AGENT_URL = "http://localhost:8080"

# Directly-addressable Neo4j instances for the integrity check. Keep names in sync
# with ha-agent.yml / test-compose.yml.
DEFAULT_NODES = {
    "node-01": "bolt://localhost:7687",
    "node-02": "bolt://localhost:7688",
    "node-03": "bolt://localhost:7689",
}


def load_env_file(path: pathlib.Path) -> None:
    """Best-effort loader for a bash-style `.env` file.

    Matches what `ha-smoke-test-3node.sh` does (``set -a; source docker/.env``).
    Existing process env takes precedence so explicit `VAR=… python ...` still wins.

    Supported lines:
        KEY=value
        KEY="value with spaces"
        KEY='value'
        export KEY=value
        # comment lines / blank lines are skipped
    """
    if not path.is_file():
        return
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ.setdefault(key, value)


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("ha-load-test")

# Silence Neo4j driver's per-query notifications. These are harmless protocol-level
# hints (e.g. "The provided label is not in the database" before the first write
# creates it) but at 10 ops/s they drown the test's own output. The notifications
# remain available programmatically on each result summary if needed.
for _name in ("neo4j", "neo4j.notifications", "neo4j.io", "neo4j.pool"):
    logging.getLogger(_name).setLevel(logging.ERROR)

# -----------------------------------------------------------------------------
# Stats
# -----------------------------------------------------------------------------

@dataclass
class OpRecord:
    ts: float
    ok: bool
    latency_ms: float
    phase: str           # "pre" | "switchover" | "post"
    error: Optional[str] = None
    # For writes only — the seq value used (success or fail)
    seq: Optional[int] = None


@dataclass
class Stats:
    writes: list[OpRecord] = field(default_factory=list)
    reads: list[OpRecord] = field(default_factory=list)
    rels: list[OpRecord] = field(default_factory=list)
    deletes: list[OpRecord] = field(default_factory=list)
    gds_runs: list[OpRecord] = field(default_factory=list)
    written_seqs: set[int] = field(default_factory=set)        # successfully written
    failed_seqs: set[int] = field(default_factory=set)         # write failed or unknown
    deleted_seqs: set[int] = field(default_factory=set)        # successfully DELETEd
    rel_pairs: set[tuple[int, int]] = field(default_factory=set)  # successful (seq_from, seq_to)
    gds_community_count: int = 0                                # # times community detection ran successfully
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add_write(self, rec: OpRecord) -> None:
        with self.lock:
            self.writes.append(rec)
            if rec.seq is not None:
                if rec.ok:
                    self.written_seqs.add(rec.seq)
                else:
                    self.failed_seqs.add(rec.seq)

    def add_read(self, rec: OpRecord) -> None:
        with self.lock:
            self.reads.append(rec)

    def add_rel(self, rec: OpRecord, pair: Optional[tuple[int, int]] = None) -> None:
        with self.lock:
            self.rels.append(rec)
            if rec.ok and pair is not None:
                self.rel_pairs.add(pair)

    def add_delete(self, rec: OpRecord) -> None:
        with self.lock:
            self.deletes.append(rec)
            if rec.ok and rec.seq is not None:
                self.deleted_seqs.add(rec.seq)
                # If the same seq was earlier "successfully written", we now expect it
                # NOT to appear on any node. Keep written_seqs as the historical record;
                # the integrity check uses (written - deleted) as the expected set.

    def add_gds(self, rec: OpRecord) -> None:
        with self.lock:
            self.gds_runs.append(rec)
            if rec.ok:
                self.gds_community_count += 1

    def expected_present_seqs(self) -> set[int]:
        """Seq values we expect to find on every node = written - deleted."""
        with self.lock:
            return self.written_seqs - self.deleted_seqs


# -----------------------------------------------------------------------------
# Load workers
# -----------------------------------------------------------------------------

STOP = threading.Event()
# When set, writer/reader/rel/delete/gds workers spin without issuing Bolt ops.
# Avoids HAProxy/Neo4j tearing down the write pool mid-switchover (defunct
# connections, spurious write failures, and inflated error_rate).
WORKLOAD_PAUSE = threading.Event()


class PhaseMarker:
    """Thread-safe label for the current workload phase.

    The orchestrator rotates through phases like:
        steady-pre  → switchover-1  → steady-1  → switchover-2  → steady-2 → …
    Each write/read op records the phase at its submission time so we can later
    compute per-phase error rates and latencies.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._label = "warmup"

    def set(self, label: str) -> None:
        with self._lock:
            self._label = label

    def get(self) -> str:
        with self._lock:
            return self._label


class RateLimiter:
    """Simple open-loop rate limiter: target N operations per second."""
    def __init__(self, rate_per_sec: float):
        self.interval = 1.0 / rate_per_sec
        self.next_at = time.monotonic()

    def sleep(self) -> None:
        now = time.monotonic()
        sleep_for = self.next_at - now
        if sleep_for > 0:
            time.sleep(sleep_for)
        self.next_at += self.interval
        # If we fell behind by more than one tick, drop the backlog to avoid bursting.
        if self.next_at < time.monotonic() - self.interval:
            self.next_at = time.monotonic() + self.interval


def writer_loop(
    driver,
    stats: Stats,
    rate_per_sec: float,
    writer_id: str,
    phase: PhaseMarker,
) -> None:
    """MERGE one node per tick. Uses a monotonic per-writer seq namespace."""
    limiter = RateLimiter(rate_per_sec)
    seq = 0
    while not STOP.is_set():
        if WORKLOAD_PAUSE.is_set():
            time.sleep(0.05)
            continue
        limiter.sleep()
        if STOP.is_set():
            break
        seq += 1
        node_id = f"probe-{writer_id}-{seq}"
        t0 = time.monotonic()
        ok = False
        err: Optional[str] = None
        try:
            with driver.session(database="neo4j") as s:
                s.run(
                    """
                    MERGE (n:TestNode {id: $id})
                    SET n.seq = $seq, n.writtenAt = timestamp(), n.writer = $writer
                    RETURN n.id AS id
                    """,
                    id=node_id, seq=seq, writer=writer_id,
                ).consume()
            ok = True
        except (ServiceUnavailable, SessionExpired, TransientError, Neo4jError) as e:
            err = f"{type(e).__name__}: {e}"
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        latency = (time.monotonic() - t0) * 1000.0
        stats.add_write(OpRecord(
            ts=time.time(), ok=ok, latency_ms=latency,
            phase=phase.get(), error=err, seq=seq,
        ))


def reader_loop(
    driver,
    stats: Stats,
    rate_per_sec: float,
    phase: PhaseMarker,
) -> None:
    limiter = RateLimiter(rate_per_sec)
    while not STOP.is_set():
        if WORKLOAD_PAUSE.is_set():
            time.sleep(0.05)
            continue
        limiter.sleep()
        if STOP.is_set():
            break
        t0 = time.monotonic()
        ok = False
        err: Optional[str] = None
        try:
            with driver.session(database="neo4j") as s:
                # A small COUNT query — cheap but exercises the read path
                s.run("MATCH (n:TestNode) RETURN count(n) AS c").single()
            ok = True
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        latency = (time.monotonic() - t0) * 1000.0
        stats.add_read(OpRecord(
            ts=time.time(), ok=ok, latency_ms=latency,
            phase=phase.get(), error=err,
        ))


def relationship_loop(
    driver,
    stats: Stats,
    rate_per_sec: float,
    writer_id: str,
    phase: PhaseMarker,
) -> None:
    """MERGE :RELATED_TO between adjacent TestNodes the writer has produced.

    Picks (seq, seq+1) pairs in the *same* writer namespace so the relationship
    creation only succeeds AFTER both endpoints exist. This is intentional: it
    means a switchover-induced lost write on either endpoint also kills the
    relationship, which is part of what we want to verify (no orphan rels).
    """
    if rate_per_sec <= 0:
        return
    limiter = RateLimiter(rate_per_sec)
    seq = 1
    while not STOP.is_set():
        if WORKLOAD_PAUSE.is_set():
            time.sleep(0.05)
            continue
        limiter.sleep()
        if STOP.is_set():
            break
        seq_from = seq
        seq_to = seq + 1
        seq += 1
        from_id = f"probe-{writer_id}-{seq_from}"
        to_id = f"probe-{writer_id}-{seq_to}"
        t0 = time.monotonic()
        ok = False
        err: Optional[str] = None
        pair_recorded: Optional[tuple[int, int]] = None
        try:
            with driver.session(database="neo4j") as s:
                # Only create the relationship if BOTH endpoints already exist —
                # avoids accidentally MERGE-creating placeholder TestNodes here.
                created = s.run(
                    """
                    MATCH (a:TestNode {id: $from_id}), (b:TestNode {id: $to_id})
                    MERGE (a)-[r:RELATED_TO]->(b)
                      ON CREATE SET r.createdAt = timestamp(), r.writer = $writer
                    RETURN id(r) AS rid
                    """,
                    from_id=from_id, to_id=to_id, writer=writer_id,
                ).single()
                ok = created is not None
                if ok:
                    pair_recorded = (seq_from, seq_to)
        except (ServiceUnavailable, SessionExpired, TransientError, Neo4jError) as e:
            err = f"{type(e).__name__}: {e}"
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        latency = (time.monotonic() - t0) * 1000.0
        stats.add_rel(OpRecord(
            ts=time.time(), ok=ok, latency_ms=latency,
            phase=phase.get(), error=err, seq=seq_from,
        ), pair=pair_recorded)


def delete_loop(
    driver,
    stats: Stats,
    rate_per_sec: float,
    writer_id: str,
    phase: PhaseMarker,
    delete_age_seconds: int,
) -> None:
    """DETACH DELETE TestNodes older than `delete_age_seconds`.

    Deleting only "old" nodes means the writer/rel loops have had time to commit
    them on the primary AND for replication to start; we don't want delete
    events arriving on standbys before the corresponding create. The
    `_CDCDeleteEvent` trigger turns the delete into a transit node that the
    sync-applier picks up and replays as DETACH DELETE on each standby.
    """
    if rate_per_sec <= 0:
        return
    limiter = RateLimiter(rate_per_sec)
    while not STOP.is_set():
        if WORKLOAD_PAUSE.is_set():
            time.sleep(0.05)
            continue
        limiter.sleep()
        if STOP.is_set():
            break
        cutoff_ms = int((time.time() - delete_age_seconds) * 1000)
        t0 = time.monotonic()
        ok = False
        err: Optional[str] = None
        deleted_seq: Optional[int] = None
        try:
            with driver.session(database="neo4j") as s:
                # Pick one random eligible TestNode and delete it. Returning the
                # seq lets us track exactly which node was removed.
                rec = s.run(
                    """
                    MATCH (n:TestNode {writer: $w})
                    WHERE n.writtenAt < $cutoff
                    WITH n, rand() AS r ORDER BY r LIMIT 1
                    WITH n, n.seq AS seq
                    DETACH DELETE n
                    RETURN seq AS seq
                    """,
                    w=writer_id, cutoff=cutoff_ms,
                ).single()
                if rec is not None:
                    deleted_seq = rec["seq"]
                    ok = True
                else:
                    # Nothing to delete yet — still record a "no-op" so phase stats are honest.
                    ok = True
        except (ServiceUnavailable, SessionExpired, TransientError, Neo4jError) as e:
            err = f"{type(e).__name__}: {e}"
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        latency = (time.monotonic() - t0) * 1000.0
        stats.add_delete(OpRecord(
            ts=time.time(), ok=ok, latency_ms=latency,
            phase=phase.get(), error=err, seq=deleted_seq,
        ))


def gds_loop(
    driver,
    stats: Stats,
    interval_seconds: float,
    phase: PhaseMarker,
) -> None:
    """Periodically run GDS Louvain community detection and write back the
    `community` property to every TestNode.

    BUG-063 contract: we deliberately use ``gds.louvain.stream`` + an explicit
    Cypher ``SET`` rather than ``gds.louvain.write``. The ``.write`` variant
    bypasses APOC ``cdc-timestamp`` (phase:'before') because GDS batch writes
    do not flow through the Cypher transaction layer — the trigger never fires,
    ``_updated_at`` is never bumped, CDC's keyset polling never sees the change,
    and the algorithm result stays pinned to the primary forever. With
    ``.stream`` the write happens inside a normal Cypher tx, so APOC fires,
    CDC captures a ``NODE_UPDATED``, and SyncApplier reproduces it on every
    standby. See ``docs/nuclear-fusion/operations/ha-agent-cluster-operations.md``
    §"GDS write contract".

    Tests two things at once:
      1. The recommended pattern (``.stream`` + ``SET``) produces
         ``NODE_UPDATED`` events that flow end-to-end to standbys.
      2. A *large* batch of property writes inside a single transaction does
         not break the CDC keyset pagination on the standby side.

    Skips silently if GDS is not installed.
    """
    if interval_seconds <= 0:
        return
    graph_name = "load-test-graph"
    while not STOP.is_set():
        # Sleep in small chunks so we react quickly to STOP.
        slept = 0.0
        while slept < interval_seconds and not STOP.is_set():
            time.sleep(min(1.0, interval_seconds - slept))
            slept += 1.0
        if STOP.is_set():
            break

        while WORKLOAD_PAUSE.is_set() and not STOP.is_set():
            time.sleep(0.05)
        if STOP.is_set():
            break

        t0 = time.monotonic()
        ok = False
        err: Optional[str] = None
        try:
            with driver.session(database="neo4j") as s:
                # Best-effort drop of any prior projection (silently ignored if absent).
                try:
                    s.run("CALL gds.graph.drop($g, false) YIELD graphName RETURN graphName",
                          g=graph_name).consume()
                except Exception:
                    pass

                # Project TestNode + RELATED_TO. UNDIRECTED makes Louvain stable for
                # tiny graphs.
                projected = s.run(
                    """
                    CALL gds.graph.project.cypher(
                        $g,
                        'MATCH (n:TestNode) RETURN id(n) AS id',
                        'MATCH (a:TestNode)-[r:RELATED_TO]->(b:TestNode)
                         RETURN id(a) AS source, id(b) AS target, "RELATED_TO" AS type'
                    )
                    YIELD nodeCount, relationshipCount
                    RETURN nodeCount, relationshipCount
                    """,
                    g=graph_name,
                ).single()

                if projected is None or projected["nodeCount"] < 2:
                    # Not enough graph yet; skip this cycle, count as ok.
                    log.info("[gds] graph too small (nodes=%s) — skipping",
                             projected["nodeCount"] if projected else "?")
                    ok = True
                else:
                    # BUG-063: stream + SET, never `.write`. Two extra
                    # subtleties we learned the hard way:
                    #
                    #  1. `.write` variants (gds.*.write, gds.graph.writeNodeProperties,
                    #     gds.graph.writeRelationship) go through GDS's batch property
                    #     store path, not the Cypher tx, so APOC `cdc-timestamp`
                    #     never fires.
                    #
                    #  2. `gds.util.asNode(nodeId)` returned from a stream-YIELD
                    #     also does NOT rebind `n` into the caller's Cypher tx —
                    #     the node handle stays in GDS's internal tx context.
                    #     A subsequent `SET n.community = ...` on that handle
                    #     bypasses Cypher's TransactionEventListener, so APOC
                    #     `cdc-timestamp` STILL does not fire (verified on Neo4j
                    #     2026.02.3 with GDS 2.x).
                    #
                    # The only safe pattern is: stream YIELD the raw nodeId,
                    # then RE-MATCH in the caller's Cypher scope:
                    #
                    #     CALL gds.louvain.stream(...) YIELD nodeId, communityId
                    #     MATCH (n) WHERE id(n) = nodeId
                    #     SET n.community = communityId
                    #
                    # The `MATCH (n) WHERE id(n) = nodeId` re-binds `n`
                    # through Cypher's standard pattern matcher, so the
                    # subsequent SET flows through the normal transaction
                    # path → APOC fires → `_updated_at` bumps → CDC picks it up.
                    stats_rec = s.run(
                        """
                        CALL gds.louvain.stats($g)
                        YIELD communityCount, modularity
                        RETURN communityCount, modularity
                        """,
                        g=graph_name,
                    ).single()

                    written = s.run(
                        """
                        CALL gds.louvain.stream($g)
                        YIELD nodeId, communityId
                        MATCH (n) WHERE id(n) = nodeId
                        SET n.community = communityId
                        RETURN count(*) AS nodePropertiesWritten
                        """,
                        g=graph_name,
                    ).single()

                    log.info("[gds] louvain.stream+SET: %s communities over %s nodes (modularity=%.3f)",
                             stats_rec["communityCount"],
                             written["nodePropertiesWritten"],
                             stats_rec["modularity"])
                    ok = True

                # Always drop the projection so it doesn't accumulate in catalog.
                try:
                    s.run("CALL gds.graph.drop($g, false) YIELD graphName RETURN graphName",
                          g=graph_name).consume()
                except Exception:
                    pass
        except Neo4jError as e:
            # `Unknown procedure 'gds.louvain.stream'` → GDS not installed;
            # log once and stop the loop so it doesn't spam.
            err = f"{type(e).__name__}: {e}"
            if "Unknown procedure" in str(e) or "There is no procedure" in str(e):
                log.warning("[gds] GDS not installed in the cluster — disabling GDS workload (%s)", e)
                stats.add_gds(OpRecord(
                    ts=time.time(), ok=False,
                    latency_ms=(time.monotonic() - t0) * 1000.0,
                    phase=phase.get(), error=err,
                ))
                return
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
        latency = (time.monotonic() - t0) * 1000.0
        stats.add_gds(OpRecord(
            ts=time.time(), ok=ok, latency_ms=latency,
            phase=phase.get(), error=err,
        ))


# -----------------------------------------------------------------------------
# Switchover coordination
# -----------------------------------------------------------------------------

def get_cluster_status(agent_url: str) -> dict:
    r = requests.get(f"{agent_url}/cluster/status", timeout=5)
    r.raise_for_status()
    return r.json()


def pick_online_standby(status: dict) -> Optional[str]:
    for n in status.get("nodes", []):
        if n.get("role") == "STANDBY" and n.get("serviceState") == "ONLINE":
            return n.get("id")
    return None


def trigger_switchover(agent_url: str, admin_token: str, target_node_id: str) -> None:
    log.info("Triggering switchover → %s", target_node_id)
    r = requests.post(
        f"{agent_url}/cluster/switchover",
        params={"targetNodeId": target_node_id},
        headers={"Authorization": f"Bearer {admin_token}"},
        timeout=30,
    )
    r.raise_for_status()


def wait_for_primary(agent_url: str, expected_primary: str, timeout: float = 30.0) -> None:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            st = get_cluster_status(agent_url)
            if st.get("primaryNode") == expected_primary:
                log.info("Primary is now %s (took %.1fs)", expected_primary,
                         time.monotonic() - start)
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"Primary did not become {expected_primary} within {timeout}s")


def wait_all_online(agent_url: str, timeout: float = 60.0) -> None:
    """Wait until every node reports serviceState=ONLINE. Caps the post-switchover
    'integrity' phase so we know the replicas are fully caught up."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            st = get_cluster_status(agent_url)
            states = [n.get("serviceState") for n in st.get("nodes", [])]
            if states and all(s == "ONLINE" for s in states):
                log.info("All nodes ONLINE after %.1fs", time.monotonic() - start)
                return
        except Exception:
            pass
        time.sleep(1.0)
    log.warning("Not all nodes reached ONLINE within %.0fs; proceeding anyway", timeout)


def wait_node_online(agent_url: str, node_id: str, timeout: float = 60.0) -> bool:
    """Poll until a specific node reports serviceState=ONLINE."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            st = get_cluster_status(agent_url)
            for n in st.get("nodes", []):
                if n.get("id") == node_id and n.get("serviceState") == "ONLINE":
                    return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


def wait_testnode_writer_sync(
    nodes: dict[str, str],
    user: str,
    password: str,
    writer_id: str,
    expected_count: int,
    timeout: float,
) -> bool:
    """Poll each Neo4j bolt URI until ``TestNode`` count for ``writer_id`` matches.

    Async HA replication (CDC → Redis → SyncApplier) can lag the primary by
    many seconds after switchovers and bulk GDS ``SET``s. A fixed sleep is
    brittle; this loop makes the integrity check wait for convergence.
    """
    if timeout <= 0:
        return True
    start = time.monotonic()
    deadline = start + timeout
    last_counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        last_counts = {}
        all_ok = True
        for nid, uri in nodes.items():
            try:
                drv = GraphDatabase.driver(uri, auth=(user, password))
                try:
                    with drv.session(database="neo4j") as s:
                        c = int(s.run(
                            "MATCH (n:TestNode {writer: $w}) RETURN count(n) AS c",
                            w=writer_id,
                        ).single()["c"])
                        last_counts[nid] = c
                        if c != expected_count:
                            all_ok = False
                finally:
                    drv.close()
            except Exception:
                all_ok = False
                last_counts[nid] = -1
        if all_ok:
            log.info(
                "Replication sync: all nodes report %d TestNodes for writer %s (%.1fs)",
                expected_count, writer_id, time.monotonic() - start,
            )
            return True
        time.sleep(1.0)
    log.warning(
        "Replication sync timed out after %.0fs — expected %d TestNodes per node, got %s",
        timeout, expected_count, last_counts,
    )
    return False


def wait_rel_count_converged(
    nodes: dict[str, str],
    user: str,
    password: str,
    writer_id: str,
    timeout: float,
) -> bool:
    """Poll each node's RELATED_TO count for ``writer_id`` until cross-node
    convergence + temporal stability.

    Rationale: :TestNode count has a globally-known expected value
    (`written - deleted`) because the writer/delete stats track it exactly.
    :RELATED_TO count does NOT — a rel `(a,b)` survives iff both endpoints
    survived DETACH DELETE, and that state is per-node (a rel only ever
    committed on node A may be entirely absent on node B if the endpoint
    was deleted on A before the rel event propagated). So "is synced"
    cannot be defined against an absolute target like for TestNodes;
    instead we define it as:

      * all three nodes agree on the rel count for this writer, AND
      * the agreed count has been stable across two consecutive polls

    The second check handles the "all three happened to be in the same
    transient mid-sync state" false positive. Cost: at minimum 2 × poll
    interval = 2 s added to the integrity phase; typically bounded by
    the actual replication lag (~1-5 s under load).

    Without this, the integrity check can begin while SyncApplier on
    standbys still has in-flight REL_MERGE / DELETE events pending from
    the final steady window, producing phantom ``rel_miss`` that converges
    to zero a few seconds later — indistinguishable in the report from
    a genuine HA data-loss bug. With this, any residual ``rel_miss``
    after the function returns True is a real gap that warrants
    investigation (BUG-080 reverse-reconcile residue, BUG-081
    cross-tx index visibility, or stream trim).
    """
    if timeout <= 0:
        return True
    start = time.monotonic()
    deadline = start + timeout
    prev_counts: Optional[dict[str, int]] = None
    last_counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        last_counts = {}
        for nid, uri in nodes.items():
            try:
                drv = GraphDatabase.driver(uri, auth=(user, password))
                try:
                    with drv.session(database="neo4j") as s:
                        last_counts[nid] = int(s.run(
                            "MATCH (a:TestNode {writer: $w})-[r:RELATED_TO]->"
                            "(b:TestNode {writer: $w}) RETURN count(r) AS c",
                            w=writer_id,
                        ).single()["c"])
                finally:
                    drv.close()
            except Exception:
                last_counts[nid] = -1
        unique = set(last_counts.values())
        # Cross-node convergence + temporal stability.
        if len(unique) == 1 and -1 not in unique and last_counts == prev_counts:
            log.info(
                "Rel-count sync: all nodes report %d :RELATED_TO for writer %s (%.1fs)",
                next(iter(unique)), writer_id, time.monotonic() - start,
            )
            return True
        prev_counts = dict(last_counts)
        time.sleep(1.0)
    log.warning(
        "Rel-count sync timed out after %.0fs — last counts=%s (divergence may "
        "indicate real rel_miss, not just replication lag)",
        timeout, last_counts,
    )
    return False


def build_rotation_plan(initial_primary: str, all_nodes: list[str],
                         rotations: int = 1) -> list[str]:
    """Produce the sequence of target nodes for switchovers.

    For a 3-node cluster with initial primary "node-01" and nodes
    ["node-01", "node-02", "node-03"], one rotation yields:
        ["node-02", "node-03", "node-01"]   (3 switchovers)
    Two rotations yield the same sequence repeated twice, always ending on the
    initial primary so the cluster finishes where it started.
    """
    others = [n for n in all_nodes if n != initial_primary]
    single_rotation = [*others, initial_primary]
    return single_rotation * max(1, rotations)


# -----------------------------------------------------------------------------
# Integrity check
# -----------------------------------------------------------------------------

def dump_node_state(node_id: str, uri: str, user: str, password: str,
                     writer_id: Optional[str] = None) -> dict:
    """Snapshot a single Neo4j node's state for the integrity check.

    Returns:
        {
          'count':              total :TestNode count in the DB,
          'count_current_run':  :TestNode written by THIS run,
          'seqs':               set of seq values in this run,
          'stale_count':        :TestNode left over from previous runs,
          'stale_writers':      list of writer_ids in stale data,
          'rel_count':          # of :RELATED_TO edges between this run's TestNodes,
          'rel_pairs':          set of (seq_from, seq_to) tuples for THIS run,
          'community_dist':     {community_id (int) → count (int)} —
                                 distribution of GDS-written `community` property,
          'community_nodes':    # of TestNodes that have a `community` property,
        }

    When `writer_id` is given, the integrity view restricts itself to nodes written by
    THIS run. Residual data from previous aborted runs is reported separately as
    `stale_count` / `stale_writers` so the operator sees it but it doesn't contaminate
    the pass/fail verdict.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database="neo4j") as s:
            total = s.run("MATCH (n:TestNode) RETURN count(n) AS c").single()["c"]

            if writer_id is not None:
                current = s.run(
                    "MATCH (n:TestNode {writer: $w}) RETURN count(n) AS c, collect(n.seq) AS seqs",
                    w=writer_id,
                ).single()
                count_current = current["c"]
                seqs = {v for v in current["seqs"] if v is not None}

                stale = s.run(
                    """
                    MATCH (n:TestNode) WHERE n.writer <> $w OR n.writer IS NULL
                    RETURN count(n) AS c, collect(DISTINCT n.writer) AS writers
                    """,
                    w=writer_id,
                ).single()
                stale_count = stale["c"]
                stale_writers = [w for w in stale["writers"] if w is not None]

                rel_rec = s.run(
                    """
                    MATCH (a:TestNode {writer: $w})-[r:RELATED_TO]->(b:TestNode {writer: $w})
                    RETURN count(r) AS c, collect([a.seq, b.seq]) AS pairs
                    """,
                    w=writer_id,
                ).single()
                rel_count = rel_rec["c"]
                rel_pairs = {tuple(p) for p in rel_rec["pairs"]
                             if p and p[0] is not None and p[1] is not None}

                # Community distribution: histogram of community-id → count, restricted
                # to THIS run's TestNodes that actually carry a community property.
                comm_recs = list(s.run(
                    """
                    MATCH (n:TestNode {writer: $w})
                    WHERE n.community IS NOT NULL
                    RETURN n.community AS c, count(*) AS cnt
                    """,
                    w=writer_id,
                ))
                community_dist = {int(r["c"]): int(r["cnt"]) for r in comm_recs}
                community_nodes = sum(community_dist.values())
            else:
                count_current = total
                seqs = {r["s"] for r in s.run(
                    "MATCH (n:TestNode) RETURN n.seq AS s"
                ) if r["s"] is not None}
                stale_count = 0
                stale_writers = []
                rel_rec = s.run(
                    "MATCH (:TestNode)-[r:RELATED_TO]->(:TestNode) "
                    "RETURN count(r) AS c"
                ).single()
                rel_count = rel_rec["c"]
                rel_pairs = set()
                community_dist = {}
                community_nodes = 0

        return {
            "count": total,
            "count_current_run": count_current,
            "seqs": seqs,
            "stale_count": stale_count,
            "stale_writers": stale_writers,
            "rel_count": rel_count,
            "rel_pairs": rel_pairs,
            "community_dist": community_dist,
            "community_nodes": community_nodes,
        }
    finally:
        driver.close()


# -----------------------------------------------------------------------------
# Reporting
# -----------------------------------------------------------------------------

def summarize(records: list[OpRecord]) -> dict:
    if not records:
        return {"count": 0}
    ok = [r for r in records if r.ok]
    fail = [r for r in records if not r.ok]
    latencies = [r.latency_ms for r in ok]
    # Group by whatever phase labels actually appeared, preserving the order the
    # orchestrator used (e.g. steady-0, switchover-1-to-node-02, steady-1, ...).
    by_phase: dict[str, dict[str, int]] = {}
    for r in records:
        slot = by_phase.setdefault(r.phase, {"total": 0, "ok": 0, "fail": 0})
        slot["total"] += 1
        if r.ok:
            slot["ok"] += 1
        else:
            slot["fail"] += 1
    return {
        "count": len(records),
        "ok": len(ok),
        "fail": len(fail),
        "error_rate": len(fail) / len(records) if records else 0.0,
        "latency_ms": {
            "p50": round(statistics.median(latencies), 2) if latencies else None,
            "p95": round(_percentile(latencies, 95), 2) if latencies else None,
            "p99": round(_percentile(latencies, 99), 2) if latencies else None,
            "max": round(max(latencies), 2) if latencies else None,
        },
        "by_phase": by_phase,
    }


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    # Auto-load docker/.env the same way ha-smoke-test-3node.sh does, so that
    # NEO4J_PASSWORD / ADMIN_TOKEN / REDIS_PASSWORD don't have to be exported by
    # the user for every invocation. Existing env vars take precedence.
    script_dir = pathlib.Path(__file__).resolve().parent
    project_root = script_dir.parent.parent
    env_file = pathlib.Path(os.environ.get("ENV_FILE") or (project_root / "docker" / ".env"))
    load_env_file(env_file)

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--write-uri", default=os.getenv("WRITE_URI", DEFAULT_WRITE_URI),
                   help="HAProxy write backend URI")
    p.add_argument("--read-uri", default=os.getenv("READ_URI", DEFAULT_READ_URI),
                   help="HAProxy read backend URI")
    p.add_argument("--agent-url", default=os.getenv("AGENT_URL", DEFAULT_AGENT_URL))
    p.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"))
    p.add_argument("--password", default=os.getenv("NEO4J_PASSWORD"))
    p.add_argument("--admin-token", default=os.getenv("ADMIN_TOKEN"))
    p.add_argument("--write-rate", type=float, default=10.0, help="writes/sec (TestNode MERGE)")
    p.add_argument("--read-rate",  type=float, default=10.0, help="reads/sec")
    p.add_argument("--rel-rate",   type=float, default=5.0,
                   help="relationships/sec (MERGE :RELATED_TO between adjacent TestNodes); "
                        "set 0 to disable")
    p.add_argument("--delete-rate", type=float, default=1.0,
                   help="deletes/sec (DETACH DELETE old random TestNode); set 0 to disable")
    p.add_argument("--delete-age-seconds", type=int, default=15,
                   help="only delete TestNodes older than N seconds, so their create+rel "
                        "had time to replicate before the delete event hits standbys")
    p.add_argument("--gds-interval", type=float, default=30.0,
                   help="seconds between GDS Louvain runs (gds.louvain.stream + SET, see BUG-063); set 0 to "
                        "disable. Auto-disables on first 'unknown procedure' error.")
    p.add_argument("--interval-seconds", type=int, default=60,
                   help="Seconds of steady load between consecutive switchovers. "
                        "Also used as the initial warm-up window and the final cool-down "
                        "window after the last switchover. (default 60)")
    p.add_argument("--rotations", type=int, default=1,
                   help="How many times to rotate the primary through every non-initial "
                        "node AND back to the initial primary. Default 1 — i.e. for a "
                        "3-node cluster this performs 3 switchovers "
                        "(initial→A→B→initial).")
    p.add_argument("--skip-switchover", action="store_true",
                   help="Do not trigger any switchover; run workload straight through "
                        "for --interval-seconds")
    p.add_argument("--post-quiet-seconds", type=int, default=10,
                   help="After workload stops, wait this many seconds for replicas to catch up")
    p.add_argument("--replication-sync-timeout", type=float, default=120.0,
                   help="After post-quiet, poll each Neo4j node until TestNode counts match "
                        "the expected total (writer-scoped), up to this many seconds. "
                        "0 disables. Default 120. For --skip-switchover this is forced to 0.")
    p.add_argument("--error-budget", type=float, default=0.02,
                   help="Max tolerated fraction of failed writes (default 2%%)")
    p.add_argument("--report-file", default="load-switchover-report.json",
                   help="Write machine-readable report to this path")
    p.add_argument("--clean-before-run", action="store_true",
                   help="DETACH DELETE every (:TestNode) on the cluster before starting. "
                        "Useful after a previous run was aborted and left residual nodes.")
    args = p.parse_args()

    repl_sync_timeout = 0.0 if args.skip_switchover else float(args.replication_sync_timeout)

    if not args.password:
        log.error("NEO4J_PASSWORD not set")
        return 3
    if not args.admin_token and not args.skip_switchover:
        log.error("ADMIN_TOKEN required unless --skip-switchover")
        return 3

    auth = (args.user, args.password)

    # Driver pools — each writer/reader uses its own session per request, so one driver per role suffices.
    log.info("Connecting: write=%s  read=%s  agent=%s",
             args.write_uri, args.read_uri, args.agent_url)
    write_driver = GraphDatabase.driver(args.write_uri, auth=auth,
                                         max_connection_lifetime=60,
                                         connection_acquisition_timeout=30)
    read_driver  = GraphDatabase.driver(args.read_uri, auth=auth,
                                         max_connection_lifetime=60,
                                         connection_acquisition_timeout=30)

    # Verify connectivity and initial cluster state
    write_driver.verify_connectivity()
    read_driver.verify_connectivity()

    if args.clean_before_run:
        log.info("Cleaning residual (:TestNode) nodes before test starts…")
        try:
            with write_driver.session(database="neo4j") as s:
                total_deleted = 0
                while True:
                    deleted = s.run(
                        "MATCH (n:TestNode) WITH n LIMIT 10000 DETACH DELETE n "
                        "RETURN count(*) AS c"
                    ).single()["c"]
                    total_deleted += deleted
                    if deleted == 0:
                        break
            log.info("Removed %d residual TestNode(s)", total_deleted)
            # Give CDC a moment to replicate the deletes so standbys are also empty
            time.sleep(2)
        except Exception as e:
            log.warning("Cleanup before run failed (continuing anyway): %s", e)

        # BUG-067 safety net: sweep orphan `:_CDCDeleteEvent` transit nodes on
        # EVERY node directly (bypassing HAProxy). Residual events can sit on
        # ex-primaries (e.g. a standby that was briefly promoted during a
        # failover, then demoted before its cleanup tick ran) and HAProxy-
        # routed writes never reach them. Fix B (CdcCollector.start() startup
        # sweep) self-heals them on the next promotion; this A-side net keeps
        # repeat test runs from inheriting the pollution of a prior run.
        log.info("Sweeping orphan :_CDCDeleteEvent on all nodes (BUG-067)…")
        for nid, uri in DEFAULT_NODES.items():
            try:
                drv = GraphDatabase.driver(uri, auth=auth)
                try:
                    with drv.session(database="neo4j") as s:
                        swept = s.run(
                            "MATCH (e:_CDCDeleteEvent) DETACH DELETE e "
                            "RETURN count(*) AS c"
                        ).single()["c"]
                    if swept > 0:
                        log.warning(
                            "  %s: removed %d orphan _CDCDeleteEvent node(s) "
                            "(BUG-067 residue from previous run)", nid, swept,
                        )
                    else:
                        log.info("  %s: no orphan _CDCDeleteEvent", nid)
                finally:
                    drv.close()
            except Exception as e:
                log.warning("  %s: sweep failed (continuing): %s", nid, e)

    status = get_cluster_status(args.agent_url)
    initial_primary = status.get("primaryNode")
    log.info("Initial primary: %s", initial_primary)
    if initial_primary is None:
        log.error("Cluster status has no primaryNode — aborting")
        return 3

    # Signal handling so ^C produces a clean report
    def _sigint(*_):
        log.warning("Interrupt received — stopping workload")
        STOP.set()
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    stats = Stats()
    phase = PhaseMarker()

    # We run a fixed mix of workers — writer / reader / relationship / delete /
    # gds — each on its own thread. The pool size is sized so even if one worker
    # blocks (e.g. GDS Louvain on a multi-thousand-node graph) the others keep
    # making progress.
    writer_id = f"w-{uuid.uuid4().hex[:6]}"

    workers_planned = ["writer", "reader"]
    if args.rel_rate > 0:
        workers_planned.append("relationship")
    if args.delete_rate > 0:
        workers_planned.append("delete")
    if args.gds_interval > 0:
        workers_planned.append("gds")

    executor = ThreadPoolExecutor(max_workers=len(workers_planned), thread_name_prefix="load")
    t0 = time.time()
    executor.submit(writer_loop, write_driver, stats, args.write_rate, writer_id, phase)
    executor.submit(reader_loop, read_driver, stats, args.read_rate, phase)
    if args.rel_rate > 0:
        executor.submit(relationship_loop, write_driver, stats, args.rel_rate, writer_id, phase)
    if args.delete_rate > 0:
        executor.submit(delete_loop, write_driver, stats, args.delete_rate,
                        writer_id, phase, args.delete_age_seconds)
    if args.gds_interval > 0:
        executor.submit(gds_loop, write_driver, stats, args.gds_interval, phase)
    log.info("Load started (writes=%s/s, reads=%s/s, rels=%s/s, deletes=%s/s, "
             "gds-interval=%ss); initial steady window %ds",
             args.write_rate, args.read_rate, args.rel_rate, args.delete_rate,
             args.gds_interval, args.interval_seconds)

    # Record switchover events for the report (each: target, start_ts, duration_s, ok)
    switchover_events: list[dict] = []

    def _abort(reason: str) -> int:
        log.error("Aborting workload: %s", reason)
        STOP.set()
        executor.shutdown(wait=True)
        return 2

    if args.skip_switchover:
        # No rotation — just run one interval and stop.
        phase.set("steady-0")
        log.info("--skip-switchover set; running steady load for %ds", args.interval_seconds)
        time.sleep(args.interval_seconds)
    else:
        # Build rotation plan
        all_node_ids = list(DEFAULT_NODES.keys())
        # Cross-check with /cluster/status so we only rotate through nodes the Agent knows
        known_ids = {n["id"] for n in get_cluster_status(args.agent_url).get("nodes", [])}
        rotation_nodes = [nid for nid in all_node_ids if nid in known_ids]
        if len(rotation_nodes) < 2:
            return _abort(f"need ≥2 nodes in cluster status for rotation, got {rotation_nodes}")
        plan = build_rotation_plan(initial_primary, rotation_nodes, args.rotations)
        log.info("Rotation plan (total %d switchovers): %s  [initial primary: %s]",
                 len(plan), " → ".join([initial_primary, *plan]), initial_primary)

        # Initial warmup window
        phase.set("steady-0")
        log.info("Steady phase 0 (%ds) before first switchover…", args.interval_seconds)
        time.sleep(args.interval_seconds)

        current_primary = initial_primary
        for i, target in enumerate(plan, start=1):
            # Before switching, make sure target is ONLINE (required by the Agent's
            # switchover endpoint after BUG-017).
            log.info("[switchover %d/%d] Waiting for target %s to be ONLINE…",
                     i, len(plan), target)
            if not wait_node_online(args.agent_url, target, timeout=60):
                return _abort(f"target {target} did not reach ONLINE within 60s")

            phase.set(f"switchover-{i}-to-{target}")
            log.info("[switchover %d/%d] Triggering %s → %s",
                     i, len(plan), current_primary, target)
            sw_start_ts = time.time()
            try:
                WORKLOAD_PAUSE.set()
                try:
                    # Let in-flight Bolt work finish before HAProxy/agent tear down pools.
                    time.sleep(0.25)
                    trigger_switchover(args.agent_url, args.admin_token, target)
                    wait_for_primary(args.agent_url, target, timeout=30)
                finally:
                    WORKLOAD_PAUSE.clear()
            except Exception as e:
                switchover_events.append({
                    "index": i, "from": current_primary, "to": target,
                    "start_ts": sw_start_ts, "duration_s": time.time() - sw_start_ts,
                    "ok": False, "error": str(e),
                })
                return _abort(f"switchover #{i} to {target} failed: {e}")

            sw_dur = time.time() - sw_start_ts
            switchover_events.append({
                "index": i, "from": current_primary, "to": target,
                "start_ts": sw_start_ts, "duration_s": round(sw_dur, 3), "ok": True,
            })
            log.info("[switchover %d/%d] Complete in %.2fs  (primary: %s → %s)",
                     i, len(plan), sw_dur, current_primary, target)
            current_primary = target

            # Steady window AFTER this switchover (workload stays on)
            phase.set(f"steady-{i}")
            log.info("[switchover %d/%d] Steady load for %ds before next switchover",
                     i, len(plan), args.interval_seconds)
            time.sleep(args.interval_seconds)

        if current_primary != initial_primary:
            log.warning("Rotation plan ended on %s, expected initial primary %s",
                        current_primary, initial_primary)
        else:
            log.info("Rotation complete, cluster back to initial primary %s",
                     current_primary)

    # --- Stop workload -----------------------------------------------------
    phase.set("stopping")
    log.info("Stopping workload after %.1fs of sustained load…", time.time() - t0)
    STOP.set()
    executor.shutdown(wait=True)

    # --- Phase 4: let replicas catch up -----------------------------------
    log.info("Waiting %ds for replicas to catch up before integrity check…",
             args.post_quiet_seconds)
    time.sleep(args.post_quiet_seconds)
    wait_all_online(args.agent_url, timeout=60)

    expected_seqs = stats.expected_present_seqs()           # written - deleted
    if repl_sync_timeout > 0:
        log.info("Polling Neo4j instances for replication sync (timeout=%.0fs, expect %d nodes)…",
                 repl_sync_timeout, len(expected_seqs))
        wait_testnode_writer_sync(
            DEFAULT_NODES, args.user, args.password, writer_id,
            len(expected_seqs), repl_sync_timeout,
        )
        # Node count agreeing does NOT imply rels have finished replicating:
        # under load the SyncApplier has separate queues for nodes vs. rels,
        # and REL_MERGE events can trail TestNode creates by several seconds
        # (especially right after a switchover with PEL replay). Without this
        # second wait, the integrity check flags phantom `rel_miss` that would
        # have cleared a few seconds later — masking whether the residual is
        # real HA data loss or just replication lag.
        log.info("Polling Neo4j instances for rel-count convergence (timeout=%.0fs)…",
                 repl_sync_timeout)
        wait_rel_count_converged(
            DEFAULT_NODES, args.user, args.password, writer_id, repl_sync_timeout,
        )

    # --- Phase 5: integrity check -----------------------------------------
    log.info("Dumping per-node TestNode sets for integrity check…")
    expected_deleted = stats.deleted_seqs.copy()
    expected_rel_pairs = stats.rel_pairs.copy()
    # NOTE: a relationship `(a, b)` survives iff BOTH endpoint nodes still exist.
    # We can't determine survival just from `expected_deleted` for two reasons:
    #   1. The writer/rel/delete loops race against each other on the primary.
    #      A rel can be MERGE'd against a node that delete_loop deletes a few
    #      milliseconds later — the rel is then DETACH-removed by Neo4j and never
    #      truly existed at "rest", but it does sit in `rel_pairs` because the
    #      MERGE returned ok=true.
    #   2. delete_loop's DETACH cascade reaches BOTH `(a, b)` and `(b, c)` when
    #      it removes node `b`, but `expected_deleted` only carries `b`.
    # The integrity ground truth therefore has to be computed PER-NODE using the
    # node set that was actually observed on that Neo4j replica (`seqs`). We do
    # this inside the per-node loop below; here we expose `expected_rel_pairs`
    # as the union of every successful MERGE so that observation can be derived.
    per_node = {}
    for nid, uri in DEFAULT_NODES.items():
        try:
            per_node[nid] = dump_node_state(nid, uri, args.user, args.password,
                                             writer_id=writer_id)
            per_node[nid]["uri"] = uri
        except Exception as e:
            per_node[nid] = {"error": f"{type(e).__name__}: {e}", "uri": uri}

    # Cross-node community-distribution agreement: GDS writes the same community
    # ids on the primary, replication should land identical histograms on every
    # standby. Pick the first non-error node as the reference.
    community_reference: Optional[dict] = None
    community_reference_node: Optional[str] = None
    for nid, state in per_node.items():
        if "error" not in state and state.get("community_nodes", 0) > 0:
            community_reference = state["community_dist"]
            community_reference_node = nid
            break

    integrity_ok = True
    integrity_details = {}
    for nid, state in per_node.items():
        if "error" in state:
            log.error("  %-8s  %s : %s", nid, state.get("uri"), state["error"])
            integrity_ok = False
            integrity_details[nid] = {"ok": False, "error": state["error"]}
            continue

        seqs = state["seqs"]
        count_current = state["count_current_run"]
        # Surviving (expected_seqs) must all be present; deleted ones must NOT be.
        missing = sorted(expected_seqs - seqs)
        extra = sorted(seqs - expected_seqs - stats.failed_seqs)
        # Deletes that didn't propagate to this node = nodes still present that
        # we successfully deleted on the primary.
        delete_leaks = sorted(expected_deleted & seqs)

        # Count check: the node count for THIS run must equal the number of
        # surviving (written - deleted) seqs.
        count_matches = (count_current == len(expected_seqs))

        # Relationship integrity. The set of rels we expect on THIS node is the
        # subset of `expected_rel_pairs` whose BOTH endpoints are still present
        # in this node's TestNode snapshot (`seqs`). This naturally accounts for
        # both kinds of legitimate disappearance:
        #   - an endpoint was DETACH DELETE'd by delete_loop
        #   - an endpoint was deleted concurrently with the MERGE, so the rel
        #     was a transient casualty that never reached at-rest state
        # so any remaining `rel_missing` is a genuine sync gap.
        rel_pairs = state.get("rel_pairs", set())
        expected_rel_pairs_here = {
            (a, b) for (a, b) in expected_rel_pairs
            if a in seqs and b in seqs
        }
        rel_missing = sorted(expected_rel_pairs_here - rel_pairs)
        # `extra` rels can be "stragglers" — pairs we created but whose endpoint
        # was deleted concurrently and the relationship was the casualty. Don't
        # count those as a failure as long as both endpoints still exist.
        rel_extra = sorted({
            (a, b) for (a, b) in rel_pairs - expected_rel_pairs_here
            if a in seqs and b in seqs
        })

        # Community-distribution agreement
        comm_dist = state.get("community_dist", {})
        comm_match: Optional[bool] = None
        if community_reference is not None:
            comm_match = (comm_dist == community_reference)

        details = {
            "ok": (not missing and not extra and count_matches
                   and not delete_leaks
                   and not rel_missing and not rel_extra
                   and (comm_match is not False)),
            "count_total": state["count"],
            "count_current_run": count_current,
            "missing_count": len(missing),
            "extra_count": len(extra),
            "missing_sample": missing[:10],
            "extra_sample": extra[:10],
            "delete_leak_count": len(delete_leaks),
            "delete_leak_sample": delete_leaks[:10],
            "rel_count": state["rel_count"],
            "rel_missing_count": len(rel_missing),
            "rel_extra_count": len(rel_extra),
            "rel_missing_sample": rel_missing[:10],
            "rel_extra_sample": rel_extra[:10],
            "community_nodes": state["community_nodes"],
            "community_distinct": len(comm_dist),
            "community_matches_reference": comm_match,
            "stale_count": state["stale_count"],
            "stale_writers": state["stale_writers"],
        }
        integrity_details[nid] = details

        if state["stale_count"]:
            log.warning("  %-8s  found %d residual TestNode(s) from previous run(s) "
                        "by writer_id(s) %s (not counted against this run)",
                        nid, state["stale_count"], state["stale_writers"])

        if not details["ok"]:
            integrity_ok = False
            log.error("  %-8s  FAIL  count=%d (exp=%d)  miss=%d  extra=%d  "
                      "delete_leak=%d  rel_miss=%d  rel_extra=%d  comm_match=%s",
                      nid, count_current, len(expected_seqs),
                      len(missing), len(extra), len(delete_leaks),
                      len(rel_missing), len(rel_extra), comm_match)
        else:
            log.info("  %-8s  OK  nodes=%d  rels=%d  community_nodes=%d/%d_distinct  comm_match=%s",
                     nid, count_current, state["rel_count"],
                     state["community_nodes"], len(comm_dist),
                     comm_match if comm_match is not None else "n/a")

    # --- Final report -----------------------------------------------------
    write_summary  = summarize(stats.writes)
    read_summary   = summarize(stats.reads)
    rel_summary    = summarize(stats.rels)
    delete_summary = summarize(stats.deletes)
    gds_summary    = summarize(stats.gds_runs)
    final_primary = None
    try:
        final_primary = get_cluster_status(args.agent_url).get("primaryNode")
    except Exception:
        pass

    report = {
        "initial_primary": initial_primary,
        "final_primary": final_primary,
        "returned_to_initial": (final_primary == initial_primary),
        "rotations_requested": args.rotations,
        "switchover_events": switchover_events,
        "write_summary": write_summary,
        "read_summary":  read_summary,
        "rel_summary": rel_summary,
        "delete_summary": delete_summary,
        "gds_summary": gds_summary,
        "expected_written_count": len(stats.written_seqs),
        "expected_present_count": len(expected_seqs),
        "deleted_count": len(expected_deleted),
        "failed_write_count": len(stats.failed_seqs),
        "expected_rel_pairs": len(expected_rel_pairs),
        "gds_community_runs": stats.gds_community_count,
        "community_reference_node": community_reference_node,
        "integrity_ok": integrity_ok,
        "per_node": integrity_details,
    }
    with open(args.report_file, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info("Report written to %s", args.report_file)

    # --- Pretty summary ---------------------------------------------------
    print()
    print("=" * 72)
    print("WRITE SUMMARY")
    print(json.dumps(write_summary, indent=2))
    print()
    print("READ SUMMARY")
    print(json.dumps(read_summary, indent=2))
    print()
    if rel_summary.get("count", 0) > 0:
        print("RELATIONSHIP SUMMARY")
        print(json.dumps(rel_summary, indent=2))
        print()
    if delete_summary.get("count", 0) > 0:
        print("DELETE SUMMARY")
        print(json.dumps(delete_summary, indent=2))
        print()
    if gds_summary.get("count", 0) > 0:
        print("GDS SUMMARY")
        print(json.dumps(gds_summary, indent=2))
        print()
    if switchover_events:
        print("SWITCHOVERS")
        for ev in switchover_events:
            status_tag = "OK" if ev.get("ok") else f"FAIL ({ev.get('error', '?')})"
            print(f"  #{ev['index']:>2d}  {ev['from']} → {ev['to']}  "
                  f"{ev['duration_s']}s  {status_tag}")
        print()

    print("INTEGRITY")
    print(f"  writer_id:                 {writer_id}")
    print(f"  initial primary:           {initial_primary}")
    print(f"  final primary:             {final_primary}  "
          f"({'returned to initial' if final_primary == initial_primary else 'DID NOT return'})")
    print(f"  written / deleted / kept:  "
          f"{len(stats.written_seqs)} / {len(expected_deleted)} / {len(expected_seqs)}")
    print(f"  failed writes:             {len(stats.failed_seqs)}")
    print(f"  rels created (in run):     {len(expected_rel_pairs)} "
          f"(expected per node depends on which endpoints survived; see per-node lines)")
    print(f"  gds louvain runs:          {stats.gds_community_count}"
          + (f"  (reference node: {community_reference_node})" if community_reference_node else ""))
    for nid, d in integrity_details.items():
        if d.get("ok"):
            extra_note = ""
            if d.get("stale_count"):
                extra_note = f"  [+{d['stale_count']} residual from prior runs]"
            comm = d.get("community_matches_reference")
            comm_tag = (
                "n/a" if comm is None else ("=ref" if comm else "DIFFERS")
            )
            print(f"  {nid}: OK  nodes={d['count_current_run']}  "
                  f"rels={d['rel_count']}  community={d['community_nodes']}/"
                  f"{d['community_distinct']}d  comm_match={comm_tag}{extra_note}")
        elif "error" in d:
            print(f"  {nid}: ERROR — {d['error']}")
        else:
            print(f"  {nid}: FAIL  nodes={d['count_current_run']}  "
                  f"miss={d['missing_count']}  extra={d['extra_count']}  "
                  f"delete_leak={d.get('delete_leak_count', 0)}  "
                  f"rels={d['rel_count']}  rel_miss={d.get('rel_missing_count', 0)}  "
                  f"rel_extra={d.get('rel_extra_count', 0)}  "
                  f"comm_match={d.get('community_matches_reference')}")
    print("=" * 72)

    # --- Verdict ----------------------------------------------------------
    error_rate = write_summary.get("error_rate", 0.0)
    if error_rate > args.error_budget:
        log.error("Write error rate %.3f exceeds budget %.3f",
                  error_rate, args.error_budget)
        return 2
    if not integrity_ok:
        log.error("Integrity check failed")
        return 1

    log.info("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
