#!/usr/bin/env python3
"""Independently check Money Graph inputs, exports and embedded viewer data.

The Markdown route uses only the standard library. No pipeline functions are
imported: raw aggregation, directed centralities, role rules and priority are
recomputed here. Community membership is checked for consistency and connectivity;
this verifier does not claim that a Louvain partition is globally optimal.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import heapq
import json
import math
import os
import random
import re
import stat
import sys
import tempfile
import time
from collections import Counter, defaultdict
from decimal import Decimal
from html.parser import HTMLParser
from numbers import Integral
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
SCHEMAS = {
    "nodes": ("gid", "depth", "is_seed"),
    "edges": ("src", "dst", "sum_kzt", "n_tx", "depth"),
    "transactions": ("src", "dst", "date", "sum_kzt"),
    "nodes_roles": ("gid", "role", "role_score", "cluster_id", "priority_score", "evidence"),
    "clusters": ("cluster_id", "n_nodes", "n_seed", "sum_kzt_internal", "top_gids", "hypothesis"),
    "top_nodes": ("rank", "gid", "role", "priority_score", "why"),
}
ROLES = {"consolidator", "transit", "distributor", "terminal", "coordinator", "peripheral"}


class VerificationError(ValueError):
    """An input or generated artifact disagrees with the public contract."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def integer(value: Any, label: str) -> int:
    require(not isinstance(value, bool), f"{label}: boolean is not an integer identifier/count")
    require(isinstance(value, (str, Integral)), f"{label}: integer required, got {type(value).__name__}")
    spelling = str(value).strip()
    require(re.fullmatch(r"[+-]?[0-9]+", spelling) is not None, f"{label}: invalid integer {value!r}")
    number = int(spelling)
    require(-(2**63) <= number < 2**63, f"{label}: outside signed int64 range")
    return number


def number(value: Any, label: str, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise VerificationError(f"{label}: invalid number") from exc
    require(result.is_finite(), f"{label}: nonfinite value")
    require(not positive or result > 0, f"{label}: value must be positive")
    return result


def read_markdown(path: Path, expected: tuple[str, ...]) -> list[dict[str, str]]:
    lines = [line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    require(len(lines) >= 2, f"{path.name}: missing Markdown table")
    cells = [[v.strip() for v in line.strip("|").split("|")] for line in lines]
    columns = cells[0]
    require(set(columns) == set(expected) and len(columns) == len(expected), f"{path.name}: unexpected schema {columns}")
    require(all(re.fullmatch(r":?-+:?", v) for v in cells[1]), f"{path.name}: invalid separator")
    records = []
    for line, values in enumerate(cells[2:], 3):
        require(len(values) == len(expected) and all(values), f"{path.name}:{line}: malformed/empty cells")
        records.append(dict(zip(columns, values)))
    require(bool(records), f"{path.name}: empty input")
    return records


def read_csv(path: Path, expected: tuple[str, ...]) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        require(tuple(reader.fieldnames or ()) == expected, f"{path.name}: unexpected CSV headers {reader.fieldnames}")
        rows = list(reader)
    require(bool(rows), f"{path.name}: empty output")
    for line, row in enumerate(rows, 2):
        require(set(row) == set(expected) and all(row.get(k) and row[k].strip() for k in expected),
                f"{path.name}:{line}: extra/missing/null cell")
    return rows


def load_raw(data_dir: Path) -> tuple[dict[str, list[dict[str, Any]]], list[Path]]:
    names = ("nodes", "edges", "transactions")
    if all((data_dir / f"{name}.md").is_file() for name in names):
        paths = [data_dir / f"{name}.md" for name in names]
        tables = {name: read_markdown(path, SCHEMAS[name]) for name, path in zip(names, paths)}
    elif all((data_dir / f"{name}.parquet").is_file() for name in names):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise VerificationError("Parquet verification needs pyarrow; install requirements.txt") from exc
        paths = [data_dir / f"{name}.parquet" for name in names]
        tables = {}
        for name, path in zip(names, paths):
            schema = pq.read_schema(path)
            require(set(schema.names) == set(SCHEMAS[name]) and len(schema.names) == len(SCHEMAS[name]),
                    f"{path.name}: unexpected schema")
            for field in schema:
                if field.name in ("gid", "src", "dst", "n_tx"):
                    require(pa.types.is_int64(field.type), f"{path.name}.{field.name}: expected signed int64")
                elif field.name == "depth":
                    require(pa.types.is_signed_integer(field.type), f"{path.name}.depth: expected signed integer")
                elif field.name == "sum_kzt":
                    require(pa.types.is_float64(field.type), f"{path.name}.sum_kzt: expected float64")
                elif field.name == "is_seed":
                    require(pa.types.is_boolean(field.type), f"{path.name}.is_seed: expected boolean")
                elif field.name == "date":
                    require(pa.types.is_date(field.type) or (pa.types.is_timestamp(field.type) and field.type.tz is None),
                            f"{path.name}.date: expected calendar date/naive timestamp")
            frame = pq.read_table(path, columns=list(SCHEMAS[name]))
            require(len(frame) > 0, f"{path.name}: empty input")
            tables[name] = frame.to_pylist()
    else:
        raise VerificationError("Provide a complete set of three Markdown or three Parquet input tables")
    for name, rows in tables.items():
        for row in rows:
            for key in ("gid",) if name == "nodes" else ("src", "dst"):
                row[key] = integer(row[key], f"{name}.{key}")
            if "depth" in row:
                row["depth"] = integer(row["depth"], f"{name}.depth")
                require((0 if name == "nodes" else 1) <= row["depth"] <= 4, f"{name}: invalid depth")
            if name == "nodes":
                value = str(row["is_seed"]).strip().lower()
                require(value in ("true", "false", "1", "0", "yes", "no"), "nodes: invalid seed flag")
                row["is_seed"] = value in ("true", "1", "yes")
                require(row["is_seed"] == (row["depth"] == 0), "nodes: depth/seed disagreement")
            else:
                row["sum_kzt"] = number(row["sum_kzt"], f"{name}.sum_kzt", positive=True)
                require(math.isfinite(float(row["sum_kzt"])), f"{name}: amount outside finite floating range")
            if name == "edges":
                row["n_tx"] = integer(row["n_tx"], "edges.n_tx")
                require(row["n_tx"] > 0, "edges: nonpositive transaction count")
            if name == "transactions":
                value = row["date"]
                if isinstance(value, dt.datetime):
                    require(value.tzinfo is None and value.time() == dt.time(), "transactions: timestamp must be naive midnight")
                    value = value.date()
                if isinstance(value, dt.date):
                    value = value.isoformat()
                require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is not None,
                        "transactions: date must be an ISO calendar date")
                try:
                    date = dt.date.fromisoformat(value)
                except ValueError as exc:
                    raise VerificationError("transactions: invalid calendar date") from exc
                require((date.year, date.month) == (2026, 7), "transactions: date outside July 2026")
                require(row["sum_kzt"] >= 5000, "transactions: value below export threshold")
    return tables, paths


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    left = int((len(ordered) - 1) * q)
    fraction = (len(ordered) - 1) * q - left
    return ordered[left] * (1 - fraction) + ordered[min(left + 1, len(ordered) - 1)] * fraction


def ranks(values: list[float]) -> list[float]:
    if len(values) == 1:
        return [0.5]
    positions = defaultdict(list)
    for position, value in enumerate(sorted(values)):
        positions[value].append(position)
    lookup = {value: (spots[0] + spots[-1]) / 2 / (len(values) - 1) for value, spots in positions.items()}
    return [lookup[value] for value in values]


def directed_betweenness(adjacency: list[list[tuple[int, float]]]) -> list[float]:
    """Dijkstra distances followed by an independently constructed path DAG."""
    n = len(adjacency)
    result = [0.0] * n
    if n < 3:
        return result
    costs = [[(v, 1 / math.log1p(amount)) for v, amount in links] for links in adjacency]
    for source in range(n):
        distance = {source: 0.0}
        queue = [(0.0, source)]
        while queue:
            cost, u = heapq.heappop(queue)
            if cost != distance[u]:
                continue
            for v, weight in costs[u]:
                candidate = cost + weight
                if candidate < distance.get(v, math.inf):
                    distance[v] = candidate
                    heapq.heappush(queue, (candidate, v))
        order = sorted(distance, key=lambda v: (distance[v], v))
        predecessors = {v: [] for v in order}
        paths = dict.fromkeys(order, 0.0)
        paths[source] = 1.0
        for u in order:
            for v, weight in costs[u]:
                if abs(distance[u] + weight - distance[v]) <= 1e-12:
                    predecessors[v].append(u)
                    paths[v] += paths[u]
        dependency = dict.fromkeys(order, 0.0)
        for v in reversed(order):
            for u in predecessors[v]:
                dependency[u] += paths[u] / paths[v] * (1 + dependency[v])
            if v != source:
                result[v] += dependency[v]
    return [v / ((n - 1) * (n - 2)) for v in result]


def expected_role(f: dict[str, Any], t: dict[str, float]) -> tuple[str, float, str]:
    indeg, outdeg = f["in_deg"], f["out_deg"]
    if not outdeg and f["depth"] == 4:
        return "terminal", .45, "Depth 4 and zero observed recipients: depth cutoff may hide further flows."
    if not outdeg and indeg:
        return "terminal", .50 if f["is_seed"] else .82, "Positive incoming flow and zero observed recipients before depth 4."
    if indeg >= max(1, t["in_deg_p75"]) and outdeg >= max(1, t["out_deg_p75"]) and f["betweenness"] > 0 and f["betweenness"] >= t["betweenness_p75_positive"]:
        return "coordinator", .76, "In/out degree meet P75 and positive directed betweenness meets its positive-value P75."
    if not f["is_seed"] and indeg >= max(1, t["in_deg_p90"]) and f["pass_through"] is not None and f["pass_through"] < .5:
        return "consolidator", .72, "Nonseed: incoming degree meets P90 and recorded outgoing/incoming value is below 0.50."
    if f["is_seed"] and indeg >= max(1, t["in_deg_p90"]) and f["pagerank"] >= t["pagerank_p75"]:
        return "consolidator", .62, "Seed exception: incoming degree meets P90 and PageRank meets P75; no balance ratio used."
    if not f["is_seed"] and indeg >= max(1, t["in_deg_p75"]) and f["pass_through"] is not None and .7 <= f["pass_through"] <= 1.3:
        return "transit", .68, "Nonseed: incoming degree meets P75 and recorded outgoing/incoming value is within [0.70, 1.30]."
    if outdeg >= max(1, t["out_deg_p90"]):
        return "distributor", .66, "Outgoing degree meets P90; all earlier rules were false."
    return "peripheral", .50, "No earlier role rule matches; zero-edge nodes have no observed flow evidence."


def recompute(tables: dict[str, list[dict[str, Any]]]) -> tuple[dict[int, dict[str, Any]], dict[str, float], dict[str, Any]]:
    nodes, edges, transactions = (tables[name] for name in ("nodes", "edges", "transactions"))
    gids = sorted(n["gid"] for n in nodes)
    require(len(gids) == len(set(gids)), "raw nodes: duplicate gid")
    node_map = {n["gid"]: n for n in nodes}
    edge_map = {(e["src"], e["dst"]): e for e in edges}
    require(len(edge_map) == len(edges), "raw edges: duplicate directed pair")
    require(all(e["src"] in node_map and e["dst"] in node_map for e in edges), "raw edges: unknown endpoint")
    total_amount = sum(float(e["sum_kzt"]) for e in edges)
    require(math.isfinite(total_amount) and math.isfinite(8.0 * total_amount * total_amount),
            "raw edges: aggregate outside supported finite numeric range")
    aggregates = defaultdict(lambda: [Decimal(0), 0])
    for row in transactions:
        entry = aggregates[row["src"], row["dst"]]
        entry[0] += row["sum_kzt"]
        entry[1] += 1
    require(set(aggregates) == set(edge_map), "raw transactions/edges: pair coverage mismatch")
    for pair, (amount, count) in aggregates.items():
        e = edge_map[pair]
        require(abs(amount - e["sum_kzt"]) <= Decimal("0.000001") and count == e["n_tx"],
                f"raw transaction aggregation mismatch for {pair}")
    incoming, outgoing = defaultdict(list), defaultdict(list)
    for e in edges:
        incoming[e["dst"]].append(e)
        outgoing[e["src"]].append(e)
    index = {gid: i for i, gid in enumerate(gids)}
    adjacency = [sorted((index[e["dst"]], float(e["sum_kzt"])) for e in outgoing[g]) for g in gids]
    pr = [1 / len(gids)] * len(gids)
    totals = [sum(amount for _, amount in links) for links in adjacency]
    for _ in range(200):
        base = .15 / len(gids) + .85 * sum(pr[i] for i, value in enumerate(totals) if value == 0) / len(gids)
        updated = [base] * len(gids)
        for i, links in enumerate(adjacency):
            if totals[i]:
                multiplier = .85 * pr[i] / totals[i]
                for j, amount in links:
                    updated[j] += multiplier * amount
        difference = sum(abs(a - b) for a, b in zip(pr, updated))
        pr = updated
        if difference < 1e-12:
            break
    between = directed_betweenness(adjacency)
    seeds = {n["gid"] for n in nodes if n["is_seed"]}
    features = {}
    for gid in gids:
        reached, frontier = {gid}, {gid}
        for _ in range(2):
            neighbors = {v for u in frontier for v in [e["src"] for e in incoming[u]] + [e["dst"] for e in outgoing[u]]}
            frontier = neighbors - reached
            reached |= frontier
        insum = sum((e["sum_kzt"] for e in incoming[gid]), Decimal(0))
        outsum = sum((e["sum_kzt"] for e in outgoing[gid]), Decimal(0))
        features[gid] = {**node_map[gid], "in_deg": len(incoming[gid]), "out_deg": len(outgoing[gid]),
                         "in_tx": sum(e["n_tx"] for e in incoming[gid]), "out_tx": sum(e["n_tx"] for e in outgoing[gid]),
                         "in_kzt": float(insum), "out_kzt": float(outsum), "pass_through": float(outsum / insum) if insum else None,
                         "pagerank": pr[index[gid]], "betweenness": between[index[gid]], "seed_reach": len(reached & seeds),
                         "incoming_gids": sorted(e["src"] for e in incoming[gid]), "outgoing_gids": sorted(e["dst"] for e in outgoing[gid]),
                         "truncated_by_depth": node_map[gid]["depth"] == 4 and not outgoing[gid]}
    thresholds = {f"{direction}_deg_p{p}": percentile([f[f"{direction}_deg"] for f in features.values()], p / 100)
                  for direction in ("in", "out") for p in (75, 90)}
    thresholds.update(betweenness_p75_positive=percentile([v for v in between if v > 0], .75), pagerank_p75=percentile(pr, .75))
    for field in ("pagerank", "betweenness", "in_deg", "out_deg", "seed_reach"):
        for gid, value in zip(gids, ranks([features[g][field] for g in gids])):
            features[gid][f"{field}_rank"] = value
    for f in features.values():
        f["role"], f["role_score"], f["rule"] = expected_role(f, thresholds)
        priority = 0.0
        for field, weight in (("pagerank", .30), ("in_deg", .25), ("betweenness", .25), ("out_deg", .10), ("seed_reach", .10)):
            priority += f[f"{field}_rank"] * weight
        f["priority_score"] = priority
    summary = {"node_count": len(nodes), "edge_count": len(edges), "transaction_count": len(transactions),
               "seed_count": len(seeds), "orphan_count": sum(not incoming[g] and not outgoing[g] for g in gids),
               "seed_without_outgoing": sum(not outgoing[g] for g in seeds), "edge_turnover": float(sum(e["sum_kzt"] for e in edges))}
    return features, thresholds, summary


def verify_evidence(row: dict[str, str], f: dict[str, Any]) -> None:
    text, role = row["evidence"], row["role"]
    require(0 < len(text) <= 200 and re.search(r"\d", text) is not None, f"gid {f['gid']}: invalid evidence length/numbers")
    required = [f"{f['in_deg']} payers/{f['in_tx']} tx"]
    if role in {"coordinator", "transit", "distributor", "peripheral"}:
        required.append(f"{f['out_deg']} recipients/{f['out_tx']} tx")
    if role == "terminal":
        required += [f"{f['in_kzt']:,.0f} KZT", f"hop {f['depth']}"]
        required.append("depth-cutoff artifact possible" if f["truncated_by_depth"] else "No captured outflow")
    elif role == "coordinator":
        required += [f"betweenness p{f['betweenness_rank'] * 100:.0f}", f"{f['seed_reach']} seeds within 2 hops"]
    elif role == "consolidator":
        required.append(f"{f['in_kzt']:,.0f} KZT")
        if not f["is_seed"]:
            required.append(f"forwarded {f['pass_through']:.0%}")
        else:
            required.append(f"{f['out_deg']} recipients")
    elif role == "transit":
        required.append(f"forwarded {f['pass_through']:.0%}")
    elif role == "distributor":
        required.append(f"{f['out_kzt']:,.0f} KZT out")
    elif role == "peripheral":
        required += [f"({f['in_kzt']:,.0f} KZT)", f"({f['out_kzt']:,.0f} KZT)", "no strong role signal"]
    if f["is_seed"]:
        required.append("seed inflow is understated")
    for phrase in required:
        require(phrase.lower() in text.lower(), f"gid {f['gid']}: evidence missing/wrong numeric claim: {phrase}")


class PayloadParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.active = False
        self.parts: list[str] = []
        self.found = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "script" and dict(attrs).get("id") == "payload":
            self.active = True
            self.found += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "script":
            self.active = False

    def handle_data(self, value: str) -> None:
        if self.active:
            self.parts.append(value)


def near(actual: Any, expected: float, label: str, tolerance: float = 1e-11) -> None:
    require(not isinstance(actual, bool), f"{label}: boolean used as number")
    value = float(number(actual, label))
    require(math.isclose(value, expected, rel_tol=1e-10, abs_tol=tolerance), f"{label}: got {value}, expected {expected}")


def verify(data_dir: Path, out_dir: Path, gid: int | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    tables, input_paths = load_raw(data_dir)
    features, thresholds, raw_summary = recompute(tables)
    outputs = {name: read_csv(out_dir / f"{name}.csv", SCHEMAS[name]) for name in ("nodes_roles", "clusters", "top_nodes")}
    roles, clusters, top = (outputs[name] for name in ("nodes_roles", "clusters", "top_nodes"))
    rows = {integer(r["gid"], "nodes_roles.gid"): r for r in roles}
    require(len(rows) == len(roles) == len(features) and set(rows) == set(features), "nodes_roles: missing/extra/duplicate gid")
    groups = defaultdict(list)
    for g, row in rows.items():
        f = features[g]
        require(row["role"] in ROLES and row["role"] == f["role"], f"gid {g}: role disagrees with recomputed rules")
        near(row["role_score"], f["role_score"], f"gid {g} role_score")
        for key in ("role_score", "priority_score"):
            require(0 <= number(row[key], key) <= 1, f"gid {g}: {key} outside [0,1]")
        near(row["priority_score"], f["priority_score"], f"gid {g} priority_score", 5.01e-7)
        verify_evidence(row, f)
        cid = integer(row["cluster_id"], "nodes_roles.cluster_id")
        f["cluster_id"] = cid
        groups[cid].append(g)
    cluster_rows = {integer(c["cluster_id"], "clusters.cluster_id"): c for c in clusters}
    require(len(cluster_rows) == len(clusters) and set(cluster_rows) == set(groups), "clusters: duplicate/orphan/missing ID")
    internal = defaultdict(Decimal)
    for e in tables["edges"]:
        cid = features[e["src"]]["cluster_id"]
        if cid == features[e["dst"]]["cluster_id"]:
            internal[cid] += e["sum_kzt"]
    for cid, members in groups.items():
        c = cluster_rows[cid]
        nseed = sum(features[g]["is_seed"] for g in members)
        require(integer(c["n_nodes"], "clusters.n_nodes") == len(members) and integer(c["n_seed"], "clusters.n_seed") == nseed,
                f"cluster {cid}: wrong size/seed count")
        near(c["sum_kzt_internal"], float(internal[cid]), f"cluster {cid} turnover", 1e-6)
        expected_top = sorted(members, key=lambda g: (-features[g]["priority_score"], g))[:5]
        require([integer(g, "clusters.top_gids") for g in c["top_gids"].split(";")] == expected_top, f"cluster {cid}: incorrect top_gids")
        counts = Counter(features[g]["role"] for g in members)
        structural = sum(counts[r] for r in ("consolidator", "coordinator", "transit", "distributor"))
        shape = ("seed-linked coordination/collection" if nseed and counts["consolidator"] + counts["coordinator"]
                 else "downstream aggregation/coordination" if counts["consolidator"] + counts["coordinator"]
                 else "fan-out distribution" if counts["distributor"] else "pass-through movement" if counts["transit"]
                 else "low-signal fragment")
        expected = f"Hypothesis: {shape}; {nseed}/{len(members)} seed, {structural} movement/hub signals, {internal[cid]:,.0f} KZT internal turnover. Verify from transactions."
        require(c["hypothesis"] == expected, f"cluster {cid}: hypothesis differs from measured composition/turnover")
        # Connected communities also ensure isolated nodes have singleton clusters.
        seen, frontier = {members[0]}, [members[0]]
        while frontier:
            u = frontier.pop()
            for v in features[u]["incoming_gids"] + features[u]["outgoing_gids"]:
                if features[v]["cluster_id"] == cid and v not in seen:
                    seen.add(v)
                    frontier.append(v)
        require(len(seen) == len(members), f"cluster {cid}: disconnected members/orphan incorrectly merged")
    if 0 < len(features) < 20:
        require(len(top) == len(features), "top_nodes: graphs with fewer than 20 nodes must include every node")
    else:
        require(len(top) >= 20, "top_nodes: fewer than 20 rows")
    order = sorted(features, key=lambda g: (-features[g]["priority_score"], g))[:len(top)]
    require([integer(r["gid"], "top_nodes.gid") for r in top] == order, "top_nodes: incorrect order/coverage/duplicate gid")
    for rank, row in enumerate(top, 1):
        g = integer(row["gid"], "top_nodes.gid")
        require(integer(row["rank"], "top_nodes.rank") == rank, "top_nodes: rank gap")
        require(row["role"] == rows[g]["role"] and row["priority_score"] == rows[g]["priority_score"], f"top gid {g}: CSV disagreement")
        require(row["why"] == f"Priority {features[g]['priority_score']:.2f}; {rows[g]['evidence']}", f"top gid {g}: wrong/empty explanation")
    parser = PayloadParser()
    parser.feed((out_dir / "index.html").read_text(encoding="utf-8"))
    require(parser.found == 1, "viewer: missing/duplicate JSON payload")
    payload = json.loads("".join(parser.parts), parse_constant=lambda value: (_ for _ in ()).throw(VerificationError(f"viewer: {value}")))
    pnodes = payload["nodes"]
    require(all(isinstance(n["gid"], str) for n in pnodes), "viewer: GIDs must be strings to preserve int64 precision")
    pgids = [integer(n["gid"], "viewer.gid") for n in pnodes]
    require(all(node["gid"] == str(g) for node, g in zip(pnodes, pgids)), "viewer: GID strings must use canonical decimal spelling for search")
    require(len(pgids) == len(set(pgids)) == len(features) and set(pgids) == set(features), "viewer: node coverage mismatch")
    for node, g in zip(pnodes, pgids):
        f, row = features[g], rows[g]
        for key in ("role", "evidence"):
            require(node[key] == row[key], f"viewer gid {g}: {key} differs from CSV")
        for key in ("depth", "is_seed", "in_deg", "out_deg", "in_tx", "out_tx", "seed_reach", "truncated_by_depth", "cluster_id"):
            require(node[key] == f[key], f"viewer gid {g}: wrong {key}")
        require(node["cluster"] == f["cluster_id"], f"viewer gid {g}: cluster alias mismatch")
        for key in ("in_kzt", "out_kzt", "pagerank", "betweenness", "pagerank_rank", "betweenness_rank", "in_deg_rank", "out_deg_rank", "seed_reach_rank", "priority_score", "role_score"):
            near(node[key], f[key], f"viewer gid {g} {key}", 1e-6 if key.endswith("_kzt") else 1e-11)
        near(node["priority"], f["priority_score"], f"viewer gid {g} priority alias")
        near(node["score"], f["role_score"], f"viewer gid {g} score alias")
        require((node["pass_through"] is None) == (f["pass_through"] is None), f"viewer gid {g}: pass-through null mismatch")
        if f["pass_through"] is not None:
            near(node["pass_through"], f["pass_through"], f"viewer gid {g} pass_through")
    edge_map = {(e["src"], e["dst"]): e for e in tables["edges"]}
    observed = set()
    for edge in payload["edges"]:
        u, v = integer(edge["src"], "viewer edge index"), integer(edge["dst"], "viewer edge index")
        require(0 <= u < len(pgids) and 0 <= v < len(pgids), "viewer: edge index out of bounds")
        pair = pgids[u], pgids[v]
        require(pair in edge_map and pair not in observed, "viewer: unknown/duplicate edge")
        observed.add(pair)
        near(edge["amount"], float(edge_map[pair]["sum_kzt"]), f"viewer edge {pair} amount", 1e-6)
        require(edge["n_tx"] == edge_map[pair]["n_tx"], f"viewer edge {pair}: transaction-count mismatch")
    require(observed == set(edge_map), "viewer: missing edges")
    require(set(payload["thresholds"]) == set(thresholds), "viewer: threshold names mismatch")
    for key, value in thresholds.items():
        near(payload["thresholds"][key], value, f"viewer threshold {key}")
    require(len(payload["clusters"]) == len(clusters)
            and {c["cluster_id"] for c in payload["clusters"]} == set(cluster_rows),
            "viewer: duplicate/missing cluster or count mismatch")
    for c in payload["clusters"]:
        require(c["cluster_id"] in cluster_rows, "viewer: unknown cluster")
        for key, value in cluster_rows[c["cluster_id"]].items():
            if key == "sum_kzt_internal":
                near(c[key], float(value), "viewer cluster turnover", 1e-6)
            else:
                require(str(c[key]) == value, f"viewer cluster {c['cluster_id']}: {key} differs from CSV")
    summary = json.loads((out_dir / "analysis_summary.json").read_text(encoding="utf-8"))
    for key, value in raw_summary.items():
        near(summary["validation"][key], value, f"analysis_summary {key}", 1e-6)
    for key, value in thresholds.items():
        near(summary["thresholds"][key], value, f"analysis_summary threshold {key}")
    counts = dict(Counter(row["role"] for row in roles))
    require(summary["role_counts"] == {role: counts.get(role, 0) for role in ROLES}, "analysis_summary: role distribution mismatch")
    require(summary["cluster_count"] == len(clusters), "analysis_summary: cluster count mismatch")
    require(summary["truncated_by_depth_count"] == sum(f["truncated_by_depth"] for f in features.values()), "analysis_summary: depth-cutoff count mismatch")
    sample = random.Random(20260923).sample(sorted(features), min(10, len(features)))
    paths = input_paths + [out_dir / name for name in ("nodes_roles.csv", "clusters.csv", "top_nodes.csv", "index.html", "analysis_summary.json")]
    report = {"status": "PASS", "checks": ["exact CSV schemas and complete node coverage", "raw edge/transaction aggregation", "independent directed metrics and documented role rules", "all evidence numeric claims and seed/depth caveats", "cluster coverage, connectivity, counts, turnover, top GIDs and hypotheses", "priority formula, ranking and explanations", "viewer int64 GIDs, metrics, direct edges and thresholds", "analysis summary consistency"],
              "raw_counts": raw_summary, "role_counts": counts, "cluster_count": len(clusters), "top_count": len(top),
              "max_evidence_length": max(len(r["evidence"]) for r in roles), "thresholds": thresholds,
              "sample_seed": 20260923, "sample_10": [{"gid": str(g), "evidence": rows[g]["evidence"], "raw_metrics": features[g]} for g in sample],
              "sha256": {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
              "elapsed_seconds": round(time.perf_counter() - started, 6)}
    if gid is not None:
        require(gid in features, f"Requested gid {gid} does not exist in input nodes")
        report["node_detail"] = {"gid": str(gid), "evidence": rows[gid]["evidence"], "raw_metrics": features[gid],
                                 "incoming_edges": [e for e in tables["edges"] if e["dst"] == gid],
                                 "outgoing_edges": [e for e in tables["edges"] if e["src"] == gid]}
    return report


def save_report(path: Path, report: dict[str, Any]) -> None:
    """Reports are restricted to project .audit/*.json, never data or outputs."""
    audit_root = ROOT / ".audit"
    target = path if path.is_absolute() else Path.cwd() / path
    target = Path(os.path.abspath(target))
    require(target.suffix.lower() == ".json" and target.is_relative_to(audit_root), "--report must be a JSON path inside the project's .audit directory")
    for current in [target, *target.parents]:
        if current.exists() or current.is_symlink():
            info = current.lstat()
            require(not current.is_symlink() and not getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400),
                    "--report path must not contain links or reparse points")
            if current == target:
                require(current.is_file() and info.st_nlink == 1, "--report target must be a regular file with one link")
        if current == ROOT:
            break
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=target.parent, suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(report, handle, indent=2, ensure_ascii=False, default=str, allow_nan=False)
            handle.write("\n")
        os.replace(temporary, target)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=ROOT / "data")
    parser.add_argument("--out", type=Path, default=ROOT / "out")
    parser.add_argument("--gid", type=lambda value: integer(value, "--gid"), help="print raw incoming/outgoing flows and the matched rule")
    parser.add_argument("--report", type=Path, help="optional JSON evidence path inside project .audit")
    args = parser.parse_args()
    try:
        report = verify(args.data, args.out, args.gid)
        if args.report:
            save_report(args.report, report)
        counts = report["raw_counts"]
        print(f"PASS: {counts['node_count']} nodes, {counts['edge_count']} edges, {counts['transaction_count']} transactions; {report['cluster_count']} clusters; top {report['top_count']}.")
        print("PASS: independent metrics/rules, exact CSV contracts, caveats, cluster totals, priority and viewer payload.")
        print(f"Random evidence sample (seed {report['sample_seed']}):")
        for row in report["sample_10"]:
            print(f"  {row['gid']}: {row['evidence']}")
        if "node_detail" in report:
            print(json.dumps(report["node_detail"], indent=2, ensure_ascii=False, default=str))
        print(f"Verified in {report['elapsed_seconds']:.3f}s.")
        return 0
    except (VerificationError, OSError, ValueError, KeyError, TypeError, IndexError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
