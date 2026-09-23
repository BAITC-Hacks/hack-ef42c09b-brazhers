#!/usr/bin/env python3
"""Explainable money-graph pipeline; standard-library only for Markdown inputs."""

from __future__ import annotations

import argparse
import csv
import heapq
import io
import json
import math
import os
import random
import re
import stat
import sys
import tempfile
import time
from collections import defaultdict, deque
from datetime import date, datetime
from numbers import Integral
from pathlib import Path
from typing import Any


ROLES = ("consolidator", "transit", "distributor", "terminal", "coordinator", "peripheral")
NODE_COLUMNS = ("gid", "role", "role_score", "cluster_id", "priority_score", "evidence")
CLUSTER_COLUMNS = ("cluster_id", "n_nodes", "n_seed", "sum_kzt_internal", "top_gids", "hypothesis")
TOP_COLUMNS = ("rank", "gid", "role", "priority_score", "why")
OUTPUT_NAMES = ("nodes_roles.csv", "clusters.csv", "top_nodes.csv", "index.html", "analysis_summary.json")
REQUIRED = {
    "edges": ("src", "dst", "sum_kzt", "n_tx", "depth"),
    "nodes": ("gid", "depth", "is_seed"),
    "transactions": ("src", "dst", "date", "sum_kzt"),
}
ROLE_COLORS = {
    "consolidator": "#ef8354",
    "transit": "#4f8fc0",
    "distributor": "#e7b94c",
    "terminal": "#8f79b8",
    "coordinator": "#d64b5a",
    "peripheral": "#91a4ad",
}


def read_markdown_table(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        raw_rows = list(csv.reader(handle, delimiter="|"))
    rows = [[cell.strip() for cell in row if cell.strip()] for row in raw_rows]
    rows = [row for row in rows if row and not all(re.fullmatch(r":?-+:?", cell) for cell in row)]
    if not rows:
        raise ValueError(f"No Markdown table found in {path}")
    columns = rows[0]
    if len(set(columns)) != len(columns):
        raise ValueError(f"{path}: duplicate column names")
    records = []
    for line_number, row in enumerate(rows[1:], start=3):
        if len(row) != len(columns):
            raise ValueError(f"{path}:{line_number}: expected {len(columns)} cells, found {len(row)}")
        records.append(dict(zip(columns, row)))
    return records


def read_parquet_tables(data_dir: Path) -> dict[str, list[dict[str, Any]]]:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Parquet input needs pyarrow. Install the pinned dependency with "
            "`python -m pip install -r requirements.txt`, or use the included Markdown exports."
        ) from exc
    tables = {}
    for name, required in REQUIRED.items():
        path = data_dir / f"{name}.parquet"
        schema = pq.read_schema(path)
        if len(set(schema.names)) != len(schema.names):
            raise ValueError(f"{name}.parquet contains duplicate column names")
        if set(schema.names) != set(required):
            raise ValueError(f"{name}.parquet must contain exactly these columns: {', '.join(required)}")
        for field in schema:
            kind = field.type
            if field.name in ("gid", "src", "dst", "n_tx"):
                valid = pa.types.is_int64(kind)
            elif field.name == "depth":
                valid = pa.types.is_signed_integer(kind)
            elif field.name == "is_seed":
                valid = pa.types.is_boolean(kind)
            elif field.name == "sum_kzt":
                valid = pa.types.is_float64(kind)
            else:  # date: timestamp exports are accepted only at naive midnight.
                valid = pa.types.is_date(kind) or (pa.types.is_timestamp(kind) and kind.tz is None)
            if not valid:
                raise ValueError(f"{name}.parquet has invalid type for {field.name}: {kind}")
        # Only the contracted fields enter the pipeline; no client attributes or
        # pandas extension metadata are materialized.
        tables[name] = pq.read_table(path, columns=list(required)).to_pylist()
    return tables


def load_tables(data_dir: Path) -> dict[str, list[dict[str, Any]]]:
    if all((data_dir / f"{name}.md").is_file() for name in REQUIRED):
        tables = {name: read_markdown_table(data_dir / f"{name}.md") for name in REQUIRED}
    elif all((data_dir / f"{name}.parquet").is_file() for name in REQUIRED):
        tables = read_parquet_tables(data_dir)
    else:
        missing_by_format = {
            extension: [f"{name}.{extension}" for name in REQUIRED if not (data_dir / f"{name}.{extension}").is_file()]
            for extension in ("md", "parquet")
        }
        details = "; ".join(f"{extension} missing: {', '.join(names)}" for extension, names in missing_by_format.items())
        raise FileNotFoundError(f"Provide one complete set of three .md or three .parquet tables in {data_dir}; mixed formats are not combined. {details}")
    for name, required in REQUIRED.items():
        if not tables[name]:
            raise ValueError(f"{name} table is empty")
        for row_number, row in enumerate(tables[name], start=1):
            actual = set(row)
            absent = set(required) - actual
            extra = actual - set(required)
            if absent or extra:
                raise ValueError(
                    f"{name} row {row_number} schema mismatch; "
                    f"missing: {', '.join(sorted(absent)) or 'none'}; "
                    f"unexpected: {', '.join(sorted(map(str, extra))) or 'none'}"
                )
    return tables


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if str(value).strip().lower() in ("true", "1", "yes"):
        return True
    if str(value).strip().lower() in ("false", "0", "no"):
        return False
    raise ValueError(f"Cannot parse boolean value: {value!r}")


def exact_int64(value: Any, field: str) -> int:
    """Reject floating IDs/counts instead of silently truncating or losing identity."""
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an exact integer, not a boolean")
    if isinstance(value, Integral):
        result = int(value)
    elif isinstance(value, str) and re.fullmatch(r"[+-]?[0-9]+", value.strip()):
        result = int(value.strip())
    else:
        raise ValueError(f"{field} must be an exact integer: {value!r}")
    if not -(2**63) <= result < 2**63:
        raise ValueError(f"{field} is outside signed int64: {value!r}")
    return result


def transaction_date(value: Any) -> str:
    """Accept native Parquet dates or canonical ISO dates in the dataset month."""
    if isinstance(value, datetime):
        if value.time().isoformat() != "00:00:00" or value.tzinfo is not None:
            raise ValueError(f"Transaction date must be a date without time/timezone: {value!r}")
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"Invalid transaction date: {value!r}") from exc
    else:
        raise ValueError(f"Transaction date must use YYYY-MM-DD: {value!r}")
    if not date(2026, 7, 1) <= parsed <= date(2026, 7, 31):
        raise ValueError(f"Transaction date is outside July 2026: {value!r}")
    return parsed.isoformat()


def monetary_amount(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a monetary amount, not a boolean")
    try:
        amount = float(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc
    if not math.isfinite(amount) or amount <= 0:
        raise ValueError(f"{field} must be finite and positive: {value!r}")
    return amount


def normalize_tables(tables: dict[str, list[dict[str, Any]]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    nodes = [
        {"gid": exact_int64(row["gid"], "nodes.gid"), "depth": exact_int64(row["depth"], "nodes.depth"), "is_seed": as_bool(row["is_seed"])}
        for row in tables["nodes"]
    ]
    edges = [
        {
            "src": exact_int64(row["src"], "edges.src"),
            "dst": exact_int64(row["dst"], "edges.dst"),
            "sum_kzt": monetary_amount(row["sum_kzt"], "edges.sum_kzt"),
            "n_tx": exact_int64(row["n_tx"], "edges.n_tx"),
            "depth": exact_int64(row["depth"], "edges.depth"),
        }
        for row in tables["edges"]
    ]
    tx = [
        {
            "src": exact_int64(row["src"], "transactions.src"),
            "dst": exact_int64(row["dst"], "transactions.dst"),
            "date": transaction_date(row["date"]),
            "sum_kzt": monetary_amount(row["sum_kzt"], "transactions.sum_kzt"),
        }
        for row in tables["transactions"]
    ]
    return nodes, edges, tx


def validate_inputs(nodes: list[dict[str, Any]], edges: list[dict[str, Any]], tx: list[dict[str, Any]]) -> dict[str, Any]:
    node_by_gid = {row["gid"]: row for row in nodes}
    if len(node_by_gid) != len(nodes):
        raise ValueError("nodes contains duplicate gids")
    edge_pairs = [(row["src"], row["dst"]) for row in edges]
    if len(set(edge_pairs)) != len(edge_pairs):
        raise ValueError("edges contains duplicate payer/payee pairs")
    node_ids = set(node_by_gid)
    endpoints = {gid for row in edges for gid in (row["src"], row["dst"])}
    unknown = endpoints - node_ids
    if unknown:
        raise ValueError(f"{len(unknown)} edge endpoint(s) are missing from nodes; first: {min(unknown)}")
    for row in edges:
        if not math.isfinite(row["sum_kzt"]) or row["sum_kzt"] <= 0 or row["n_tx"] < 1:
            raise ValueError(f"Invalid edge amount or transaction count: {row}")
        if not 1 <= row["depth"] <= 4:
            raise ValueError(f"Edge depth must be between 1 and 4: {row}")
    for row in nodes:
        if not 0 <= row["depth"] <= 4 or row["is_seed"] != (row["depth"] == 0):
            raise ValueError(f"Node depth and seed flag are inconsistent: {row}")

    # All amounts are positive, so a safe graph total also bounds every node and
    # community subtotal. Louvain's existing gain formula squares these totals.
    edge_turnover = sum(row["sum_kzt"] for row in edges)
    if not math.isfinite(edge_turnover) or not math.isfinite(8.0 * edge_turnover * edge_turnover):
        raise ValueError("Aggregate edge amounts exceed the supported finite numeric range")

    agg: dict[tuple[int, int], list[float | int]] = defaultdict(lambda: [0.0, 0])
    for row in tx:
        transaction_date(row["date"])
        if not math.isfinite(row["sum_kzt"]) or row["sum_kzt"] < 5000:
            raise ValueError(f"Transaction amount must be finite and at least 5,000 KZT: {row}")
        key = (row["src"], row["dst"])
        agg[key][0] += row["sum_kzt"]
        if not math.isfinite(agg[key][0]):
            raise ValueError(f"Aggregate transaction amount exceeds the finite numeric range for {key}")
        agg[key][1] += 1
    edge_map = {(row["src"], row["dst"]): row for row in edges}
    if set(agg) != set(edge_map):
        raise ValueError(
            f"transactions/edges pair mismatch: {len(set(agg) - set(edge_map))} transaction-only, "
            f"{len(set(edge_map) - set(agg))} edge-only"
        )
    max_diff = 0.0
    for pair, (amount, count) in agg.items():
        edge = edge_map[pair]
        diff = abs(amount - edge["sum_kzt"])
        max_diff = max(max_diff, diff)
        if not math.isclose(amount, edge["sum_kzt"], rel_tol=1e-10, abs_tol=1e-6):
            raise ValueError(f"Transaction sum does not match edge {pair}: {amount} vs {edge['sum_kzt']}")
        if int(count) != edge["n_tx"]:
            raise ValueError(f"Transaction count does not match edge {pair}: {count} vs {edge['n_tx']}")
    seed_ids = {row["gid"] for row in nodes if row["is_seed"]}
    orphan_ids = node_ids - endpoints
    components = weak_components(node_ids, edges)
    nontrivial_components = sum(1 for component in components if any(gid in endpoints for gid in component))
    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "transaction_count": len(tx),
        "seed_count": len(seed_ids),
        "orphan_count": len(orphan_ids),
        "orphan_seed_count": len(orphan_ids & seed_ids),
        "seed_without_outgoing": sum(1 for gid in seed_ids if not any(e["src"] == gid for e in edges)),
        "edge_turnover": edge_turnover,
        "weak_component_count_including_orphans": len(components),
        "weak_component_count_with_edges": nontrivial_components,
        "max_transaction_aggregate_diff_kzt": max_diff,
    }


def weak_components(node_ids: set[int], edges: list[dict[str, Any]]) -> list[set[int]]:
    adj: dict[int, set[int]] = {gid: set() for gid in node_ids}
    for row in edges:
        u, v = row["src"], row["dst"]
        adj[u].add(v)
        adj[v].add(u)
    unseen = set(node_ids)
    components = []
    while unseen:
        root = min(unseen)
        unseen.remove(root)
        component = {root}
        queue = [root]
        while queue:
            node = queue.pop()
            for neighbor in adj[node]:
                if neighbor in unseen:
                    unseen.remove(neighbor)
                    component.add(neighbor)
                    queue.append(neighbor)
        components.append(component)
    return components


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def percentile_ranks(values: list[float]) -> list[float]:
    """Average empirical percentile rank, keeping ties together and results in [0, 1]."""
    n = len(values)
    if n <= 1:
        return [0.5] * n
    order = sorted(range(n), key=lambda i: (values[i], i))
    result = [0.0] * n
    start = 0
    while start < n:
        end = start + 1
        while end < n and values[order[end]] == values[order[start]]:
            end += 1
        rank = ((start + end - 1) / 2) / (n - 1)
        for k in range(start, end):
            result[order[k]] = rank
        start = end
    return result


def pagerank(n: int, out_adj: list[list[tuple[int, float, int]]], out_kzt: list[float], damping: float = 0.85) -> list[float]:
    if n == 0:
        return []
    rank = [1.0 / n] * n
    for _ in range(200):
        dangling = sum(rank[i] for i in range(n) if out_kzt[i] == 0.0) / n
        new = [(1.0 - damping) / n + damping * dangling] * n
        for src, links in enumerate(out_adj):
            if out_kzt[src] == 0.0:
                continue
            scale = damping * rank[src] / out_kzt[src]
            for dst, amount, _ in links:
                new[dst] += scale * amount
        error = sum(abs(new[i] - rank[i]) for i in range(n))
        rank = new
        if error < 1e-12:
            break
    return rank


def weighted_directed_betweenness(
    n: int, out_adj: list[list[tuple[int, float, int]]]
) -> list[float]:
    """Exact directed Brandes betweenness; transfer strength is converted to path cost."""
    centrality = [0.0] * n
    if n < 3:
        return centrality
    for source in range(n):
        distances = [math.inf] * n
        sigma = [0.0] * n
        predecessors: list[list[int]] = [[] for _ in range(n)]
        distances[source] = 0.0
        sigma[source] = 1.0
        heap = [(0.0, source)]
        order = []
        while heap:
            dist_v, v = heapq.heappop(heap)
            if dist_v > distances[v] + 1e-12:
                continue
            order.append(v)
            for w, amount, _ in out_adj[v]:
                candidate = dist_v + 1.0 / math.log1p(amount)
                if candidate < distances[w] - 1e-12:
                    distances[w] = candidate
                    heapq.heappush(heap, (candidate, w))
                    sigma[w] = sigma[v]
                    predecessors[w] = [v]
                elif abs(candidate - distances[w]) <= 1e-12:
                    sigma[w] += sigma[v]
                    predecessors[w].append(v)
        dependency = [0.0] * n
        while order:
            w = order.pop()
            if sigma[w]:
                coefficient = (1.0 + dependency[w]) / sigma[w]
                for v in predecessors[w]:
                    dependency[v] += sigma[v] * coefficient
            if w != source:
                centrality[w] += dependency[w]
    normalizer = (n - 1) * (n - 2)
    if normalizer:
        centrality = [value / normalizer for value in centrality]
    return centrality


def seed_reach_within_two_hops(
    n: int, undirected_adj: list[set[int]], seed_indices: set[int]
) -> list[int]:
    result = [0] * n
    for start in range(n):
        visited = {start}
        reached = {start} if start in seed_indices else set()
        frontier = [start]
        for _ in range(2):
            next_frontier = []
            for node in frontier:
                for neighbor in undirected_adj[node]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        next_frontier.append(neighbor)
                        if neighbor in seed_indices:
                            reached.add(neighbor)
            frontier = next_frontier
        result[start] = len(reached)
    return result


def louvain_partition(n: int, edges: list[dict[str, Any]], seed: int = 41) -> list[int]:
    """Deterministic multilevel Louvain on an undirected, sum_kzt-weighted projection."""
    if n == 0:
        return []
    adjacency: dict[int, dict[int, float]] = {i: {} for i in range(n)}
    loops: dict[int, float] = defaultdict(float)
    for row in edges:
        u, v, weight = row["u"], row["v"], row["sum_kzt"]
        if u == v:
            loops[u] += weight
        else:
            adjacency[u][v] = adjacency[u].get(v, 0.0) + weight
            adjacency[v][u] = adjacency[v].get(u, 0.0) + weight
    current_nodes = list(range(n))
    original_to_current = list(range(n))
    rng = random.Random(seed)

    for _level in range(20):
        degree = {
            node: sum(adjacency[node].values()) + 2.0 * loops.get(node, 0.0)
            for node in current_nodes
        }
        total_weight = sum(degree.values()) / 2.0
        if total_weight <= 0.0:
            return list(range(n))
        community = {node: node for node in current_nodes}
        totals = dict(degree)
        order = current_nodes[:]
        for _sweep in range(100):
            rng.shuffle(order)
            moved = False
            for node in order:
                node_degree = degree[node]
                if node_degree <= 0.0:
                    continue
                old = community[node]
                connections: dict[int, float] = defaultdict(float)
                for neighbor, weight in adjacency[node].items():
                    connections[community[neighbor]] += weight
                old_connection = connections.get(old, 0.0)
                old_total = totals[old]
                best = old
                best_gain = 0.0
                for candidate in sorted(connections):
                    if candidate == old:
                        continue
                    candidate_total = totals[candidate]
                    gain = (connections[candidate] - old_connection) / total_weight
                    gain -= (node_degree * (candidate_total - old_total) + node_degree * node_degree) / (2.0 * total_weight * total_weight)
                    if gain > best_gain + 1e-15:
                        best, best_gain = candidate, gain
                if best != old:
                    totals[old] -= node_degree
                    totals[best] += node_degree
                    community[node] = best
                    moved = True
            if not moved:
                break

        community_groups: dict[int, list[int]] = defaultdict(list)
        for node, com in community.items():
            community_groups[com].append(node)
        if len(community_groups) == len(current_nodes):
            return [community[original_to_current[i]] for i in range(n)]
        ordered_groups = sorted(community_groups.values(), key=lambda group: min(group))
        community_to_new = {community[group[0]]: new for new, group in enumerate(ordered_groups)}
        old_to_new = {node: community_to_new[community[node]] for node in current_nodes}
        original_to_current = [old_to_new[node] for node in original_to_current]
        new_nodes = list(range(len(ordered_groups)))
        new_adjacency: dict[int, dict[int, float]] = {node: {} for node in new_nodes}
        new_loops: dict[int, float] = defaultdict(float)
        for node, weight in loops.items():
            new_loops[old_to_new[node]] += weight
        for u in current_nodes:
            for v, weight in adjacency[u].items():
                if u >= v:
                    continue
                cu, cv = old_to_new[u], old_to_new[v]
                if cu == cv:
                    new_loops[cu] += weight
                else:
                    new_adjacency[cu][cv] = new_adjacency[cu].get(cv, 0.0) + weight
                    new_adjacency[cv][cu] = new_adjacency[cv].get(cu, 0.0) + weight
        adjacency, loops, current_nodes = new_adjacency, new_loops, new_nodes
    return [original_to_current[i] for i in range(n)]


def build_features(
    nodes: list[dict[str, Any]], edges: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, float]]:
    sorted_nodes = sorted(nodes, key=lambda row: row["gid"])
    gids = [row["gid"] for row in sorted_nodes]
    index = {gid: i for i, gid in enumerate(gids)}
    n = len(gids)
    out_adj: list[list[tuple[int, float, int]]] = [[] for _ in range(n)]
    in_adj: list[list[tuple[int, float, int]]] = [[] for _ in range(n)]
    undirected_adj: list[set[int]] = [set() for _ in range(n)]
    edge_idx = []
    for row in edges:
        u, v = index[row["src"]], index[row["dst"]]
        amount = float(row["sum_kzt"])
        tx_count = int(row["n_tx"])
        out_adj[u].append((v, amount, tx_count))
        in_adj[v].append((u, amount, tx_count))
        undirected_adj[u].add(v)
        undirected_adj[v].add(u)
        edge_idx.append({"u": u, "v": v, "sum_kzt": amount, "n_tx": tx_count, "depth": row["depth"]})
    for row in out_adj:
        row.sort(key=lambda item: item[0])
    for row in in_adj:
        row.sort(key=lambda item: item[0])

    in_deg = [len(links) for links in in_adj]
    out_deg = [len(links) for links in out_adj]
    in_kzt = [sum(amount for _, amount, _ in links) for links in in_adj]
    out_kzt = [sum(amount for _, amount, _ in links) for links in out_adj]
    in_tx = [sum(count for _, _, count in links) for links in in_adj]
    out_tx = [sum(count for _, _, count in links) for links in out_adj]
    pr = pagerank(n, out_adj, out_kzt)
    betweenness = weighted_directed_betweenness(n, out_adj)
    seed_indices = {i for i, row in enumerate(sorted_nodes) if row["is_seed"]}
    seed_reach = seed_reach_within_two_hops(n, undirected_adj, seed_indices)
    truncated = [sorted_nodes[i]["depth"] == 4 and out_deg[i] == 0 for i in range(n)]
    pass_through = [out_kzt[i] / in_kzt[i] if in_kzt[i] > 0.0 else None for i in range(n)]

    pr_rank = percentile_ranks(pr)
    in_deg_rank = percentile_ranks([float(x) for x in in_deg])
    out_deg_rank = percentile_ranks([float(x) for x in out_deg])
    between_rank = percentile_ranks(betweenness)
    seed_reach_rank = percentile_ranks([float(x) for x in seed_reach])
    priority = [
        0.30 * pr_rank[i]
        + 0.25 * in_deg_rank[i]
        + 0.25 * between_rank[i]
        + 0.10 * out_deg_rank[i]
        + 0.10 * seed_reach_rank[i]
        for i in range(n)
    ]

    positive_betweenness = [value for value in betweenness if value > 0.0]
    thresholds = {
        "in_deg_p75": percentile([float(x) for x in in_deg], 0.75),
        "in_deg_p90": percentile([float(x) for x in in_deg], 0.90),
        "out_deg_p75": percentile([float(x) for x in out_deg], 0.75),
        "out_deg_p90": percentile([float(x) for x in out_deg], 0.90),
        "betweenness_p75_positive": percentile(positive_betweenness, 0.75),
        "pagerank_p75": percentile(pr, 0.75),
    }
    role_scores = {
        "consolidator": 0.72,
        "transit": 0.68,
        "distributor": 0.66,
        "terminal": 0.82,
        "coordinator": 0.76,
        "peripheral": 0.50,
    }
    result = []
    for i, node in enumerate(sorted_nodes):
        in_count, out_count = in_deg[i], out_deg[i]
        ratio = pass_through[i]
        is_seed = node["is_seed"]
        truncated_at_hop4 = truncated[i]
        role = "peripheral"
        score = role_scores[role]
        if out_count == 0 and node["depth"] == 4:
            role, score = "terminal", 0.45
        elif out_count == 0 and in_count > 0:
            role, score = "terminal", 0.50 if is_seed else 0.82
        elif (
            in_count >= max(1.0, thresholds["in_deg_p75"])
            and out_count >= max(1.0, thresholds["out_deg_p75"])
            and betweenness[i] > 0.0
            and betweenness[i] >= thresholds["betweenness_p75_positive"]
        ):
            role, score = "coordinator", role_scores["coordinator"]
        elif not is_seed and in_count >= max(1.0, thresholds["in_deg_p90"]) and ratio is not None and ratio < 0.5:
            role, score = "consolidator", role_scores["consolidator"]
        elif (
            is_seed
            and in_count >= max(1.0, thresholds["in_deg_p90"])
            and pr[i] >= thresholds["pagerank_p75"]
        ):
            # Seed pass-through ratios are biased by construction; use independent graph signals instead.
            role, score = "consolidator", 0.62
        elif not is_seed and in_count >= max(1.0, thresholds["in_deg_p75"]) and ratio is not None and 0.7 <= ratio <= 1.3:
            role, score = "transit", role_scores["transit"]
        elif out_count >= max(1.0, thresholds["out_deg_p90"]):
            role, score = "distributor", role_scores["distributor"]

        seed_note = " Seed inflow is understated." if is_seed else ""
        if role == "terminal" and truncated_at_hop4:
            evidence = (
                f"Hop 4: {fmt_money(in_kzt[i])} from {in_count} payers/{in_tx[i]} tx, 0 captured recipients; "
                "depth-cutoff artifact possible; terminal status unproven."
            )
        elif role == "terminal":
            evidence = (
                f"No captured outflow at hop {node['depth']}; {fmt_money(in_kzt[i])} from "
                f"{in_count} payers/{in_tx[i]} tx. Observed-sample terminal; verify."
            ) + seed_note
        elif role == "coordinator":
            evidence = (
                f"Coordination signs: {in_count} payers/{in_tx[i]} tx in, {out_count} recipients/{out_tx[i]} tx out; "
                f"betweenness p{between_rank[i] * 100:.0f}, {seed_reach[i]} seeds within 2 hops. Verify."
            ) + seed_note
        elif role == "consolidator" and is_seed:
            evidence = (
                f"Consolidation signs: {in_count} payers/{in_tx[i]} tx, {fmt_money(in_kzt[i])} observed in, "
                f"{out_count} recipients; seed inflow is understated. Verify."
            )
        elif role == "consolidator":
            evidence = (
                f"Consolidation signs: {in_count} payers/{in_tx[i]} tx, {fmt_money(in_kzt[i])} in; "
                f"forwarded {ratio:.0%} of recorded inflow; verify."
            )
        elif role == "transit":
            evidence = (
                f"Transit signs: {in_count} payers/{in_tx[i]} tx in, {out_count} recipients/{out_tx[i]} tx out; "
                f"forwarded {ratio:.0%} of recorded inflow. Verify."
            )
        elif role == "distributor":
            evidence = (
                f"Fan-out signs: {out_count} recipients/{out_tx[i]} tx, {fmt_money(out_kzt[i])} out; "
                f"{in_count} payers/{in_tx[i]} tx in. Verify."
            ) + seed_note
        else:
            evidence = (
                f"{in_count} payers/{in_tx[i]} tx in ({fmt_money(in_kzt[i])}), "
                f"{out_count} recipients/{out_tx[i]} tx out ({fmt_money(out_kzt[i])}); no strong role signal."
            ) + seed_note
        evidence = evidence[:197] + "..." if len(evidence) > 200 else evidence
        result.append(
            {
                "gid": node["gid"],
                "depth": node["depth"],
                "is_seed": is_seed,
                "in_deg": in_count,
                "out_deg": out_count,
                "in_kzt": in_kzt[i],
                "out_kzt": out_kzt[i],
                "in_tx": in_tx[i],
                "out_tx": out_tx[i],
                "pagerank": pr[i],
                "betweenness": betweenness[i],
                "betweenness_rank": between_rank[i],
                "pagerank_rank": pr_rank[i],
                "in_deg_rank": in_deg_rank[i],
                "out_deg_rank": out_deg_rank[i],
                "seed_reach_rank": seed_reach_rank[i],
                "pass_through": ratio,
                "seed_reach": seed_reach[i],
                "truncated_by_depth": truncated_at_hop4,
                "role": role,
                "role_score": score,
                "priority_score": priority[i],
                "evidence": evidence,
            }
        )
    partition = louvain_partition(n, edge_idx)
    groups: dict[int, list[int]] = defaultdict(list)
    for i, community in enumerate(partition):
        groups[community].append(i)
    stable_groups = sorted(groups.values(), key=lambda group: (-len(group), min(gids[i] for i in group)))
    cluster_for_index = {}
    for cluster_id, group in enumerate(stable_groups):
        for i in group:
            cluster_for_index[i] = cluster_id
            result[i]["cluster_id"] = cluster_id
    cluster_rows = make_cluster_rows(stable_groups, result, cluster_for_index, edge_idx)
    return result, cluster_rows, thresholds


def fmt_money(value: float) -> str:
    return f"{value:,.0f} KZT"


def make_cluster_rows(
    groups: list[list[int]],
    features: list[dict[str, Any]],
    cluster_for_index: dict[int, int],
    edges: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    internal_turnover = [0.0] * len(groups)
    for edge in edges:
        if cluster_for_index[edge["u"]] == cluster_for_index[edge["v"]]:
            internal_turnover[cluster_for_index[edge["u"]]] += edge["sum_kzt"]
    rows = []
    signal_roles = {"consolidator", "coordinator", "distributor", "transit"}
    for cluster_id, members in enumerate(groups):
        member_features = [features[i] for i in members]
        n_nodes = len(members)
        n_seed = sum(row["is_seed"] for row in member_features)
        role_counts = {role: sum(row["role"] == role for row in member_features) for role in ROLES}
        structural = sum(role_counts[role] for role in signal_roles)
        if n_seed and role_counts["consolidator"] + role_counts["coordinator"]:
            shape = "seed-linked coordination/collection"
        elif role_counts["consolidator"] + role_counts["coordinator"]:
            shape = "downstream aggregation/coordination"
        elif role_counts["distributor"]:
            shape = "fan-out distribution"
        elif role_counts["transit"]:
            shape = "pass-through movement"
        else:
            shape = "low-signal fragment"
        top_gids = sorted(member_features, key=lambda row: (-row["priority_score"], row["gid"]))[:5]
        top_text = ";".join(str(row["gid"]) for row in top_gids)
        hypothesis = (
            f"Hypothesis: {shape}; {n_seed}/{n_nodes} seed, {structural} movement/hub signals, "
            f"{fmt_money(internal_turnover[cluster_id])} internal turnover. Verify from transactions."
        )
        rows.append(
            {
                "cluster_id": cluster_id,
                "n_nodes": n_nodes,
                "n_seed": n_seed,
                "sum_kzt_internal": internal_turnover[cluster_id],
                "top_gids": top_text,
                "hypothesis": hypothesis,
            }
        )
    return rows


def _reject_link(path: Path, metadata: os.stat_result) -> None:
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    if stat.S_ISLNK(metadata.st_mode) or getattr(metadata, "st_file_attributes", 0) & reparse_flag:
        raise ValueError(f"Output paths cannot contain symlinks, junctions, or reparse points: {path}")


def _check_output_directory(directory: Path) -> None:
    """Check existing ancestors without resolving away links or junctions."""
    for component in (*reversed(directory.parents), directory):
        try:
            metadata = component.lstat()
        except FileNotFoundError:
            continue
        _reject_link(component, metadata)
        if not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(f"Output directory ancestor is not a directory: {component}")


def _check_output_file(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    _reject_link(path, metadata)
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"Output destination is not a regular file: {path}")
    if metadata.st_nlink > 1:
        raise ValueError(f"Output destination has multiple hard links: {path}")


def prepare_output_directory(directory: Path) -> Path:
    """Allow an explicit destination while rejecting existing redirection.

    Preflight every fixed output name before writing any result. This is not a
    transaction across files or protection against a concurrently hostile owner.
    """
    directory = Path(os.path.abspath(directory))
    _check_output_directory(directory)
    for name in OUTPUT_NAMES:
        _check_output_file(directory / name)
    directory.mkdir(parents=True, exist_ok=True)
    _check_output_directory(directory)
    for name in OUTPUT_NAMES:
        _check_output_file(directory / name)
    return directory


def atomic_write_text(path: Path, content: str, *, newline: str | None = None) -> None:
    """Replace one output from an exclusive temporary file in the same directory."""
    path = Path(os.path.abspath(path))
    _check_output_directory(path.parent)
    _check_output_file(path)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline=newline) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        _check_output_directory(path.parent)
        _check_output_file(path)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    handle = io.StringIO(newline="")
    writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_text(path, handle.getvalue(), newline="")


def make_top_rows(features: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    ordered = sorted(features, key=lambda row: (-row["priority_score"], row["gid"]))[: max(20, limit)]
    return [
        {
            "rank": rank,
            "gid": row["gid"],
            "role": row["role"],
            "priority_score": f"{row['priority_score']:.6f}",
            "why": f"Priority {row['priority_score']:.2f}; {row['evidence']}",
        }
        for rank, row in enumerate(ordered, start=1)
    ]


def dashboard_html(features: list[dict[str, Any]], edges: list[dict[str, Any]], thresholds: dict[str, float], clusters: list[dict[str, Any]]) -> str:
    index = {row["gid"]: i for i, row in enumerate(features)}
    nodes = [
        {
            **row,
            # JavaScript Number cannot preserve these 18-digit identifiers.
            "gid": str(row["gid"]),
            "role": row["role"],
            "score": row["role_score"],
            "priority": row["priority_score"],
            "cluster": row["cluster_id"],
            "in_deg": row["in_deg"],
            "out_deg": row["out_deg"],
            "in_kzt": round(row["in_kzt"], 2),
            "out_kzt": round(row["out_kzt"], 2),
            "evidence": row["evidence"],
        }
        for row in features
    ]
    graph_edges = [
        {"src": index[row["src"]], "dst": index[row["dst"]], "amount": row["sum_kzt"], "n_tx": row["n_tx"]}
        for row in edges
    ]
    payload = json.dumps({"nodes": nodes, "edges": graph_edges, "roleColors": ROLE_COLORS, "thresholds": thresholds, "clusters": clusters}, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    payload = payload.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    template = Path(__file__).with_name("viewer.html").read_text(encoding="utf-8")
    return template.replace("__DATA__", payload)


def print_review(features: list[dict[str, Any]], clusters: list[dict[str, Any]], top: list[dict[str, Any]], thresholds: dict[str, float], validation: dict[str, Any], elapsed: float) -> None:
    role_counts = defaultdict(int)
    for row in features:
        role_counts[row["role"]] += 1
    cluster_sizes = sorted((row["n_nodes"] for row in clusters), reverse=True)
    print("DATA CHECK")
    for key, value in validation.items():
        print(f"  {key}: {value}")
    print("ROLE DISTRIBUTION")
    for role in ROLES:
        print(f"  {role}: {role_counts[role]}")
    print(f"CLUSTERS: {len(clusters)}; largest sizes: {cluster_sizes[:10]}")
    print("ROLE THRESHOLDS FROM THIS DATASET")
    for key, value in thresholds.items():
        print(f"  {key}: {value:.8g}")
    print("TOP 5")
    for row in top[:5]:
        print(f"  {row['rank']}. gid={row['gid']} role={row['role']} priority={row['priority_score']} — {row['why']}")
    print(f"Pipeline elapsed: {elapsed:.2f} seconds")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze a directed transaction graph and generate CSVs plus a local dashboard.")
    parser.add_argument("--data", type=Path, default=Path(__file__).parent / "data", help="directory containing the three Markdown or Parquet tables")
    parser.add_argument("--out", type=Path, default=Path(__file__).parent / "out", help="directory for CSV files and dashboard")
    parser.add_argument("--top", type=int, default=100, help="number of ranked nodes to export (minimum 20)")
    args = parser.parse_args()
    started = time.perf_counter()
    tables = load_tables(args.data)
    nodes, edges, tx = normalize_tables(tables)
    validation = validate_inputs(nodes, edges, tx)
    features, cluster_rows, thresholds = build_features(nodes, edges)
    top_rows = make_top_rows(features, args.top)
    dashboard = dashboard_html(features, edges, thresholds, cluster_rows)
    args.out = prepare_output_directory(args.out)
    write_csv(args.out / "nodes_roles.csv", NODE_COLUMNS, [
        {
            "gid": row["gid"],
            "role": row["role"],
            "role_score": f"{row['role_score']:.2f}",
            "cluster_id": row["cluster_id"],
            "priority_score": f"{row['priority_score']:.6f}",
            "evidence": row["evidence"],
        }
        for row in features
    ])
    write_csv(args.out / "clusters.csv", CLUSTER_COLUMNS, cluster_rows)
    write_csv(args.out / "top_nodes.csv", TOP_COLUMNS, top_rows)
    atomic_write_text(args.out / "index.html", dashboard)
    elapsed = time.perf_counter() - started
    summary = {
        "python_version": sys.version.split()[0],
        "validation": validation,
        "thresholds": thresholds,
        "role_counts": {role: sum(row["role"] == role for row in features) for role in ROLES},
        "cluster_count": len(cluster_rows),
        "truncated_by_depth_count": sum(row["truncated_by_depth"] for row in features),
        "elapsed_seconds": round(elapsed, 6),
    }
    atomic_write_text(args.out / "analysis_summary.json", json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False))
    print_review(features, cluster_rows, top_rows, thresholds, validation, elapsed)
    print(f"Outputs: {args.out / 'nodes_roles.csv'}, {args.out / 'clusters.csv'}, {args.out / 'top_nodes.csv'}, {args.out / 'index.html'}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
