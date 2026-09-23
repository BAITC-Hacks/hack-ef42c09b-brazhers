"""Regression checks for export corruption and independent graph calculations."""

import csv
import datetime
import json
import math
import random
import shutil
import tempfile
import unittest
from pathlib import Path

import pipeline
import verify


ROOT = Path(__file__).resolve().parents[1]


class IndependentInputContractTests(unittest.TestCase):
    def test_signed_ascii_integer_contract(self):
        for value in (" +17 ", "-2", 0, -(2**63), 2**63 - 1):
            with self.subTest(value=value):
                self.assertEqual(verify.integer(value, "gid"), int(value))
                self.assertEqual(verify.integer(value, "gid"), pipeline.exact_int64(value, "gid"))
        for value in (True, 1.0, 1.5, "1.0", "1e3", "１２", 2**63):
            with self.subTest(value=value), self.assertRaises(verify.VerificationError):
                verify.integer(value, "gid")

    def test_markdown_boolean_spellings_and_reordered_columns(self):
        tables = {
            "nodes": [{"gid": "+1", "depth": "+0", "is_seed": "yes"}, {"gid": "+2", "depth": "+1", "is_seed": "no"}],
            "edges": [{"src": "+1", "dst": "+2", "sum_kzt": "5000", "n_tx": "+1", "depth": "+1"}],
            "transactions": [{"src": "+1", "dst": "+2", "date": "2026-07-01", "sum_kzt": "5000"}],
        }
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for name, rows in tables.items():
                columns = list(reversed(verify.SCHEMAS[name]))
                lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
                lines += ["| " + " | ".join(row[c] for c in columns) + " |" for row in rows]
                (directory / f"{name}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
            parsed, _ = verify.load_raw(directory)
            self.assertEqual([r["gid"] for r in parsed["nodes"]], [1, 2])
            self.assertEqual([r["is_seed"] for r in parsed["nodes"]], [True, False])
            self.assertEqual(verify.recompute(parsed)[2]["edge_count"], 1)

    def test_parquet_rejects_unsigned_depth_and_float_ids(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            self.skipTest("Optional Parquet dependency is not installed in this interpreter")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            nodes = pa.table({"gid": pa.array([1, 2], type=pa.int64()), "depth": pa.array([0, 1], type=pa.int8()), "is_seed": [True, False]})
            edges = pa.table({"src": pa.array([1], type=pa.int64()), "dst": pa.array([2], type=pa.int64()), "sum_kzt": pa.array([5000.0], type=pa.float64()), "n_tx": pa.array([1], type=pa.int64()), "depth": pa.array([1], type=pa.int8())})
            tx = pa.table({"src": pa.array([1], type=pa.int64()), "dst": pa.array([2], type=pa.int64()), "date": pa.array([datetime.date(2026, 7, 1)], type=pa.date32()), "sum_kzt": pa.array([5000.0], type=pa.float64())})
            pq.write_table(edges, directory / "edges.parquet")
            pq.write_table(tx, directory / "transactions.parquet")
            pq.write_table(nodes, directory / "nodes.parquet")
            self.assertEqual(len(verify.load_raw(directory)[0]["nodes"]), 2)
            for name, kind, values, message in (("depth", pa.uint8(), [0, 1], "signed integer"), ("gid", pa.float64(), [1.0, 2.0], "signed int64")):
                bad = nodes.set_column(nodes.schema.get_field_index(name), name, pa.array(values, type=kind))
                pq.write_table(bad, directory / "nodes.parquet")
                with self.subTest(column=name), self.assertRaisesRegex(verify.VerificationError, message):
                    verify.load_raw(directory)


class ExportContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.out = Path(self.temporary.name)
        for name in ("nodes_roles.csv", "clusters.csv", "top_nodes.csv", "index.html", "analysis_summary.json"):
            shutil.copyfile(ROOT / "out" / name, self.out / name)

    def edit_csv(self, name, change):
        path = self.out / f"{name}.csv"
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fields, rows = reader.fieldnames, list(reader)
        change(fields, rows)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def assert_rejected(self, message):
        with self.assertRaisesRegex(verify.VerificationError, message):
            verify.verify(ROOT / "data", self.out)

    def test_complete_delivered_exports_pass(self):
        report = verify.verify(ROOT / "data", self.out)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(len(report["sample_10"]), 10)

    def test_missing_node_is_detected(self):
        self.edit_csv("nodes_roles", lambda _, rows: rows.pop())
        self.assert_rejected("missing/extra/duplicate gid")

    def test_extra_column_is_detected(self):
        self.edit_csv("nodes_roles", lambda fields, _: fields.append("invented_attribute"))
        self.assert_rejected("unexpected CSV headers")

    def test_plausible_but_wrong_evidence_is_detected(self):
        def change(_, rows):
            target = next(row for row in rows if row["role"] == "consolidator")
            target["evidence"] = "Consolidation signs: 999 payers/999 tx, 999 KZT in; forwarded 3%; verify."
        self.edit_csv("nodes_roles", change)
        self.assert_rejected("evidence missing/wrong numeric claim")

    def test_wrong_cluster_seed_count_is_detected(self):
        self.edit_csv("clusters", lambda _, rows: rows[0].update(n_seed=str(int(rows[0]["n_seed"]) + 1)))
        self.assert_rejected("wrong size/seed count")

    def test_priority_corruption_is_detected(self):
        self.edit_csv("nodes_roles", lambda _, rows: rows[0].update(priority_score="0.000000"))
        self.assert_rejected("priority_score")

    def test_top_ranking_corruption_is_detected(self):
        def change(_, rows):
            rows[0], rows[1] = rows[1], rows[0]
        self.edit_csv("top_nodes", change)
        self.assert_rejected("incorrect order")

    def test_browser_number_gid_is_detected_before_rounding(self):
        path = self.out / "index.html"
        html = path.read_text(encoding="utf-8")
        parser = verify.PayloadParser()
        parser.feed(html)
        old = "".join(parser.parts)
        payload = json.loads(old)
        payload["nodes"][0]["gid"] = int(payload["nodes"][0]["gid"])
        path.write_text(html.replace(old, json.dumps(payload)), encoding="utf-8")
        self.assert_rejected("GIDs must be strings")

    def test_report_cannot_overwrite_inputs(self):
        with self.assertRaisesRegex(verify.VerificationError, "inside the project's .audit"):
            verify.save_report(ROOT / "data" / "report.json", {"status": "PASS"})


def exhaustive_betweenness(adjacency):
    n = len(adjacency)
    result = [0.0] * n
    for source in range(n):
        for target in range(n):
            if source == target:
                continue
            shortest, paths = math.inf, []

            def walk(u, path, distance):
                nonlocal shortest, paths
                if distance > shortest + 1e-12:
                    return
                if u == target:
                    if distance < shortest - 1e-12:
                        shortest, paths = distance, [path]
                    elif abs(distance - shortest) <= 1e-12:
                        paths.append(path)
                    return
                for v, amount in adjacency[u]:
                    if v not in path:
                        walk(v, path + [v], distance + 1 / math.log1p(amount))

            walk(source, [source], 0.0)
            for path in paths:
                for node in path[1:-1]:
                    result[node] += 1 / len(paths)
    return [v / ((n - 1) * (n - 2)) for v in result]


class AlgorithmRegressionTests(unittest.TestCase):
    def test_both_brandes_implementations_match_exhaustive_paths(self):
        rng = random.Random(32811)
        for example in range(60):
            n = rng.randrange(3, 7)
            adjacency = [[(v, rng.choice([5000, 5000, 10000, 1000000]))
                          for v in range(n) if v != u and rng.random() < .3] for u in range(n)]
            reference = exhaustive_betweenness(adjacency)
            production = pipeline.weighted_directed_betweenness(n, [[(v, amount, 1) for v, amount in row] for row in adjacency])
            independent = verify.directed_betweenness(adjacency)
            with self.subTest(graph=example):
                for expected, actual, audited in zip(reference, production, independent):
                    self.assertAlmostEqual(actual, expected, places=11)
                    self.assertAlmostEqual(audited, expected, places=11)

    def test_louvain_increases_modularity_and_preserves_components(self):
        rng = random.Random(92841)
        for example in range(30):
            n = rng.randrange(3, 25)
            edges = [{"u": u, "v": v, "sum_kzt": float(rng.randrange(1, 100))}
                     for u in range(n) for v in range(n) if rng.random() < .12]
            if not edges:
                continue

            def modularity(partition):
                degree, internal = [0.0] * n, {}
                weight = sum(e["sum_kzt"] for e in edges)
                for edge in edges:
                    u, v, w = edge["u"], edge["v"], edge["sum_kzt"]
                    degree[u] += w
                    degree[v] += w
                    if partition[u] == partition[v]:
                        internal[partition[u]] = internal.get(partition[u], 0) + w
                totals = {}
                for node, value in enumerate(degree):
                    label = partition[node]
                    totals[label] = totals.get(label, 0) + value
                return sum(internal.get(label, 0) / weight - (value / (2 * weight)) ** 2 for label, value in totals.items())

            partition = pipeline.louvain_partition(n, edges)
            adjacency = [set() for _ in range(n)]
            for edge in edges:
                adjacency[edge["u"]].add(edge["v"])
                adjacency[edge["v"]].add(edge["u"])
            with self.subTest(graph=example):
                self.assertEqual(partition, pipeline.louvain_partition(n, edges))
                self.assertGreaterEqual(modularity(partition) + 1e-12, modularity(list(range(n))))
                for label in set(partition):
                    members = {i for i, value in enumerate(partition) if value == label}
                    seen, frontier = set(), [min(members)]
                    while frontier:
                        u = frontier.pop()
                        if u in seen:
                            continue
                        seen.add(u)
                        frontier.extend((adjacency[u] & members) - seen)
                    self.assertEqual(members, seen)


if __name__ == "__main__":
    unittest.main()
