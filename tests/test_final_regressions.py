"""End-to-end regressions for the three supplied final-review fixtures.

Every fixture is synthetic and is sent through the pipeline CLI. No existing
outputs or production client identifiers are used as expected answers.
"""

import csv
import os
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import verify


COLUMNS = {
    "nodes": ("gid", "depth", "is_seed"),
    "edges": ("src", "dst", "sum_kzt", "n_tx", "depth"),
    "transactions": ("src", "dst", "date", "sum_kzt"),
}


class FinalRegressionTests(unittest.TestCase):
    def setUp(self):
        audit_root = ROOT / ".audit"
        self.assertTrue(audit_root.resolve().is_relative_to(ROOT), "Audit fixtures must stay inside the repository")
        audit_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="final_regression_", dir=audit_root)
        self.work = Path(self.temporary.name)
        # TemporaryDirectory only removes this newly created, bounded fixture.
        self.assertEqual(self.work.resolve().parent, audit_root.resolve())
        self.addCleanup(self.temporary.cleanup)

    def generate(self, nodes, transfers):
        data, out = self.work / "data", self.work / "out"
        data.mkdir()
        edges = [{"src": src, "dst": dst, "sum_kzt": amount, "n_tx": 1, "depth": depth}
                 for src, dst, amount, depth in transfers]
        transactions = [{"src": src, "dst": dst, "sum_kzt": amount, "date": "2026-07-01"}
                        for src, dst, amount, _ in transfers]
        tables = {"nodes": nodes, "edges": edges, "transactions": transactions}
        for name, rows in tables.items():
            columns = COLUMNS[name]
            lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
            lines += ["| " + " | ".join(str(row[key]) for key in columns) + " |" for row in rows]
            (data / f"{name}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        environment = os.environ.copy()
        environment["PYTHONUTF8"] = "1"
        result = subprocess.run(
            [sys.executable, "-B", str(ROOT / "pipeline.py"), "--data", str(data), "--out", str(out)],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace", env=environment, timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"Pipeline did not generate the fixture outputs:\n{result.stdout}\n{result.stderr}")
        for filename in ("nodes_roles.csv", "clusters.csv", "top_nodes.csv", "index.html", "analysis_summary.json"):
            self.assertTrue((out / filename).is_file(), f"Missing freshly generated output {filename}")
        return data, out

    @staticmethod
    def csv_rows(path):
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return reader.fieldnames, list(reader)

    def assert_verifies(self, data, out):
        try:
            report = verify.verify(data, out)
        except verify.VerificationError as exc:
            self.fail(f"Independent verifier rejected freshly generated fixture: {exc}")
        self.assertEqual(report["status"], "PASS")
        return report

    def test_decimal_boundary_assigns_transit_and_verifies(self):
        nodes = [{"gid": gid, "depth": 1 if gid == 2 else 2 if gid == 3 else 0,
                  "is_seed": gid not in (2, 3)} for gid in range(24)]
        transfers = [(0, 2, "5000.01", 1), (1, 2, "5000.39", 1), (2, 3, "7000.28", 2)]
        transfers += [(gid, gid + 1, "10000", 1) for gid in range(4, 23)]
        self.assertEqual(Decimal("7000.28") / (Decimal("5000.01") + Decimal("5000.39")), Decimal("0.70"))
        data, out = self.generate(nodes, transfers)
        _, rows = self.csv_rows(out / "nodes_roles.csv")
        node = next(row for row in rows if row["gid"] == "2")
        self.assertEqual(node["role"], "transit", "Exact 0.70 pass-through must satisfy the inclusive transit boundary")
        self.assertEqual(Decimal(node["role_score"]), Decimal("0.68"))
        self.assert_verifies(data, out)

    def test_louvain_counterexample_exports_only_connected_communities(self):
        index_edges = [
            (1, 17, 100000), (2, 24, 100000), (3, 9, 30000), (3, 11, 10000),
            (4, 24, 100000), (5, 27, 100000), (6, 12, 100000), (7, 14, 30000),
            (8, 27, 30000), (9, 11, 100000), (9, 21, 30000), (9, 25, 5000),
            (9, 30, 30000), (10, 20, 30000), (10, 23, 100000), (11, 24, 100000),
            (11, 25, 30000), (11, 28, 30000), (13, 25, 100000), (14, 16, 100000),
            (15, 19, 30000), (16, 23, 100000), (18, 27, 10000), (19, 27, 100000),
            (21, 30, 100000), (22, 29, 30000), (24, 28, 100000),
        ]
        nodes = [{"gid": 1000 + index, "depth": 0, "is_seed": True} for index in range(32)]
        transfers = [(1000 + src, 1000 + dst, str(amount), 1) for src, dst, amount in index_edges]
        data, out = self.generate(nodes, transfers)
        _, rows = self.csv_rows(out / "nodes_roles.csv")
        groups, neighbors = defaultdict(set), defaultdict(set)
        for row in rows:
            groups[row["cluster_id"]].add(int(row["gid"]))
        for src, dst, _, _ in transfers:
            neighbors[src].add(dst)
            neighbors[dst].add(src)
        self.assertEqual({int(row["gid"]) for row in rows}, {node["gid"] for node in nodes})
        for members in groups.values():
            seen, pending = set(), [min(members)]
            while pending:
                node = pending.pop()
                if node in seen:
                    continue
                seen.add(node)
                pending.extend((neighbors[node] & members) - seen)
            self.assertEqual(seen, members, f"Disconnected induced community exported: {sorted(members)}")
        self.assert_verifies(data, out)

    def test_two_node_top_is_complete_and_missing_row_is_detected(self):
        nodes = [{"gid": 1, "depth": 0, "is_seed": True}, {"gid": 2, "depth": 1, "is_seed": False}]
        data, out = self.generate(nodes, [(1, 2, "5000", 1)])
        fields, rows = self.csv_rows(out / "top_nodes.csv")
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["gid"] for row in rows}, {"1", "2"})
        self.assertEqual(self.assert_verifies(data, out)["top_count"], 2)
        with (out / "top_nodes.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows[:-1])
        with self.assertRaisesRegex(verify.VerificationError, "top_nodes"):
            verify.verify(data, out)


if __name__ == "__main__":
    unittest.main()
