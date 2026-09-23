"""Regression checks for malformed inputs and filesystem output redirection."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime
from unittest import mock

import pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def fixture_tables():
    return {
        "nodes": [{"gid": 1, "depth": 0, "is_seed": True}, {"gid": 2, "depth": 1, "is_seed": False}],
        "edges": [{"src": 1, "dst": 2, "sum_kzt": 5000, "n_tx": 1, "depth": 1}],
        "transactions": [{"src": 1, "dst": 2, "sum_kzt": 5000, "date": "2026-07-01"}],
    }


def write_tables(directory, tables):
    directory.mkdir(exist_ok=True)
    for name, rows in tables.items():
        columns = list(rows[0])
        lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
        lines.extend("| " + " | ".join(str(row[column]) for column in columns) + " |" for row in rows)
        (directory / f"{name}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


class InputSafetyTests(unittest.TestCase):
    def setUp(self):
        audit_root = PROJECT_ROOT / ".audit"
        audit_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="input_safety_", dir=audit_root)
        self.root = Path(self.temporary.name)
        self.assertEqual(self.root.resolve().parent, audit_root.resolve())
        self.addCleanup(self.temporary.cleanup)

    def test_integer_ids_never_accept_floats_booleans_or_overflow(self):
        for bad in (1.0, 1.9, True, False, 2**63, -(2**63) - 1, "1e18", "1.0", "=1+1"):
            with self.subTest(bad=bad):
                tables = fixture_tables()
                tables["nodes"][0]["gid"] = bad
                with self.assertRaises(ValueError):
                    pipeline.normalize_tables(tables)
        for good in (-(2**63), 2**63 - 1, "100000003684369100"):
            self.assertEqual(pipeline.exact_int64(good, "gid"), int(good))

    def test_counts_and_depths_are_not_truncated(self):
        for table, key in (("nodes", "depth"), ("edges", "depth"), ("edges", "n_tx")):
            for bad in (1.5, True, "1.5"):
                with self.subTest(table=table, key=key, bad=bad):
                    tables = fixture_tables()
                    tables[table][0][key] = bad
                    with self.assertRaises(ValueError):
                        pipeline.normalize_tables(tables)

    def test_dates_must_be_real_dates_in_dataset_month(self):
        invalid = ("not-a-date", "2026-07-32", "2026-06-30", "2026-08-01", "2026-7-1", "2026-07-01T00:00:00", datetime(2026, 7, 1, 1))
        for bad in invalid:
            with self.subTest(date=bad), self.assertRaises(ValueError):
                pipeline.transaction_date(bad)
        for good in ("2026-07-01", date(2026, 7, 31), datetime(2026, 7, 15)):
            self.assertTrue(pipeline.transaction_date(good).startswith("2026-07-"))

    def test_transaction_floor_and_finite_aggregates(self):
        for amount in (4999.99, float("inf"), float("nan"), 1e308, 1e200):
            tables = fixture_tables()
            tables["edges"][0]["sum_kzt"] = amount
            tables["transactions"][0]["sum_kzt"] = amount
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                pipeline.validate_inputs(*pipeline.normalize_tables(tables))
        tables = fixture_tables()
        pipeline.validate_inputs(*pipeline.normalize_tables(tables))

    def test_unknown_attributes_and_duplicate_columns_rejected(self):
        tables = fixture_tables()
        tables["nodes"][0]["invented_name"] = "not allowed"
        tables["nodes"][1]["invented_name"] = "not allowed"
        write_tables(self.root, tables)
        with self.assertRaisesRegex(ValueError, "unexpected: invented_name"):
            pipeline.load_tables(self.root)
        (self.root / "nodes.md").write_text("| gid | gid |\n| --- | --- |\n| 1 | 2 |\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "duplicate column"):
            pipeline.read_markdown_table(self.root / "nodes.md")

    def test_mixed_formats_report_actionable_missing_files(self):
        (self.root / "nodes.md").write_text("placeholder", encoding="utf-8")
        (self.root / "edges.parquet").touch()
        (self.root / "transactions.md").write_text("placeholder", encoding="utf-8")
        with self.assertRaises(FileNotFoundError) as caught:
            pipeline.load_tables(self.root)
        message = str(caught.exception)
        self.assertIn("mixed formats are not combined", message)
        for missing in ("edges.md", "nodes.parquet", "transactions.parquet"):
            self.assertIn(missing, message)

    def test_complete_markdown_set_wins_over_complete_parquet_set(self):
        write_tables(self.root, fixture_tables())
        for name in pipeline.REQUIRED:
            (self.root / f"{name}.parquet").touch()
        with mock.patch.object(pipeline, "read_parquet_tables", side_effect=AssertionError("unexpected Parquet read")):
            self.assertEqual(len(pipeline.load_tables(self.root)["nodes"]), 2)

    def test_missing_parquet_dependency_has_pinned_install_instruction(self):
        with mock.patch.dict(sys.modules, {"pyarrow": None, "pyarrow.parquet": None}):
            with self.assertRaisesRegex(RuntimeError, "pip install -r requirements.txt"):
                pipeline.read_parquet_tables(self.root)

    def test_hardlink_preflight_preserves_every_existing_output(self):
        data = self.root / "data"
        write_tables(data, fixture_tables())
        out = self.root / "out"
        out.mkdir()
        for name in pipeline.OUTPUT_NAMES[:-1]:
            (out / name).write_text("previous output", encoding="utf-8")
        victim = self.root / "unrelated.txt"
        victim.write_text("untouched", encoding="utf-8")
        os.link(victim, out / pipeline.OUTPUT_NAMES[-1])
        with mock.patch.object(sys, "argv", ["pipeline.py", "--data", str(data), "--out", str(out)]):
            with self.assertRaisesRegex(ValueError, "multiple hard links"):
                pipeline.main()
        self.assertEqual(victim.read_text(encoding="utf-8"), "untouched")
        for name in pipeline.OUTPUT_NAMES[:-1]:
            self.assertEqual((out / name).read_text(encoding="utf-8"), "previous output")

    def test_symlink_file_and_ancestor_are_rejected(self):
        target = self.root / "target"
        target.mkdir()
        victim = target / "victim.txt"
        victim.write_text("untouched", encoding="utf-8")
        out = self.root / "out"
        out.mkdir()
        try:
            (out / "index.html").symlink_to(victim)
            ancestor = self.root / "linked_parent"
            ancestor.symlink_to(target, target_is_directory=True)
        except OSError as exc:
            self.skipTest(f"Symlink creation unavailable: {exc}")
        with self.assertRaisesRegex(ValueError, "symlinks"):
            pipeline.prepare_output_directory(out)
        with self.assertRaisesRegex(ValueError, "symlinks"):
            pipeline.prepare_output_directory(ancestor / "new_output")
        self.assertFalse((target / "new_output").exists())
        self.assertEqual(victim.read_text(encoding="utf-8"), "untouched")

    @unittest.skipUnless(os.name == "nt", "Windows junction regression")
    def test_windows_junction_ancestor_is_rejected(self):
        target = self.root / "junction_target"
        target.mkdir()
        junction = self.root / "junction"
        result = subprocess.run(["cmd", "/c", "mklink", "/J", str(junction), str(target)], capture_output=True)
        if result.returncode:
            self.skipTest("Junction creation unavailable on this filesystem")
        with self.assertRaisesRegex(ValueError, "junctions"):
            pipeline.prepare_output_directory(junction / "new_output")
        self.assertFalse((target / "new_output").exists())

    def test_atomic_replace_failure_leaves_original_and_cleans_temporary(self):
        out = pipeline.prepare_output_directory(self.root / "chosen_directory")
        path = out / "nodes_roles.csv"
        path.write_text("original", encoding="utf-8")
        with mock.patch.object(pipeline.os, "replace", side_effect=OSError("simulated replace failure")):
            with self.assertRaisesRegex(OSError, "simulated"):
                pipeline.atomic_write_text(path, "replacement")
        self.assertEqual(path.read_text(encoding="utf-8"), "original")
        self.assertEqual(sorted(item.name for item in out.iterdir()), ["nodes_roles.csv"])
        pipeline.atomic_write_text(path, "replacement")
        self.assertEqual(path.read_text(encoding="utf-8"), "replacement")


if __name__ == "__main__":
    unittest.main()
