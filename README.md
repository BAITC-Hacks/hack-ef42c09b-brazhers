# Money Graph

Local, explainable analysis of the supplied July 2026 transaction network. The pipeline reads the three included Markdown table exports (or matching Parquet files), validates the transaction aggregates, assigns one documented role to every node, clusters the graph, ranks review priorities, and writes a standalone browser dashboard.

The results are investigative hypotheses based only on anonymized graph structure. They do not establish guilt or a client's identity.

## Run

Requires Python 3.10 or newer. The included Markdown inputs and default pipeline use only the Python standard library.

```powershell
python pipeline.py
```

This writes `nodes_roles.csv`, `clusters.csv`, `top_nodes.csv`, and `index.html` into `out/`. A supplementary `analysis_summary.json` records validation counts, actual thresholds, role counts, Python version, and runtime. To choose paths or export a different number of priority rows:

```powershell
python pipeline.py --data data --out out --top 100
```

Open `out/index.html` in a browser. It is self-contained and works without a server or internet connection. The source template is `viewer.html`. The viewer initially opens the highest-priority node's direct neighborhood. Search for a full `gid`, click a node, or choose an entry in the priority list to see its direct incoming/outgoing connections. The card includes the exact role rule, observed metrics, and all five percentile contributions to priority. Use the color selector, cluster filter, and whole-network view to explore the rest of the graph; drag to pan and scroll to zoom. All 18-digit GIDs are serialized as strings in the browser to preserve exact identity; the required CSVs retain integer-form IDs.

The overview groups nodes by community; the direct-neighborhood view uses a radial layout. Screen distances are for readability and are not an analytical metric. A five-minute demonstration with three computed examples is in [DEMO.md](DEMO.md).

The loader also accepts `edges.parquet`, `nodes.parquet`, and `transactions.parquet`. Parquet input is optional and needs pandas and pyarrow (`python -m pip install -r requirements.txt`). Markdown is preferred if both complete file sets are present. Use a separate input directory for Parquet, for example `python pipeline.py --data parquet_data --out parquet_out`. The packaged Markdown example runs on a clean Python installation without third-party packages.

## Inputs and data checks

`data/` contains the supplied `edges.md`, `nodes.md`, and `transactions.md` tables. Their fields are:

- `edges`: `src`, `dst`, `sum_kzt`, `n_tx`, `depth` — one directed payer/payee pair per row, with July sum and transaction count.
- `nodes`: `gid`, `depth`, `is_seed` — the complete client list, including isolated seeds.
- `transactions`: `src`, `dst`, `date`, `sum_kzt` — individual transactions for the exported pairs.

The run checks unique node IDs and edges, known edge endpoints, positive finite amounts, valid depths, and exact pair/count agreement between transactions and edges. Floating sums are compared with a small tolerance for decimal representation. On the supplied data it finds 2,248 nodes (81 seeds), 3,119 edges, 4,840 transactions, and 365,890,012.01 KZT of recorded turnover; the maximum pair-sum discrepancy is below 0.000001 KZT. Nineteen seeds have no edge at all and remain in `nodes_roles.csv`, the graph, and community accounting; 31 seeds have no outgoing edge in the export.

The data description's 16 weak components are the components that contain edges. Counting the 19 isolated seed nodes as components gives 35 weak components in the full node set. Clustering uses Louvain, so its community count is different from either component count.

## Feature definitions and role rules

All thresholds below are recomputed from the input on every run. Quantiles use linear interpolation over the sorted node-level values. Degree means distinct counterparties, while `in_tx` and `out_tx` mean individual transfers. `in_kzt` and `out_kzt` are sums inside the observed graph.

For the supplied dataset, the computed cutoffs are:

| Metric | P75 | P90 | Use |
|---|---:|---:|---|
| `in_deg` | 1 | 2 | fan-in and coordinator rules |
| `out_deg` | 1 | 3 | fan-out and coordinator rules |
| directed weighted betweenness among nodes with nonzero betweenness | 0.0000973892 at P75 | — | coordinator rule |
| weighted PageRank over all nodes | 0.0004604633 at P75 | — | seed consolidator rule |

The graph stays directed for all flow features. Weighted PageRank follows edges in the payer-to-payee direction using `sum_kzt`. Directed betweenness uses Dijkstra/Brandes over the directed graph; a larger transfer gets a shorter path cost `1 / log(1 + sum_kzt)`. Betweenness is normalized by the directed node-pair count. The coordinator cutoff is calculated among positive betweenness values because the many zero values otherwise hide variation among actual intermediaries.

Rules are evaluated in this order:

1. **`terminal` at depth 4:** `out_deg == 0` and `depth == 4`. Score **0.45**. Evidence says the hop limit may have cut off further movement; this is not treated as proof the money settled.
2. **Observed-sample `terminal` before depth 4:** `out_deg == 0` and `in_deg > 0`. Score **0.82** for non-seeds. This means no qualifying outgoing edge appears in this extract, not that the account has no activity outside it. For seed nodes with visible incoming edges, score **0.50** and the evidence explicitly flags their incomplete incoming history. A zero-edge seed is `peripheral`, since the export has no flow evidence to support a terminal role.
3. **`coordinator`:** `in_deg >= max(1, P75(in_deg))`, `out_deg >= max(1, P75(out_deg))`, and positive directed betweenness at or above P75 of positive betweenness. Score **0.76**. This combines observed collection and forwarding links with shortest-path brokerage; it is a coordination candidate for review.
4. **`consolidator`, non-seed:** `in_deg >= max(1, P90(in_deg))` and `out_kzt / in_kzt < 0.50`. Score **0.72**. Multiple distinct payers and a low recorded pass-through are signs of accumulation.
5. **`consolidator`, seed exception:** seed `in_deg >= max(1, P90(in_deg))` and PageRank at or above P75, with at least one observed incoming and outgoing edge (the terminal rule has already been checked). Score **0.62**. No seed pass-through ratio is used; evidence marks the seed inflow as understated.
6. **`transit`, non-seed:** `in_deg >= max(1, P75(in_deg))` and `0.70 <= out_kzt / in_kzt <= 1.30`. Score **0.68**. The ratio is the fraction of *recorded* incoming value forwarded.
7. **`distributor`:** `out_deg >= max(1, P90(out_deg))`. Score **0.66**. The rule is based on the number of distinct recipients; outbound KZT and transaction counts explain the signal.
8. **`peripheral`:** none of the above. Score **0.50**, indicating weak role evidence rather than certainty about the client's real-world function.

Scores are fixed rule-strength labels, not calibrated probabilities. `evidence` is generated from the same node metrics and stays within 200 characters. It includes concrete counterparty and individual transaction counts, KZT amounts, and the seed/depth-four caveat when relevant. Wording describes signs that require verification.

## Data traps and limitations

1. **Hop-four truncation:** `truncated_by_depth` is true for `depth == 4 && out_deg == 0`. Such nodes receive a lower terminal score and an explicit hop-cutoff warning; they are never described as proven recipients.
2. **Understated seed inflows:** the graph starts from seed outflows, so a seed's observed `in_kzt` is incomplete. Seed evidence always includes that caveat. Seed pass-through is not used for either transit or the low-pass-through consolidator rule; the seed-specific consolidator rule uses degree and PageRank instead.
3. **Amounts and transaction counts are different signals:** the role logic uses unique payer/recipient degree and KZT pass-through where appropriate. Evidence reports transfer counts (`in_tx`, `out_tx`) separately from sums, so repeated smaller transfers cannot be mistaken for one large transfer. Counts and amounts are not treated as interchangeable.
4. **Direction and weight:** the analytical graph is directed and weighted. PageRank, flow sums, degrees, pass-through, and betweenness preserve payer-to-payee direction. Only seed reach and community detection use an undirected projection; Louvain combines reciprocal edge values by summing their KZT weights. That projection is used to group connected flow neighborhoods, not to infer flow direction.

Other limits: only intra-bank transfers in July 2026 at or above 5,000 KZT are present. Smaller transfers and possible structuring below that threshold are invisible. Outgoing-only traversal omits incoming flows from outside the sample, and the four-hop boundary hides additional edges. There are no names, balances, transaction types, ages, or other client attributes; none are inferred or enriched. The graph contains 16 edge-bearing weak components plus 19 isolated seed components, so one organization-wide interpretation would be unsupported. Role labels are structural hypotheses, not proof of intent or guilt.

## Clusters

Communities are found with multilevel Louvain modularity optimization on the undirected projection weighted by `sum_kzt` (resolution 1, deterministic random seed 41). Isolated nodes remain singleton communities. Cluster IDs are made reproducible by sorting communities by descending size and then minimum `gid`.

`clusters.csv` reports each community's node count, seed count, directed KZT turnover on edges whose endpoints are both in the community, and up to five highest-priority GIDs. Its hypothesis text summarizes whether seed nodes and consolidator/coordinator/distributor/transit signals are present, along with the counts and internal turnover. These are prompts for review, not claims about purpose.

## Review priority

Each metric is converted to an empirical percentile rank in `[0, 1]`; tied values share their average rank. The score is:

```text
priority_score = 0.30 * pagerank_rank
               + 0.25 * in_deg_rank
               + 0.25 * betweenness_rank
               + 0.10 * out_deg_rank
               + 0.10 * seed_reach_rank
```

PageRank and betweenness help surface influential and intermediary positions; incoming and outgoing degree add the number of counterparties; seed reach counts distinct seeds within two hops on the undirected local neighborhood. Percentile ranks keep money amounts from dominating count-based metrics. `top_nodes.csv` contains the top 100 by default, sorted by priority then `gid`; use `--top N` to change the size (minimum 20).

## Pipeline map

```text
Markdown / Parquet tables
        │
        ├── schema, endpoint, edge ↔ transaction consistency checks
        ▼
Directed weighted graph ──► flow metrics, PageRank, betweenness, seed reach
        │                                              │
        ├── undirected KZT projection ──► Louvain clusters
        ▼                                              ▼
thresholded roles + evidence ───────────────► cluster hypotheses
        │
        ▼
percentile priority ──► three CSV exports + self-contained HTML viewer
```

## Scaling to about one million nodes

The current exact all-sources weighted betweenness and in-memory adjacency fit this 2,248-node case; they are not appropriate for a million-node graph. At that size, store edges and attributes in a graph database such as Neo4j or ArangoDB, partition work by weak component/time window, and update affected neighborhoods incrementally. Replace full betweenness with sampled-source Brandes or another documented approximation, and compute PageRank with distributed or incremental iterations. Keep the dashboard focused on a searched node's bounded neighborhood and cluster summaries rather than embedding the full graph in one page. Recalculate global thresholds from a scheduled snapshot and version them alongside each output.

## Current run summary

The supplied Markdown files were run with the documented `python pipeline.py` command on Python 3.14. The full calculation and required exports took about 0.8 seconds locally. Output schemas, complete GID coverage, score ranges, cluster accounting, and ranking were checked. The Parquet adapter was not exercised here because the supplied inputs are Markdown exports and optional Parquet dependencies are not installed. Every run prints role counts, largest community sizes, thresholds, and the top five candidates; `out/analysis_summary.json` saves the numeric run summary.
