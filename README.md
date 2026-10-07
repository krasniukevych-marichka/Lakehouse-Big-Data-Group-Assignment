# TPC-H Lakehouse — Procurement

Group Assignment 1: migration of the TPC-H wholesale supplier data to a Lakehouse on Databricks (bronze → silver → gold) and answers to the business questions of the **Procurement** customer profile.

**Presentation:** https://canva.link/ythr7su1th2wqfp 

## Team

| member | responsibility |
|---|---|
| Krasniukevych Mariia | Bronze layer (`01_bronze`) |
| Sofia Trush | Silver layer, 3NF, data quality, profiling (`02_silver`) |
| Victoria Khimiak | Gold layer (`03_gold`) |
| Olha Kaplysh | Business questions, monitoring, alert, cross-layer reconciliation (`04_business_questions`) |

## Repository

| file | what it does |
|---|---|
| `01_bronze.py` | copies the 8 `samples.tpch` tables as is, adds load metadata |
| `02_silver.ipynb` | 3NF, types, `NOT NULL`, PK / FK, quarantine of bad rows, validation |
| `03_gold.ipynb` | gold tables for Procurement, reconciliation with silver |
| `04_business_questions.ipynb` | profiling, answers with visualisations, monitoring, alert with demo |
| `pyproject.toml`, `uv.lock` | Python project managed with [uv](https://docs.astral.sh/uv/) |

## How to run

1. In Databricks: **Workspace → Create → Git folder**, paste the repo URL.
2. Run the notebooks in order: `01_bronze` → `02_silver` → `03_gold` → `04_business_questions` (**Run all**).
3. Every notebook has the same widgets at the top. Use the same values in all four:
   - `target_catalog` — default `workspace`
   - `target_schema` — default `lakehouse_<your_login>`, so every team member works in a separate schema
   - `source_catalog` / `source_schema` (only in `01`) — default `samples.tpch`

Nothing is hard-coded to a workspace or user, so the notebooks run in any workspace with access to the pre-production `samples.tpch` data. Every notebook stops with an `assert` if a check fails.

Locally (optional, for editing and linting):

```bash
uv sync
```

## Layers

**Bronze** — `bronze_<table>`: the source as is. Only metadata columns are added: `_ingested_at`, `_source`, `_batch_id`. Check: row counts match the source.

**Silver** — `silver_<table>`: 3NF with enforced data quality.
- Explicit types, every column `NOT NULL`.
- `PRIMARY KEY` / `FOREIGN KEY` on all tables.
- 3NF: in `part`, `p_mfgr` depends on `p_brand`, not on the key, so it is moved to `silver_brand` (verified: every brand has exactly one manufacturer).
- Rows that break a rule go to quarantine, not to silver: `silver_partsupp_quarantine` and their line items in `silver_lineitem_quarantine`.
- Check: bronze rows = silver rows + quarantined rows for every table.

**Gold** — designed for the Procurement questions:

| table | grain | used for |
|---|---|---|
| `gold_supplier` | supplier | inventory value, complaints flag, nation, region |
| `gold_part_supplier` | (part, supplier) offer from partsupp, including offers never ordered | cost spread, cheapest supplier, single sourcing |
| `gold_supplier_spend_monthly` | month × supplier | spend share, concentration, cost by region over time |

## Definitions

Used by every query in the solution.

- **Spend** = `l_quantity * ps_supplycost`, using the supply cost of the line item's exact (part, supplier) pair. This is what we pay the supplier. `l_extendedprice` is not used, because it is the sale price to the customer.
- **Inventory value** = `ps_availqty * ps_supplycost`.
- **Supplier with complaints** = `s_comment LIKE '%Customer%Complaints%'`.
- **Trends over time** use complete quarters only (orders on every calendar day). Only 1998-Q3 is partial.

## Validation rules

| rule | how it is enforced |
|---|---|
| Every line item's (part, supplier) pair exists in partsupp | Two-column `FOREIGN KEY (l_partkey, l_suppkey) REFERENCES silver_partsupp (ps_partkey, ps_suppkey)`. Delta foreign keys are informational only, so the rule is also checked with a `LEFT ANTI JOIN` of lineitem on both columns; the check must return 0 rows. |
| Every supplier resolves to a valid nation, every nation to a valid region | Foreign keys + `LEFT ANTI JOIN` checks in `02_silver`. |
| Supply cost is positive and never above the part's retail price | Rows that break it are moved to `silver_partsupp_quarantine` (and their line items to `silver_lineitem_quarantine`), then checked in `02_silver`. |
| No double-counting across suppliers of the same part | `03_gold`: gold totals equal silver totals for spend, inventory value, line count and row count. |

**Cross-layer reconciliation** (`04_business_questions`): spend and inventory value are computed in every layer; bronze = silver + quarantine and silver = gold, tolerance $0.01.

## Profiling decisions

- **Quarantine:** 3,479 of 4,000,000 offers (0.087%) have supply cost above retail price; no cost is ≤ 0. They hold 0.17% of spend and are excluded from gold. After quarantine, 10 parts have only one valid supplier.
- **Complaints:** the pattern `Customer ... Complaints` finds 26 suppliers; a broader case-insensitive search for "complain" finds the same 26, so the pattern is complete.
- **Periods:** orders cover 1992-01-01 – 1998-08-02 with orders on every day, so only 1998-08 and 1998-Q3 are partial.

## Answers

1. **Top-10 suppliers by inventory value** hold 2.76B of 10.00T, or **0.028%** (0.02% if all were equal). No concentration.
2. **Brand#32:** for a typical part the most expensive supplier costs **~4.8×** the cheapest. We are **not** ordering from the cheapest: quantity is split evenly across suppliers (~25% each). With the cheapest supplier, spend would be 6.11B instead of 15.25B, an overpay of **9.14B (60%)**.
3. **Single supplier:** **0 parts.** Every part with several suppliers was ordered from at least two. The real exposure is the 10 parts that have only one valid supplier after quarantine.
4. **Complaints:** **26 suppliers**, **0.053%** of spend, the same as their share of suppliers. Complaints do not affect where we buy.

## Monitoring and alert

- **Spend concentration** (top-10 share per complete quarter): stable at ~0.045%.
- **Average supply cost by region** per complete quarter: ~500 per unit in every region.
- **Alert:** fires when one supplier gets more than **10× the equal share** of quarterly spend (0.02% with 50,000 suppliers). On real data the largest share is 0.0054%, so it fires 0 times. **Demo:** spend of one supplier in 1997-Q2 is multiplied by 10 in a temporary view; the alert fires only for that supplier and quarter.
