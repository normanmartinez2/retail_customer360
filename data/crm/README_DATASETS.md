# Retail Customer 360 — Synthetic source datasets (LAB 04–12)

All data is synthetic. Generated deterministically with seed `20261010`. This package does **not** replace the original 1,220-row CRM customer CSV.

## Source datasets / intended GitHub paths

| Dataset | Rows | Source simulated |
|---|---:|---|
| `data/erp/products_lab04_v1.csv` | 500 | SAP ERP |
| `data/ecommerce/orders_lab04_v1.csv` | 15,000 | E-commerce |
| `data/ecommerce/order_items_lab04_v1.csv` | 40,000 | E-commerce |
| `data/payments/payments_lab04_v1.csv` | 15,500 | Payment gateway |
| `data/marketing/customer_interactions_lab04_v1.csv` | 8,000 | Marketing CRM |
| `data/cdc/customers_cdc_lab06_v1.csv` | 180 | CRM CDC |
| `data/cdc/orders_cdc_lab06_v1.csv` | 300 | E-commerce CDC |
| `data/cdc/payments_cdc_lab06_v1.csv` | 200 | Gateway CDC |

### Existing CRM file (not duplicated here)
`https://raw.githubusercontent.com/normanmartinez2/retail_customer360/refs/heads/main/data/crm/customers_lab02_v1.csv`

It has 1,220 rows; 1,190 unique nonblank customer IDs, of which about 1,153 satisfy the LAB 03 key/email/date/country/consent rules. All valid referenced customer IDs in the new datasets come from those 1,153 keys. Missing, quarantined, and unknown FKs are deliberately planted.

## Bronze ingestion contracts

- Treat every source column as STRING in Bronze. Never coerce dates or decimal numbers until the Silver data quality step.
- Preserve source `event_timestamp`, `updated_at`, `ingestion_timestamp`, original source rows, and ingestion metadata.
- Version each CSV with its filename; avoid overwriting historic Bronze tables during subsequent CDC loads.
- Use the `dataset_manifest.json` for exact column names, checksums, dataset volumes, and planted DQ cases.
- All monetary figures are in EUR, and all timestamp strings should be interpreted as UTC. A small number of invalid currency/timestamp values exist by design.

## Referential model

`CRM customers (C...)` ← `orders (O...)` ← `order_items (LI...)` → `products (P...)`

`orders (O...)` ← `payments (PAY...)`

`CRM customers (C...)` ← `customer_interactions (INT...)`

An order can have multiple items and multiple payment events. Item totals should reconcile against order subtotals *except for deliberate data-quality mutations*. Refund payments have negative `amount_eur`; not every charge is captured/successful, and order statuses affect business revenue definitions.

## SCD2 and CDC

CDC files are **separate event streams** used in LAB 06, not an extension to the initial LAB 04 snapshots.
- `event_id` is the deduplication key for replayed events.
- `source_sequence` provides order per business key and may be out of arrival order.
- `event_timestamp` is source/event time; `ingestion_timestamp` is arrival time.
- CDC UPDATE carries partial changed attributes (`new_*` or the supplied nonempty fields); blank means leave existing value unchanged.
- CDC DELETE is a tombstone; decide soft-delete semantics and SCD2 expiry.
- Validate state transitions, late events, replays, conflicts, and event identity before applying MERGE.
- These synthetic streams are training fixtures, not a full production-grade CDC protocol.

## Suggested lab sequence

- LAB 04: land the five new source CSVs in Unity Catalog Volumes; ingest Bronze Delta tables.
- LAB 05: clean, normalize, validate cross-source foreign keys; quarantine invalid records; reconcile orders/items/payments.
- LAB 06: ingest CDC CSVs, perform replay-safe Delta MERGE, SCD2, event sequencing, and late-arrival checks.
- LAB 07: build Gold `customer_360`, `sales_analytics`, `customer_segments`.
- LAB 08: Databricks SQL / KPIs (paid net revenue vs gross order amount).
- LAB 09: AI/BI and Genie on Gold views, with governed definitions.
- LAB 10: Lakeflow Jobs orchestration and monitoring.
- LAB 11–12: architecture reviews and rehearsed end-to-end Solutions Architect demo.

All data generation logic is included separately in `build_retail_customer360_sources.py` in the downloadable package root.
