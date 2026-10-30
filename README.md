# Data Engineering Portfolio

Practical learning projects for data engineering, pipelines, and privacy.

## Projects

### 01 - PHI Masking Pipeline

**What:** Learn data masking techniques using healthcare data  
**Tech:** Python, DuckDB, YAML, SQL  
**Topics:** Privacy, PII detection, 3 masking strategies, schema-driven design

[Go to project →](./01-phi-masking) | [Deep guide](./01-phi-masking/MASKING_TECHNIQUES.md)

### 02 - Ingestion and Freshness

**What:** Load data four ways, announce each load, and check freshness before a DAG uses a table  
**Tech:** Python, DuckDB, YAML, SQL (mapped to BigQuery + Airflow)  
**Topics:** Snapshot, incremental, file drop and CDC ingestion, write-audit-publish, watermarks, freshness checks, control tables

[Go to project →](./02-ingestion-and-freshness)

### 03 - E-commerce Real-Time Pipeline

**What:** Stream shopper events through Pub/Sub into DuckDB, on your laptop  
**Tech:** Python, Pub/Sub (local SQLite broker or Google emulator), DuckDB, SQL, pytest  
**Topics:** Streaming ingestion, event-driven services, at-least-once delivery, idempotency, dead letters, data contracts, schema evolution, windows and watermarks, local-first development

[Go to project →](./03-ecommerce-realtime-pipeline) | [Deep guide](./03-ecommerce-realtime-pipeline/STREAMING_CONCEPTS.md)

### 04 - Fraud Detection Rules

**What:** Flag suspicious bank transactions for analyst review, with the reasons each was flagged  
**Tech:** Python, pandas, pytest (mapped to BigQuery + Dataflow)  
**Topics:** Rules engine, per-account baselines, risk scoring, account takeover, precision and recall, label leakage

[Go to project →](./04-fraud-detection)

## Quick Start

```bash
cd 01-phi-masking
pip install -r requirements.txt
python scripts/generate_data.py
python run_pipeline.py

cd ../02-ingestion-and-freshness
pip install -r requirements.txt
python run_demo.py

cd ../03-ecommerce-realtime-pipeline
pip install -r requirements.txt
python run_pipeline.py

cd ../04-fraud-detection
pip install -r requirements.txt
python seed.py
python run_pipeline.py
```

## Structure

```
01-phi-masking/
├── config/schemas.yaml      # Schema + masking policy
├── pipeline/                # 4-stage pipeline (load, detect, mask, store)
├── data/                    # Sample data
├── README.md                # Project overview
└── MASKING_TECHNIQUES.md    # Detailed guide

02-ingestion-and-freshness/
├── config/tables.yaml            # Per table: ingestion type + freshness check
├── ingestion/                    # 4 ingestion types, write → audit → publish
├── freshness/ensure_fresh.py     # Check → ingest → re-check step
├── dags/                         # Two DAGs sharing one table
└── run_demo.py                   # Plays out 7 scenarios

03-ecommerce-realtime-pipeline/
├── config/                  # Topology (topics, subscriptions) + event contracts
├── pipeline/broker/         # Broker interface: local SQLite Pub/Sub + Google Pub/Sub
├── pipeline/services/       # producer, validator, aggregator, alerts, sink
├── sql/                     # DuckDB tables + bronze/silver/gold models
├── tests/                   # pytest: broker semantics, contracts, windows, end-to-end
├── README.md                # Project overview
└── STREAMING_CONCEPTS.md    # Detailed guide

04-fraud-detection/
├── seed.py                  # Synthetic accounts + transactions, injected account takeover
├── fraud/                   # rules, engine (risk score), evaluation
├── tests/                   # pytest: one file per rule
└── run_pipeline.py          # load → score → flag → CSV → precision/recall report
```

## License

MIT - Use freely for learning, portfolios, and teaching.
