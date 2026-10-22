# Data Engineering Portfolio

Practical learning projects for data engineering, pipelines, and privacy.

## Projects

### 01 - PHI Masking Pipeline

**What:** Learn data masking techniques using healthcare data  
**Tech:** Python, DuckDB, YAML, SQL  
**Topics:** Privacy, PII detection, 3 masking strategies, schema-driven design

[Go to project →](./01-phi-masking) | [Deep guide](./01-phi-masking/MASKING_TECHNIQUES.md)

### 02 - Data Freshness Check

**What:** Check a table is fresh before using it, and ingest it on the fly if it isn't  
**Tech:** Python, DuckDB, YAML, SQL (mapped to BigQuery + Airflow)  
**Topics:** Freshness checks, idempotent ingestion, shared tables across DAGs, control tables

[Go to project →](./02-data-freshness-check)

### 03 - E-commerce Real-Time Pipeline

**What:** Stream shopper events through Pub/Sub into DuckDB, on your laptop  
**Tech:** Python, Pub/Sub (local SQLite broker or Google emulator), DuckDB, SQL, pytest  
**Topics:** Streaming ingestion, event-driven services, at-least-once delivery, idempotency, dead letters, data contracts, schema evolution, windows and watermarks, local-first development

[Go to project →](./03-ecommerce-realtime-pipeline) | [Deep guide](./03-ecommerce-realtime-pipeline/STREAMING_CONCEPTS.md)

## Quick Start

```bash
cd 01-phi-masking
pip install -r requirements.txt
python scripts/generate_data.py
python run_pipeline.py

cd ../02-data-freshness-check
pip install -r requirements.txt
python run_demo.py

cd ../03-ecommerce-realtime-pipeline
pip install -r requirements.txt
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

02-data-freshness-check/
├── config/freshness_rules.yaml   # Per-table freshness check
├── ingestion/                    # One idempotent SQL per table
├── common/ensure_fresh.py        # Check → ingest → re-check step
├── dags/                         # Two DAGs sharing one table
└── run_demo.py                   # Plays out 5 scenarios

03-ecommerce-realtime-pipeline/
├── config/                  # Topology (topics, subscriptions) + event contracts
├── pipeline/broker/         # Broker interface: local SQLite Pub/Sub + Google Pub/Sub
├── pipeline/services/       # producer, validator, aggregator, alerts, sink
├── sql/                     # DuckDB tables + bronze/silver/gold models
├── tests/                   # pytest: broker semantics, contracts, windows, end-to-end
├── README.md                # Project overview
└── STREAMING_CONCEPTS.md    # Detailed guide
```

## License

MIT - Use freely for learning, portfolios, and teaching.
