# Data Engineering Portfolio

Practical learning projects for data engineering, pipelines, and privacy.

## Projects

### 01 - PHI Masking Pipeline

**What:** Learn data masking techniques using healthcare data  
**Tech:** Python, DuckDB, YAML, SQL  
**Topics:** Privacy, PII detection, 3 masking strategies, schema-driven design

[Go to project →](./01-phi-masking) | [Deep guide](./01-phi-masking/MASKING_TECHNIQUES.md)

## Quick Start

```bash
cd 01-phi-masking
pip install -r requirements.txt
python scripts/generate_data.py
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
```

## License

MIT - Use freely for learning, portfolios, and teaching.
