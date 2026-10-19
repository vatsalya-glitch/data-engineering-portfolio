# Data Engineering Learning Hub

A collection of **practical learning projects** for data engineering, privacy, and pipeline development.

Each project is self-contained with clear code, documentation, and real-world scenarios. Perfect for **portfolio**, **interviews**, and **public learning**.

---

## 📚 Projects

### **01 - PHI Masking Pipeline** `[Complete]`

**Focus:** Data masking techniques and privacy-preserving data handling

Learn three fundamental masking strategies:
- **One-way hashing** (deterministic, linkable)
- **Redaction** (irreversible destruction)
- **Flag + Preserve** (conditional access)

**Scenario:** Healthcare data (patient records, appointments)  
**Tech Stack:** Python, DuckDB, YAML, SQL  
**Duration:** End-to-end pipeline (4 stages)

📂 [Go to Project →](./01-phi-masking)

**Key Learnings:**
- How to identify PII systematically
- Choosing masking strategies based on use case
- Maintaining referential integrity while masking
- Creating safe analytics views from sensitive data
- Privacy-preserving data engineering

---

## 🎯 Design Philosophy

Each project follows these principles:

1. **Simple & Educational** - Clear code, not production-ready complexity
2. **Complete** - End-to-end solution you can run
3. **Well-Documented** - README + deep guides
4. **Real Scenarios** - Based on realistic problems
5. **Hands-On** - Learn by doing, not reading

---

## 📁 Repository Structure

```
data-engineering-learning/
├── 01-phi-masking/              ← Project 1
│   ├── config/                  ← Configuration (YAML)
│   ├── pipeline/                ← Core code (Python)
│   ├── data/                    ← Sample data
│   ├── scripts/                 ← Utilities
│   ├── README.md                ← Project docs
│   ├── MASKING_TECHNIQUES.md    ← Deep guide
│   └── run_pipeline.py          ← Entry point
│
├── 02-next-project/             ← Project 2 (coming soon)
│   ├── ...
│   └── README.md
│
└── README.md                    ← This file
```

---

## 🚀 Quick Start

Each project has its own setup. For the first project:

```bash
cd 01-phi-masking

# Install dependencies
pip install -r requirements.txt

# Generate sample data
python scripts/generate_data.py

# Run complete pipeline
python run_pipeline.py
```

---

## 💡 Use Cases

### For Portfolios
- Showcase data engineering skills
- Demonstrate privacy awareness
- Show code quality and documentation

### For Learning
- Understand masking techniques
- Learn SQL + Python data pipelines
- See schema-driven design in practice

### For Interviews
- Discuss design decisions
- Explain tradeoffs (privacy vs utility)
- Walk through real code examples

### For Teaching
- Share with others learning data engineering
- Adaptable for different domains
- MIT License - use freely

---

## 📖 What You'll Learn

### Project 01: PHI Masking

After completing this project, you understand:

✅ **Masking Strategy Selection**
- When to hash vs redact vs flag
- Privacy-utility tradeoffs

✅ **Practical Implementation**
- Write masking logic in Python/SQL
- Schema-driven configuration
- Multi-stage data pipelines

✅ **Privacy Engineering**
- How to identify PII systematically
- Referential integrity in masked data
- Safe analytics views from sensitive data

✅ **Data Quality**
- Schema validation
- Audit trails for compliance
- Verification testing

---

## 🛠️ Tech Stack (Across Projects)

- **Python** - Data processing, orchestration
- **DuckDB** - Fast SQL database (single file)
- **YAML** - Configuration & policy
- **SQL** - Data transformation
- **Git** - Version control

---

## 📝 Contributing

This is a learning repository. If you:
- Find issues or bugs → Report them
- Have improvements → Suggest them
- Want to add projects → Create a discussion

---

## 📄 License

MIT License - Use these projects freely for learning, portfolios, and teaching.

---

## 🔗 Quick Links

- **Project 01:** [PHI Masking Pipeline](./01-phi-masking)
  - [README](./01-phi-masking/README.md)
  - [Masking Techniques Guide](./01-phi-masking/MASKING_TECHNIQUES.md)

---

**Last Updated:** October 2024  
**Status:** 1 project complete, more coming soon 🚀
