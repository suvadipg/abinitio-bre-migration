# Ab Initio BRE to Open Source Migration - Sample Project

## Overview
This project demonstrates how to convert **Ab Initio Business Rules Environment (BRE)** 
rules into an open-source Python-based rules engine integrated with **Apache Spark** 
for high-performance data processing.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  ORIGINAL (Ab Initio)                    │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐   │
│  │ BRE Rules│───>│ DML Xfm  │───>│ Co>Op Parallel   │   │
│  │ (XML/RDF)│    │ Component│    │ Execution Engine  │   │
│  └──────────┘    └──────────┘    └──────────────────┘   │
└─────────────────────────────────────────────────────────┘
                         │
                    MIGRATION
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│                  TARGET (Open Source)                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐   │
│  │ YAML/JSON│───>│ Python   │───>│ Apache Spark     │   │
│  │ Rules    │    │ Rules Eng│    │ (PySpark)        │   │
│  └──────────┘    └──────────┘    └──────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## What Gets Migrated

| Ab Initio BRE Concept     | Open Source Equivalent         |
|---------------------------|-------------------------------|
| Rulesets (XML/RDF)        | YAML rule definitions         |
| Decision Tables           | Pandas/Spark decision lookups |
| DML Transform expressions | Python rule functions         |
| Lookup files              | Broadcast variables / joins   |
| Rule chaining             | Rule dependency graph         |
| Audit/logging             | Python logging + Spark metrics|

## Project Structure
```
abinitio-bre-migration/
├── README.md
├── requirements.txt
├── config/
│   ├── abinitio_rules_sample.xml     # Original Ab Initio BRE rules
│   ├── converted_rules.yaml          # Converted open-source rules
│   └── decision_tables.yaml          # Decision table definitions
├── sample_data/
│   ├── transactions.csv              # Sample input data
│   └── customer_segments.csv         # Lookup/reference data
├── src/
│   ├── __init__.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── rule_models.py            # Rule data models
│   ├── converters/
│   │   ├── __init__.py
│   │   └── bre_converter.py          # Ab Initio BRE XML -> YAML converter
│   ├── rules_engine/
│   │   ├── __init__.py
│   │   ├── engine.py                 # Core rules engine
│   │   ├── decision_table.py         # Decision table processor
│   │   └── rule_functions.py         # Built-in rule functions
│   └── processors/
│       ├── __init__.py
│       └── spark_processor.py        # Spark integration layer
├── tests/
│   ├── __init__.py
│   ├── test_converter.py
│   ├── test_engine.py
│   └── test_spark_processor.py
├── main.py                           # Entry point / demo runner
└── docs/
    └── migration_guide.md
```

## Quick Start

```bash
pip install -r requirements.txt
python main.py
```

## How It Works

1. **Convert**: Parse Ab Initio BRE XML rules → YAML rule definitions
2. **Load**: Rules engine loads YAML rules into memory
3. **Execute**: PySpark applies rules to data in parallel via UDFs
4. **Audit**: Full rule execution logging and lineage tracking
