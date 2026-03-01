# Ab Initio BRE Migration Guide

## Concept Mapping

This table maps every Ab Initio BRE concept to its open-source equivalent
in this project:

| Ab Initio BRE | Open Source | File |
|---|---|---|
| BRE Ruleset (XML/RDF) | YAML Rule Definition | `config/converted_rules.yaml` |
| DML Expression | Python Expression | `src/converters/bre_converter.py` |
| Rule Condition | `record['field'] > value` | `src/rules_engine/engine.py` |
| Rule Action (set_field) | `RuleAction` dataclass | `src/models/rule_models.py` |
| Decision Table | YAML matrix + evaluator | `src/rules_engine/decision_table.py` |
| Lookup (set) | Python set in memory | `config/converted_rules.yaml` → lookups |
| Lookup (key-value) | Python dict / CSV | Same as above |
| First-match semantics | `stop_on_first_match: true` | YAML config |
| All-match semantics | `stop_on_first_match: false` | YAML config |
| Rule priority | `priority` field (lower = higher) | YAML config |
| Co>Op parallel exec | PySpark partitions + UDF | `src/processors/spark_processor.py` |
| GDE monitoring | Spark UI + logging | Spark built-in |
| BRE audit trail | `__rules_fired`, `__execution_time_ms` | Engine output |

## DML Expression Conversion Reference

Common Ab Initio DML patterns and their Python equivalents:

```
# Comparison
DML:    transaction_amount > 10000
Python: record['transaction_amount'] > 10000

# String equality
DML:    customer_segment == "PREMIUM"
Python: record['customer_segment'] == 'PREMIUM'

# Compound condition
DML:    amount > 5000 and is_intl == "Y"
Python: record['amount'] > 5000 and record['is_intl'] == 'Y'

# Null check
DML:    is_null(field_name)
Python: record['field_name'] is None

# String functions
DML:    string_length(name) > 5
Python: len(str(record['name'])) > 5

# Lookup membership
DML:    country in_lookup "bad_countries"
YAML:   condition.type: lookup, field: country, lookup_name: bad_countries
```

## Performance Tuning Guide

| Ab Initio Setting | Spark Equivalent | How to Set |
|---|---|---|
| Number of partitions | `num_partitions` | `SparkRulesProcessor(spark, num_partitions=N)` |
| Maximum core memory | `spark.driver.memory` | `create_spark_session(driver_memory="4g")` |
| Parallelism | `spark.default.parallelism` | Spark config |
| Lookup broadcast | `spark.broadcast()` | Automatic in `SparkRulesProcessor` |
| Sort/merge join | `spark.sql.autoBroadcastJoinThreshold` | Spark config |

## Testing Strategy

1. **Unit tests**: Test each rule in isolation (`tests/test_engine.py`)
2. **Regression tests**: Compare outputs against Ab Initio baseline
3. **Performance tests**: Benchmark Spark execution against Ab Initio runtimes
4. **Data validation**: Row counts + checksums on output data
