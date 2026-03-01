"""
Spark Rules Processor
=====================
Integrates the rules engine with Apache Spark for parallel execution.

This module is the open-source equivalent of Ab Initio's Co>Operating System
executing BRE transform components in parallel across partitions.

Key Performance Optimizations (matching Ab Initio):
  1. Rules are broadcast to all executors (like Ab Initio's lookup broadcast)
  2. Rule evaluation happens via PySpark UDF (parallel per partition)
  3. Lookups are broadcast variables (like Ab Initio's in-memory lookups)
  4. Partitioning can be tuned to match Ab Initio's layout/partition settings
  5. Decision tables are broadcast for fast local lookup

Architecture:
  ┌──────────────────────────────────────────────────────┐
  │                  Spark Driver                         │
  │  ┌────────────┐  ┌────────────┐  ┌───────────────┐  │
  │  │ Rules YAML │  │ Lookup Data│  │ Decision Tbls │  │
  │  └─────┬──────┘  └─────┬──────┘  └───────┬───────┘  │
  │        │               │                  │          │
  │        └───────┬───────┴──────────────────┘          │
  │                │ broadcast()                          │
  │                ▼                                      │
  │  ┌──────────────────────────────────┐                │
  │  │   Broadcast Variables            │                │
  │  │   (sent to all executors once)   │                │
  │  └──────────────┬───────────────────┘                │
  └─────────────────┼────────────────────────────────────┘
                    │
    ┌───────────────┼───────────────────────┐
    │               ▼                       │
    │  ┌─────────────────────────────────┐  │
    │  │     Spark Executors             │  │
    │  │  ┌───────┐ ┌───────┐ ┌───────┐ │  │
    │  │  │Part 0 │ │Part 1 │ │Part N │ │  │
    │  │  │ UDF() │ │ UDF() │ │ UDF() │ │  │
    │  │  │  ▼    │ │  ▼    │ │  ▼    │ │  │
    │  │  │Engine │ │Engine │ │Engine │ │  │
    │  │  │.eval()│ │.eval()│ │.eval()│ │  │
    │  │  └───────┘ └───────┘ └───────┘ │  │
    │  └─────────────────────────────────┘  │
    └───────────────────────────────────────┘
"""

import json
import logging
import time
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, IntegerType, MapType, ArrayType
)

from src.rules_engine.engine import RulesEngine

logger = logging.getLogger(__name__)


class SparkRulesProcessor:
    """
    Apache Spark integration for the rules engine.
    
    Replaces Ab Initio's Co>Operating System for parallel rule execution.
    
    Usage:
        processor = SparkRulesProcessor(spark_session)
        processor.load_rules("config/converted_rules.yaml")
        result_df = processor.apply_rules(input_df)
    """
    
    def __init__(self, spark: SparkSession, num_partitions: int = None):
        """
        Initialize the Spark rules processor.
        
        Args:
            spark: Active SparkSession
            num_partitions: Number of partitions for parallel processing.
                           Maps to Ab Initio's "number of partitions" layout setting.
                           None = use Spark's default.
        """
        self.spark = spark
        self.num_partitions = num_partitions
        self.engine = RulesEngine()
        self._rules_config = None
        self._broadcast_config = None
    
    def load_rules(self, yaml_path: str) -> 'SparkRulesProcessor':
        """
        Load rules and broadcast to all executors.
        
        This is equivalent to Ab Initio loading the BRE ruleset
        and broadcasting lookup data at graph startup.
        """
        import yaml
        with open(yaml_path, 'r') as f:
            self._rules_config = yaml.safe_load(f)
        
        # Initialize the local engine for validation
        self.engine.load_rules_from_dict(self._rules_config)
        
        # Validate rules
        issues = self.engine.validate()
        if issues:
            for issue in issues:
                logger.warning(f"Rule validation issue: {issue}")
        
        # Broadcast the rules config to all executors
        # This is analogous to Ab Initio's lookup broadcast mechanism
        config_json = json.dumps(self._rules_config)
        self._broadcast_config = self.spark.sparkContext.broadcast(config_json)
        
        logger.info(
            f"Rules broadcast to executors: {self.engine.get_stats()}"
        )
        return self

    def apply_rules(
        self, 
        input_df: DataFrame,
        output_columns: list[str] = None,
        include_audit: bool = True
    ) -> DataFrame:
        """
        Apply business rules to a Spark DataFrame in parallel.
        
        This is the main processing method — equivalent to running data 
        through an Ab Initio BRE transform component in a parallel graph.
        
        Args:
            input_df: Input Spark DataFrame
            output_columns: Specific output columns to include (None = all)
            include_audit: Include __rules_fired and __execution_time_ms columns
            
        Returns:
            DataFrame with original columns + rule-generated columns
        """
        start_time = time.time()
        
        # Repartition if configured (matches Ab Initio partition count)
        if self.num_partitions:
            input_df = input_df.repartition(self.num_partitions)
        
        # Get the broadcast config reference for the UDF closure
        broadcast_ref = self._broadcast_config
        
        # Define the UDF that evaluates rules on each row
        # This runs in parallel across all Spark executors
        @F.udf(returnType=MapType(StringType(), StringType()))
        def evaluate_rules_udf(*cols):
            """
            PySpark UDF that evaluates the rules engine on each row.
            
            Each executor deserializes the broadcast rules config and
            creates a local RulesEngine instance (cached per partition
            in practice via Spark's UDF optimization).
            """
            # Build record dict from column values
            record = {}
            col_names = input_df.columns
            for i, val in enumerate(cols):
                if i < len(col_names):
                    record[col_names[i]] = val
            
            # Initialize engine from broadcast config
            config = json.loads(broadcast_ref.value)
            engine = RulesEngine()
            engine.load_rules_from_dict(config)
            
            # Evaluate rules
            result = engine.evaluate(record)
            
            # Convert all values to strings for MapType compatibility
            return {k: str(v) for k, v in result.items()}
        
        # Apply the UDF to all rows
        input_cols = [F.col(c) for c in input_df.columns]
        result_df = input_df.withColumn(
            '_rule_results',
            evaluate_rules_udf(*input_cols)
        )
        
        # Extract rule-generated fields from the map column
        result_columns = self._get_output_columns()
        
        for col_name in result_columns:
            col_type = self._infer_column_type(col_name)
            result_df = result_df.withColumn(
                col_name,
                F.col('_rule_results').getItem(col_name).cast(col_type)
            )
        
        # Add audit columns if requested
        if include_audit:
            result_df = result_df.withColumn(
                '__rules_fired',
                F.col('_rule_results').getItem('__rules_fired')
            )
            result_df = result_df.withColumn(
                '__execution_time_ms',
                F.col('_rule_results').getItem('__execution_time_ms').cast(DoubleType())
            )
        
        # Drop the intermediate map column
        result_df = result_df.drop('_rule_results')
        
        # Filter output columns if specified
        if output_columns:
            keep_cols = list(input_df.columns) + output_columns
            if include_audit:
                keep_cols += ['__rules_fired', '__execution_time_ms']
            result_df = result_df.select(
                *[c for c in keep_cols if c in result_df.columns]
            )
        
        elapsed = time.time() - start_time
        logger.info(f"Rules applied in {elapsed:.2f}s across {input_df.rdd.getNumPartitions()} partitions")
        
        return result_df

    def _get_output_columns(self) -> list[str]:
        """Get the list of output columns generated by rules."""
        output_cols = set()
        
        for ruleset in self._rules_config.get('rulesets', []):
            for rule in ruleset.get('rules', []):
                for action in rule.get('actions', []):
                    output_cols.add(action['field'])
        
        for dt in self._rules_config.get('decision_tables', []):
            for col in dt.get('output_columns', []):
                output_cols.add(col)
        
        return sorted(output_cols)

    def _infer_column_type(self, col_name: str):
        """Infer Spark column type from rule actions."""
        # Heuristic based on common patterns
        if col_name.endswith('_amount') or col_name.endswith('_rate') or \
           col_name.endswith('_score') or col_name.endswith('_pct') or \
           col_name.endswith('_surcharge'):
            return DoubleType()
        elif col_name.endswith('_required') or col_name.endswith('_applied') or \
             col_name.startswith('alert_') or col_name.endswith('_processing'):
            return BooleanType()
        else:
            return StringType()

    def get_performance_metrics(self, result_df: DataFrame) -> dict:
        """
        Get performance metrics from a processed DataFrame.
        
        Equivalent to Ab Initio's graph execution statistics.
        """
        metrics = result_df.select(
            F.count('*').alias('total_records'),
            F.avg('__execution_time_ms').alias('avg_rule_eval_ms'),
            F.max('__execution_time_ms').alias('max_rule_eval_ms'),
            F.min('__execution_time_ms').alias('min_rule_eval_ms')
        ).collect()[0]
        
        return {
            'total_records': metrics['total_records'],
            'avg_rule_eval_ms': round(metrics['avg_rule_eval_ms'], 3),
            'max_rule_eval_ms': round(metrics['max_rule_eval_ms'], 3),
            'min_rule_eval_ms': round(metrics['min_rule_eval_ms'], 3),
        }


def create_spark_session(
    app_name: str = "BRE_Migration",
    num_cores: str = "local[*]",
    driver_memory: str = "2g"
) -> SparkSession:
    """
    Create a SparkSession configured for rules processing.
    
    Tuning parameters map to Ab Initio equivalents:
      - num_cores → Ab Initio "number of partitions" / parallelism
      - driver_memory → Ab Initio "maximum core" memory setting
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master(num_cores)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
