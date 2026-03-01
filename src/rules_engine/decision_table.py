"""
Decision Table Processor
========================
Handles Ab Initio BRE Decision Table evaluation.

Ab Initio BRE decision tables are matrix-style rule lookups where
rows define input ranges/conditions and output values. They're commonly
used for:
  - Customer segmentation / tiering
  - Fee schedule lookups
  - Risk scoring matrices
  - Product eligibility determination

This module provides both:
  1. In-memory evaluation (for use in PySpark UDFs)
  2. Spark DataFrame-based evaluation (using join/filter operations)
"""

import logging
from typing import Any, Optional
import yaml

logger = logging.getLogger(__name__)


class DecisionTableProcessor:
    """
    Evaluates decision tables against records.
    
    Supports:
    - Range-based inputs (min/max boundaries)
    - Exact-match inputs
    - Multiple output columns per match
    - First-match semantics (returns first matching row)
    """
    
    def __init__(self):
        self.tables: dict[str, dict] = {}

    def load_from_yaml(self, yaml_path: str) -> 'DecisionTableProcessor':
        """Load decision tables from YAML config."""
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
        
        for dt in config.get('decision_tables', []):
            self.tables[dt['name']] = dt
            logger.info(
                f"Loaded decision table '{dt['name']}' with "
                f"{len(dt['rows'])} rows"
            )
        
        return self

    def load_table(self, name: str, table_config: dict):
        """Load a single decision table from config dict."""
        self.tables[name] = table_config

    def evaluate(self, table_name: str, record: dict) -> Optional[dict]:
        """
        Evaluate a record against a named decision table.
        
        Args:
            table_name: Name of the decision table
            record: Dict of field_name → value
            
        Returns:
            Dict of output column values if matched, None if no match
        """
        table = self.tables.get(table_name)
        if not table:
            logger.warning(f"Decision table '{table_name}' not found")
            return None
        
        for row in table.get('rows', []):
            if self._matches_row(row, record):
                return dict(row['outputs'])
        
        logger.debug(
            f"No matching row in decision table '{table_name}' "
            f"for record: {record}"
        )
        return None

    def _matches_row(self, row: dict, record: dict) -> bool:
        """Check if a record matches a decision table row."""
        for col_name, condition in row.get('inputs', {}).items():
            value = record.get(col_name)
            if value is None:
                return False
            
            # Range condition
            if isinstance(condition, dict) and 'min' in condition and 'max' in condition:
                try:
                    num_val = float(value)
                    if not (condition['min'] <= num_val < condition['max']):
                        return False
                except (ValueError, TypeError):
                    return False
            
            # Exact match
            elif isinstance(condition, dict) and 'value' in condition:
                if str(value) != str(condition['value']):
                    return False
            else:
                # Direct comparison
                if str(value) != str(condition):
                    return False
        
        return True

    def evaluate_batch(self, table_name: str, records: list[dict]) -> list[Optional[dict]]:
        """Evaluate multiple records against a decision table."""
        return [self.evaluate(table_name, record) for record in records]

    def get_table_info(self, table_name: str) -> dict:
        """Get information about a decision table."""
        table = self.tables.get(table_name, {})
        return {
            'name': table_name,
            'input_columns': [c['name'] for c in table.get('input_columns', [])],
            'output_columns': table.get('output_columns', []),
            'row_count': len(table.get('rows', []))
        }
