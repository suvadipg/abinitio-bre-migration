"""
Open-Source Rules Engine
========================
This is the core rules execution engine — the open-source replacement for
Ab Initio's BRE runtime. It loads YAML rules and evaluates them against
data records, preserving Ab Initio BRE's execution semantics:

  - Priority-based rule ordering (lower number = higher priority)
  - First-match mode (stop after first matching rule in a ruleset)
  - All-match mode (fire all matching rules, accumulate results)
  - Lookup-based conditions (set membership, key-value lookups)
  - Decision table evaluation (range-based matrix lookups)
  - Rule execution audit trail

Performance Notes:
  - Rules are compiled once at load time
  - Lookups are pre-loaded into memory (like Ab Initio's in-memory lookups)
  - The engine is designed to be called as a PySpark UDF for parallel execution
  - No I/O during rule evaluation — everything is in-memory
"""

import time
import logging
import yaml
from typing import Any, Optional

from src.models.rule_models import (
    Rule, RuleSet, RuleAction, RuleExecutionResult,
    DecisionTable, DecisionTableRow, LookupDefinition,
    ConditionType, ActionType
)

logger = logging.getLogger(__name__)


class RulesEngine:
    """
    Core rules execution engine.
    
    Replaces Ab Initio BRE's runtime evaluation of business rules.
    
    Usage:
        engine = RulesEngine()
        engine.load_rules("config/converted_rules.yaml")
        result = engine.evaluate(record_dict)
    """
    
    def __init__(self):
        self.rulesets: list[RuleSet] = []
        self.decision_tables: list[DecisionTable] = []
        self.lookups: dict[str, LookupDefinition] = {}
        self._compiled_rules: dict[str, callable] = {}
        self._loaded = False

    # ─────────────────────────────────────────────────────────────
    # LOADING & COMPILATION
    # ─────────────────────────────────────────────────────────────
    
    def load_rules(self, yaml_path: str) -> 'RulesEngine':
        """
        Load rules from a YAML configuration file.
        
        This is the equivalent of Ab Initio BRE loading rulesets
        at graph initialization time.
        """
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self._load_lookups(config.get('lookups', {}))
        self._load_rulesets(config.get('rulesets', []))
        self._load_decision_tables(config.get('decision_tables', []))
        self._compile_rules()
        
        self._loaded = True
        logger.info(
            f"Rules engine loaded: {len(self.rulesets)} rulesets, "
            f"{sum(len(rs.rules) for rs in self.rulesets)} rules, "
            f"{len(self.decision_tables)} decision tables, "
            f"{len(self.lookups)} lookups"
        )
        return self

    def load_rules_from_dict(self, config: dict) -> 'RulesEngine':
        """Load rules from an already-parsed dict (useful for testing)."""
        self._load_lookups(config.get('lookups', {}))
        self._load_rulesets(config.get('rulesets', []))
        self._load_decision_tables(config.get('decision_tables', []))
        self._compile_rules()
        self._loaded = True
        return self

    def _load_lookups(self, lookups_config: dict):
        """Load lookup definitions into memory."""
        for name, lookup_cfg in lookups_config.items():
            lookup_type = lookup_cfg.get('type', 'set')
            
            if lookup_type == 'set':
                values = set(lookup_cfg.get('values', []))
                self.lookups[name] = LookupDefinition(
                    name=name, lookup_type='set', values=values
                )
            elif lookup_type == 'key_value':
                values = lookup_cfg.get('values', {})
                self.lookups[name] = LookupDefinition(
                    name=name, lookup_type='key_value', values=values,
                    source=lookup_cfg.get('path')
                )

    def _load_rulesets(self, rulesets_config: list):
        """Load ruleset definitions."""
        for rs_cfg in rulesets_config:
            rules = []
            for rule_cfg in rs_cfg.get('rules', []):
                rule = self._build_rule(rule_cfg)
                rules.append(rule)
            
            ruleset = RuleSet(
                name=rs_cfg['name'],
                description=rs_cfg.get('description', ''),
                priority=rs_cfg.get('priority', 100),
                rules=rules,
                active=rs_cfg.get('active', True),
                stop_on_first_match=rs_cfg.get('stop_on_first_match', True)
            )
            self.rulesets.append(ruleset)
        
        # Sort rulesets by priority
        self.rulesets.sort(key=lambda rs: rs.priority)

    def _build_rule(self, rule_cfg: dict) -> Rule:
        """Build a Rule object from YAML config."""
        condition = rule_cfg.get('condition', {})
        condition_type = ConditionType(condition.get('type', 'expression'))
        
        actions = []
        for action_cfg in rule_cfg.get('actions', []):
            action_type = ActionType.COMPUTED if action_cfg.get('type') == 'computed' else ActionType.SET_VALUE
            actions.append(RuleAction(
                field=action_cfg['field'],
                value=action_cfg.get('value'),
                expression=action_cfg.get('expression'),
                action_type=action_type
            ))
        
        return Rule(
            name=rule_cfg['name'],
            priority=rule_cfg.get('priority', 50),
            condition_type=condition_type,
            condition_expression=condition.get('expression'),
            condition_lookup_field=condition.get('field'),
            condition_lookup_name=condition.get('lookup_name'),
            actions=actions,
            description=rule_cfg.get('description', ''),
            enabled=rule_cfg.get('enabled', True)
        )

    def _load_decision_tables(self, dt_configs: list):
        """Load decision table definitions."""
        for dt_cfg in dt_configs:
            rows = []
            for row_cfg in dt_cfg.get('rows', []):
                rows.append(DecisionTableRow(
                    inputs=row_cfg.get('inputs', {}),
                    outputs=row_cfg.get('outputs', {})
                ))
            
            self.decision_tables.append(DecisionTable(
                name=dt_cfg['name'],
                description=dt_cfg.get('description', ''),
                input_columns=dt_cfg.get('input_columns', []),
                output_columns=dt_cfg.get('output_columns', []),
                rows=rows
            ))

    def _compile_rules(self):
        """
        Pre-compile rule expressions for performance.
        
        In Ab Initio, DML expressions are compiled at graph load time.
        Here, we compile Python expressions into callable code objects.
        """
        for ruleset in self.rulesets:
            for rule in ruleset.rules:
                if rule.condition_type == ConditionType.EXPRESSION and rule.condition_expression:
                    try:
                        compiled = compile(
                            rule.condition_expression, 
                            f'<rule:{rule.name}>', 
                            'eval'
                        )
                        self._compiled_rules[rule.name] = compiled
                    except SyntaxError as e:
                        logger.error(f"Failed to compile rule '{rule.name}': {e}")
                
                # Compile action expressions too
                for action in rule.actions:
                    if action.action_type == ActionType.COMPUTED and action.expression:
                        try:
                            compiled = compile(
                                action.expression,
                                f'<action:{rule.name}.{action.field}>',
                                'eval'
                            )
                            key = f"{rule.name}__action__{action.field}"
                            self._compiled_rules[key] = compiled
                        except SyntaxError as e:
                            logger.error(
                                f"Failed to compile action '{rule.name}.{action.field}': {e}"
                            )

    # ─────────────────────────────────────────────────────────────
    # RULE EVALUATION (Core Engine)
    # ─────────────────────────────────────────────────────────────

    def evaluate(self, record: dict) -> dict:
        """
        Evaluate all rulesets against a single record.
        
        This is the main entry point — equivalent to passing a record
        through an Ab Initio BRE transform component.
        
        Args:
            record: Dict of field_name → value (one data record)
            
        Returns:
            Dict with original fields + all fields set by rules
        """
        start_time = time.time()
        result = dict(record)  # Copy to avoid mutation
        rules_fired = []
        errors = []
        
        # Execute rulesets in priority order
        for ruleset in self.rulesets:
            if not ruleset.active:
                continue
            
            rs_result = self._evaluate_ruleset(ruleset, result)
            result.update(rs_result['fields'])
            rules_fired.extend(rs_result['rules_fired'])
            errors.extend(rs_result.get('errors', []))
        
        # Execute decision tables
        for dt in self.decision_tables:
            dt_result = self._evaluate_decision_table(dt, result)
            result.update(dt_result)
        
        # Add audit metadata
        result['__rules_fired'] = rules_fired
        result['__execution_time_ms'] = round((time.time() - start_time) * 1000, 2)
        result['__errors'] = errors
        
        return result

    def _evaluate_ruleset(self, ruleset: RuleSet, record: dict) -> dict:
        """
        Evaluate a single ruleset against a record.
        
        Respects the ruleset's execution mode:
        - stop_on_first_match=True: Return after first matching rule (Ab Initio default)
        - stop_on_first_match=False: Apply all matching rules
        """
        fields = {}
        rules_fired = []
        errors = []
        
        for rule in ruleset.get_sorted_rules():
            try:
                if self._evaluate_condition(rule, record):
                    # Rule matched — execute actions
                    action_results = self._execute_actions(rule, record)
                    fields.update(action_results)
                    rules_fired.append(f"{ruleset.name}.{rule.name}")
                    
                    # Update record for subsequent rules in this ruleset
                    record.update(action_results)
                    
                    if ruleset.stop_on_first_match:
                        break  # First-match semantics
                        
            except Exception as e:
                error_msg = f"Error evaluating rule '{rule.name}': {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        return {'fields': fields, 'rules_fired': rules_fired, 'errors': errors}

    def _evaluate_condition(self, rule: Rule, record: dict) -> bool:
        """
        Evaluate a rule's condition against a record.
        
        Supports:
        - Expression conditions (Python expressions)
        - Lookup conditions (set membership)
        """
        if rule.condition_type == ConditionType.EXPRESSION:
            compiled = self._compiled_rules.get(rule.name)
            if compiled:
                return bool(eval(compiled, {"__builtins__": {}}, {"record": record}))
            return False
        
        elif rule.condition_type == ConditionType.LOOKUP:
            lookup = self.lookups.get(rule.condition_lookup_name)
            if lookup and lookup.lookup_type == 'set':
                field_value = record.get(rule.condition_lookup_field)
                return field_value in lookup.values
            return False
        
        return False

    def _execute_actions(self, rule: Rule, record: dict) -> dict:
        """Execute all actions for a matched rule."""
        results = {}
        
        for action in rule.actions:
            if action.action_type == ActionType.SET_VALUE:
                results[action.field] = action.value
            
            elif action.action_type == ActionType.COMPUTED:
                key = f"{rule.name}__action__{action.field}"
                compiled = self._compiled_rules.get(key)
                if compiled:
                    try:
                        value = eval(compiled, {"__builtins__": {}}, {"record": record})
                        results[action.field] = value
                    except Exception as e:
                        logger.error(
                            f"Error computing '{action.field}' in rule '{rule.name}': {e}"
                        )
                        results[action.field] = None
        
        return results

    def _evaluate_decision_table(self, dt: DecisionTable, record: dict) -> dict:
        """
        Evaluate a decision table against a record.
        
        Scans rows top-to-bottom and returns the first matching row's outputs.
        This matches Ab Initio BRE's decision table "first match" behavior.
        """
        for row in dt.rows:
            if self._matches_decision_row(row, record):
                return dict(row.outputs)
        
        return {}

    def _matches_decision_row(self, row: DecisionTableRow, record: dict) -> bool:
        """Check if a record matches a decision table row's input conditions."""
        for col_name, condition in row.inputs.items():
            record_value = record.get(col_name)
            if record_value is None:
                return False
            
            # Range condition
            if 'min' in condition and 'max' in condition:
                try:
                    val = float(record_value)
                    if not (condition['min'] <= val < condition['max']):
                        return False
                except (ValueError, TypeError):
                    return False
            
            # Exact match condition
            elif 'value' in condition:
                if str(record_value) != str(condition['value']):
                    return False
        
        return True

    # ─────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Get engine statistics."""
        return {
            'rulesets': len(self.rulesets),
            'total_rules': sum(len(rs.rules) for rs in self.rulesets),
            'decision_tables': len(self.decision_tables),
            'lookups': len(self.lookups),
            'compiled_expressions': len(self._compiled_rules)
        }

    def validate(self) -> list[str]:
        """Validate all rules for correctness."""
        issues = []
        
        for ruleset in self.rulesets:
            for rule in ruleset.rules:
                # Check that lookup references exist
                if rule.condition_type == ConditionType.LOOKUP:
                    if rule.condition_lookup_name not in self.lookups:
                        issues.append(
                            f"Rule '{rule.name}' references missing lookup "
                            f"'{rule.condition_lookup_name}'"
                        )
                
                # Check that expressions compile
                if rule.condition_type == ConditionType.EXPRESSION:
                    if rule.name not in self._compiled_rules:
                        issues.append(
                            f"Rule '{rule.name}' has an expression that failed to compile"
                        )
        
        return issues
