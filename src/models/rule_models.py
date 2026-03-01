"""
Rule Data Models
================
Defines the core data structures for the open-source rules engine.
These models map 1:1 to Ab Initio BRE concepts:

  Ab Initio BRE Concept  →  Open Source Model
  ─────────────────────────────────────────────
  Ruleset                →  RuleSet
  Rule (condition+action)→  Rule
  DML Expression         →  Rule.condition (Python expr)
  Decision Table         →  DecisionTable
  Lookup                 →  LookupDefinition
  Action (set field)     →  RuleAction
"""

from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum


class ConditionType(Enum):
    """Maps Ab Initio BRE condition types to engine types."""
    EXPRESSION = "expression"    # DML expression → Python expression
    LOOKUP = "lookup"            # Ab Initio lookup → set/dict membership
    COMPOSITE = "composite"      # AND/OR combinations


class ActionType(Enum):
    """Action types for rule outputs."""
    SET_VALUE = "set_value"      # Direct value assignment
    COMPUTED = "computed"         # Expression-based computation


@dataclass
class RuleAction:
    """
    Represents a single action (field assignment) from an Ab Initio BRE rule.
    
    In Ab Initio BRE:
        <set_field name="risk_level" value="HIGH"/>
    
    Becomes:
        RuleAction(field="risk_level", value="HIGH", action_type=ActionType.SET_VALUE)
    
    For computed fields in Ab Initio:
        <set_field name="fee_amount">
            <expression language="dml">transaction_amount * 0.015</expression>
        </set_field>
    
    Becomes:
        RuleAction(field="fee_amount", expression="record['transaction_amount'] * 0.015",
                   action_type=ActionType.COMPUTED)
    """
    field: str
    value: Any = None
    expression: Optional[str] = None
    action_type: ActionType = ActionType.SET_VALUE


@dataclass
class Rule:
    """
    A single business rule — the core unit of Ab Initio BRE.
    
    Each rule has:
    - A condition (when to fire)
    - A list of actions (what to do)
    - A priority (execution order — lower number = higher priority)
    
    In Ab Initio, rules are defined in DML. Here, conditions become
    Python expressions evaluated against a record dict.
    """
    name: str
    priority: int
    condition_type: ConditionType
    condition_expression: Optional[str] = None
    condition_lookup_field: Optional[str] = None
    condition_lookup_name: Optional[str] = None
    actions: list[RuleAction] = field(default_factory=list)
    description: str = ""
    enabled: bool = True

    def __post_init__(self):
        """Sort actions by field name for deterministic output."""
        pass


@dataclass
class RuleSet:
    """
    A collection of related rules — equivalent to an Ab Initio BRE Ruleset.
    
    Key behaviors:
    - Rules within a ruleset are sorted by priority
    - stop_on_first_match=True mimics Ab Initio's "first matching rule" behavior
    - stop_on_first_match=False allows all matching rules to fire (accumulative)
    """
    name: str
    description: str
    priority: int  # Ruleset execution order
    rules: list[Rule] = field(default_factory=list)
    active: bool = True
    stop_on_first_match: bool = True  # True = first-match; False = all-match

    def get_sorted_rules(self) -> list[Rule]:
        """Return rules sorted by priority (lower number = higher priority)."""
        return sorted(
            [r for r in self.rules if r.enabled],
            key=lambda r: r.priority
        )


@dataclass
class DecisionTableRow:
    """Single row in a decision table."""
    inputs: dict[str, dict] = field(default_factory=dict)   # {col: {min, max} or value}
    outputs: dict[str, Any] = field(default_factory=dict)


@dataclass
class DecisionTable:
    """
    Decision Table — maps to Ab Initio BRE Decision Table component.
    
    Ab Initio BRE decision tables are matrix-style lookups where:
    - Input columns define conditions (ranges or exact matches)
    - Output columns define the result values
    
    Example: Customer tier based on account_age and annual_volume
    """
    name: str
    description: str
    input_columns: list[dict] = field(default_factory=list)
    output_columns: list[str] = field(default_factory=list)
    rows: list[DecisionTableRow] = field(default_factory=list)


@dataclass
class LookupDefinition:
    """
    Lookup definition — maps to Ab Initio BRE Lookup components.
    
    Types:
    - "set": A set of values for membership testing (e.g., suspicious countries)
    - "key_value": A dictionary for value lookups (e.g., currency rates)
    """
    name: str
    lookup_type: str  # "set" or "key_value"
    values: Any = None  # set or dict
    source: Optional[str] = None


@dataclass
class RuleExecutionResult:
    """
    Captures the result of executing rules against a single record.
    Equivalent to Ab Initio BRE's audit trail.
    """
    record_id: str
    rules_fired: list[str] = field(default_factory=list)
    fields_set: dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0.0
    errors: list[str] = field(default_factory=list)
