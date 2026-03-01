"""
Ab Initio BRE XML to Open-Source YAML Converter
=================================================
This module parses Ab Initio BRE XML rule exports and converts them
into the open-source YAML rule format consumed by the rules engine.

Ab Initio BRE exports rules in an XML/RDF format containing:
  - Rulesets with conditions expressed in DML (Data Manipulation Language)
  - Decision tables with range-based lookups
  - Lookup definitions (sets and key-value maps)
  - Rule priorities and chaining logic

This converter handles:
  1. Parsing the XML structure
  2. Converting DML expressions to Python expressions
  3. Mapping decision tables to YAML matrix format
  4. Extracting lookup definitions
  5. Preserving rule priorities and execution semantics

Usage:
    converter = BREConverter("config/abinitio_rules_sample.xml")
    converter.convert("config/converted_rules.yaml")
"""

import re
import yaml
from lxml import etree
from typing import Any


class DMLExpressionConverter:
    """
    Converts Ab Initio DML expressions to Python expressions.
    
    Ab Initio DML is a proprietary expression language. Common patterns:
    
    DML                              →  Python
    ─────────────────────────────────────────────────
    field_name > 100                 →  record['field_name'] > 100
    field1 == "VALUE"                →  record['field1'] == 'VALUE'
    field in_lookup "lookup_name"    →  field IN lookup (handled separately)
    is_null(field)                   →  record['field'] is None
    string_length(field) > 5        →  len(str(record['field'])) > 5
    decimal_strip(field)            →  float(record['field'])
    """
    
    # Ab Initio DML field names that should become record['field_name']
    FIELD_PATTERN = re.compile(r'\b([a-z_][a-z0-9_]*)\b')
    
    # Python reserved words and built-in names to NOT convert
    RESERVED = {
        'and', 'or', 'not', 'in', 'is', 'true', 'false', 'none',
        'True', 'False', 'None', 'record', 'len', 'str', 'float',
        'int', 'abs', 'min', 'max', 'round'
    }
    
    # Common DML function mappings
    DML_FUNCTIONS = {
        'is_null': lambda f: f"record['{f}'] is None",
        'is_not_null': lambda f: f"record['{f}'] is not None",
        'string_length': lambda f: f"len(str(record['{f}']))",
        'decimal_strip': lambda f: f"float(record['{f}'])",
        'string_downcase': lambda f: f"str(record['{f}']).lower()",
        'string_upcase': lambda f: f"str(record['{f}']).upper()",
        'string_trim': lambda f: f"str(record['{f}']).strip()",
    }

    @classmethod
    def convert(cls, dml_expression: str, known_fields: set[str] = None) -> str:
        """
        Convert a DML expression to a Python expression.
        
        Args:
            dml_expression: The Ab Initio DML expression string
            known_fields: Optional set of known field names for accurate conversion
            
        Returns:
            Python expression string that operates on a 'record' dict
        """
        expr = dml_expression.strip()
        
        # Handle in_lookup separately (Ab Initio lookup membership test)
        lookup_match = re.match(r'(\w+)\s+in_lookup\s+"(\w+)"', expr)
        if lookup_match:
            return None  # Handled as lookup condition type
        
        # Handle DML functions
        for func_name, converter in cls.DML_FUNCTIONS.items():
            func_match = re.search(rf'{func_name}\((\w+)\)', expr)
            if func_match:
                field = func_match.group(1)
                expr = expr.replace(func_match.group(0), converter(field))
        
        # Convert field references to record['field'] notation
        # We need to identify which tokens are field names vs operators/values
        tokens = cls._tokenize(expr)
        converted_tokens = []
        
        for token in tokens:
            if cls._is_field_reference(token, known_fields):
                converted_tokens.append(f"record['{token}']")
            elif token == '==' and converted_tokens and "record['" in converted_tokens[-1]:
                converted_tokens.append('==')
            else:
                converted_tokens.append(token)
        
        return ' '.join(converted_tokens)

    @classmethod
    def _tokenize(cls, expr: str) -> list[str]:
        """Split expression into tokens preserving strings and operators."""
        # Simple tokenizer for DML expressions
        tokens = []
        current = ''
        in_string = False
        string_char = None
        
        i = 0
        while i < len(expr):
            c = expr[i]
            
            if in_string:
                current += c
                if c == string_char:
                    tokens.append(current)
                    current = ''
                    in_string = False
            elif c in ('"', "'"):
                if current.strip():
                    tokens.append(current.strip())
                current = c
                in_string = True
                string_char = c
            elif c in (' ', '\t', '\n'):
                if current.strip():
                    tokens.append(current.strip())
                current = ''
            elif c in ('>', '<', '=', '!'):
                if current.strip():
                    tokens.append(current.strip())
                current = c
                # Check for two-char operators
                if i + 1 < len(expr) and expr[i + 1] == '=':
                    current += '='
                    i += 1
                tokens.append(current)
                current = ''
            else:
                current += c
            i += 1
        
        if current.strip():
            tokens.append(current.strip())
        
        return tokens

    @classmethod
    def _is_field_reference(cls, token: str, known_fields: set[str] = None) -> bool:
        """Determine if a token is a field reference vs keyword/literal."""
        if not re.match(r'^[a-z_][a-z0-9_]*$', token):
            return False
        if token in cls.RESERVED:
            return False
        if known_fields and token not in known_fields:
            return False
        # If no known_fields provided, assume identifiers are fields
        if known_fields is None and re.match(r'^[a-z_][a-z0-9_]*$', token):
            return True
        return token in (known_fields or set())


class BREConverter:
    """
    Main converter class: Parses Ab Initio BRE XML → YAML rules.
    
    Usage:
        converter = BREConverter("config/abinitio_rules_sample.xml")
        yaml_data = converter.parse()
        converter.write_yaml("config/converted_rules.yaml")
    """
    
    def __init__(self, xml_path: str):
        self.xml_path = xml_path
        self.tree = None
        self.root = None
        self.rulesets = []
        self.decision_tables = []
        self.lookups = {}
        self.known_fields = set()
    
    def parse(self) -> dict:
        """Parse the Ab Initio BRE XML file and return structured data."""
        self.tree = etree.parse(self.xml_path)
        self.root = self.tree.getroot()
        
        # First pass: extract lookups (needed for rule conversion)
        self._parse_lookups()
        
        # Second pass: extract rulesets and rules
        self._parse_rulesets()
        
        return self._build_yaml_structure()

    def _parse_lookups(self):
        """Extract lookup definitions from BRE XML."""
        lookups_elem = self.root.find('.//lookups')
        if lookups_elem is None:
            return
            
        for lookup in lookups_elem.findall('lookup'):
            name = lookup.get('name')
            lookup_type = lookup.get('type')
            
            if lookup_type == 'set':
                values = [v.text for v in lookup.findall('.//value')]
                self.lookups[name] = {
                    'type': 'set',
                    'values': values
                }
            elif lookup_type == 'key_value':
                source = lookup.get('source', 'inline')
                path_elem = lookup.find('path')
                self.lookups[name] = {
                    'type': 'key_value',
                    'source': source,
                    'path': path_elem.text if path_elem is not None else None
                }

    def _parse_rulesets(self):
        """Extract rulesets and their rules from BRE XML."""
        for ruleset_elem in self.root.findall('.//ruleset'):
            ruleset_type = ruleset_elem.get('type', 'standard')
            
            if ruleset_type == 'decision_table':
                self._parse_decision_table(ruleset_elem)
            else:
                self._parse_standard_ruleset(ruleset_elem)

    def _parse_standard_ruleset(self, ruleset_elem):
        """Parse a standard ruleset with condition-based rules."""
        ruleset = {
            'name': ruleset_elem.get('name'),
            'description': ruleset_elem.get('description', ''),
            'priority': int(ruleset_elem.get('priority', 100)),
            'active': ruleset_elem.get('active', 'true').lower() == 'true',
            'stop_on_first_match': True,
            'rules': []
        }
        
        for rule_elem in ruleset_elem.findall('rule'):
            rule = self._parse_rule(rule_elem)
            if rule:
                ruleset['rules'].append(rule)
        
        # Sort rules by priority
        ruleset['rules'].sort(key=lambda r: r['priority'])
        self.rulesets.append(ruleset)

    def _parse_rule(self, rule_elem) -> dict:
        """Parse a single rule element from the BRE XML."""
        rule = {
            'name': rule_elem.get('name'),
            'priority': int(rule_elem.get('priority', 50)),
            'description': '',
            'actions': []
        }
        
        # Parse condition
        condition = rule_elem.find('.//condition')
        if condition is not None:
            expr_elem = condition.find('expression')
            if expr_elem is not None and expr_elem.text:
                dml_expr = expr_elem.text.strip()
                
                # Check if it's a lookup-based condition
                lookup_match = re.match(r'(\w+)\s+in_lookup\s+"(\w+)"', dml_expr)
                if lookup_match:
                    rule['condition'] = {
                        'type': 'lookup',
                        'field': lookup_match.group(1),
                        'lookup_name': lookup_match.group(2)
                    }
                else:
                    # Convert DML to Python expression
                    python_expr = DMLExpressionConverter.convert(dml_expr)
                    rule['condition'] = {
                        'type': 'expression',
                        'expression': python_expr
                    }
        
        # Parse actions
        action_elem = rule_elem.find('.//action')
        if action_elem is not None:
            for set_field in action_elem.findall('set_field'):
                action = self._parse_action(set_field)
                if action:
                    rule['actions'].append(action)
        
        return rule

    def _parse_action(self, set_field_elem) -> dict:
        """Parse a set_field action element."""
        field_name = set_field_elem.get('name')
        value = set_field_elem.get('value')
        
        # Check for computed expression inside the element
        expr_elem = set_field_elem.find('expression')
        if expr_elem is not None and expr_elem.text:
            dml_expr = expr_elem.text.strip()
            python_expr = DMLExpressionConverter.convert(dml_expr)
            return {
                'field': field_name,
                'type': 'computed',
                'expression': python_expr
            }
        
        # Direct value assignment — try to parse as number/bool
        if value is not None:
            parsed_value = self._parse_value(value)
            return {
                'field': field_name,
                'value': parsed_value
            }
        
        return None

    def _parse_decision_table(self, ruleset_elem):
        """Parse a decision table ruleset."""
        dt_elem = ruleset_elem.find('decision_table')
        if dt_elem is None:
            return
        
        dt = {
            'name': dt_elem.get('name', ruleset_elem.get('name')),
            'description': ruleset_elem.get('description', ''),
            'input_columns': [],
            'output_columns': [],
            'rows': []
        }
        
        # Parse input columns
        for col in dt_elem.findall('.//input_columns/column'):
            dt['input_columns'].append({
                'name': col.get('name'),
                'type': col.get('type', 'exact')
            })
        
        # Parse output columns
        for col in dt_elem.findall('.//output_columns/column'):
            dt['output_columns'].append(col.get('name'))
        
        # Parse rows
        for row_elem in dt_elem.findall('.//rows/row'):
            row = {'inputs': {}, 'outputs': {}}
            
            # Parse range inputs
            for range_elem in row_elem.findall('.//input/range'):
                field = range_elem.get('field')
                row['inputs'][field] = {
                    'min': self._parse_value(range_elem.get('min')),
                    'max': self._parse_value(range_elem.get('max'))
                }
            
            # Parse outputs
            output_elem = row_elem.find('output')
            if output_elem is not None:
                for out_col in dt['output_columns']:
                    val = output_elem.get(out_col)
                    if val is not None:
                        row['outputs'][out_col] = self._parse_value(val)
            
            dt['rows'].append(row)
        
        self.decision_tables.append(dt)

    def _parse_value(self, value_str: str) -> Any:
        """Parse a string value into the appropriate Python type."""
        if value_str is None:
            return None
        
        # Boolean
        if value_str.lower() == 'true':
            return True
        if value_str.lower() == 'false':
            return False
        
        # Integer
        try:
            return int(value_str)
        except ValueError:
            pass
        
        # Float
        try:
            return float(value_str)
        except ValueError:
            pass
        
        # String
        return value_str

    def _build_yaml_structure(self) -> dict:
        """Build the final YAML-compatible dict structure."""
        return {
            'rulesets': self.rulesets,
            'decision_tables': self.decision_tables,
            'lookups': self.lookups
        }

    def write_yaml(self, output_path: str):
        """Parse the XML and write the converted YAML file."""
        data = self.parse()
        
        with open(output_path, 'w') as f:
            f.write("# ============================================================\n")
            f.write("# AUTO-GENERATED from Ab Initio BRE XML\n")
            f.write(f"# Source: {self.xml_path}\n")
            f.write("# ============================================================\n\n")
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, width=120)
        
        return data

    def get_conversion_report(self) -> dict:
        """Generate a report of what was converted."""
        data = self.parse() if not self.rulesets else self._build_yaml_structure()
        
        total_rules = sum(len(rs['rules']) for rs in data['rulesets'])
        total_dt_rows = sum(len(dt['rows']) for dt in data['decision_tables'])
        
        return {
            'source_file': self.xml_path,
            'rulesets_converted': len(data['rulesets']),
            'total_rules': total_rules,
            'decision_tables': len(data['decision_tables']),
            'decision_table_rows': total_dt_rows,
            'lookups': len(data['lookups']),
            'rulesets': [
                {
                    'name': rs['name'],
                    'rule_count': len(rs['rules']),
                    'rules': [r['name'] for r in rs['rules']]
                }
                for rs in data['rulesets']
            ]
        }
