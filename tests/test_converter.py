"""
Tests for Ab Initio BRE → YAML Converter
=========================================
Validates that the converter correctly parses Ab Initio BRE XML
and produces equivalent YAML rule definitions.
"""

import os
import sys
import pytest
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.converters.bre_converter import BREConverter, DMLExpressionConverter


class TestDMLExpressionConverter:
    """Test DML expression conversion to Python."""

    def test_simple_comparison(self):
        result = DMLExpressionConverter.convert(
            "transaction_amount > 10000.00",
            known_fields={'transaction_amount'}
        )
        assert "record['transaction_amount']" in result
        assert "> 10000.00" in result

    def test_string_equality(self):
        result = DMLExpressionConverter.convert(
            'customer_segment == "PREMIUM"',
            known_fields={'customer_segment'}
        )
        assert "record['customer_segment']" in result
        assert "==" in result

    def test_compound_condition(self):
        result = DMLExpressionConverter.convert(
            'transaction_amount > 5000.00 and is_international == "Y"',
            known_fields={'transaction_amount', 'is_international'}
        )
        assert "record['transaction_amount']" in result
        assert "record['is_international']" in result
        assert "and" in result

    def test_lookup_returns_none(self):
        """Lookup expressions are handled as a separate condition type."""
        result = DMLExpressionConverter.convert(
            'country_code in_lookup "suspicious_countries"'
        )
        assert result is None


class TestBREConverter:
    """Test the full BRE XML to YAML conversion."""

    @pytest.fixture
    def sample_xml_path(self):
        return os.path.join(
            os.path.dirname(__file__), '..', 'config', 'abinitio_rules_sample.xml'
        )

    def test_parse_returns_dict(self, sample_xml_path):
        converter = BREConverter(sample_xml_path)
        result = converter.parse()
        assert isinstance(result, dict)
        assert 'rulesets' in result
        assert 'decision_tables' in result
        assert 'lookups' in result

    def test_rulesets_count(self, sample_xml_path):
        converter = BREConverter(sample_xml_path)
        result = converter.parse()
        # Should have 2 standard rulesets (risk + fee)
        assert len(result['rulesets']) == 2

    def test_decision_tables_count(self, sample_xml_path):
        converter = BREConverter(sample_xml_path)
        result = converter.parse()
        assert len(result['decision_tables']) == 1

    def test_lookups_parsed(self, sample_xml_path):
        converter = BREConverter(sample_xml_path)
        result = converter.parse()
        assert 'suspicious_countries' in result['lookups']
        assert result['lookups']['suspicious_countries']['type'] == 'set'

    def test_rule_priorities_sorted(self, sample_xml_path):
        converter = BREConverter(sample_xml_path)
        result = converter.parse()
        for ruleset in result['rulesets']:
            priorities = [r['priority'] for r in ruleset['rules']]
            assert priorities == sorted(priorities), \
                f"Rules in '{ruleset['name']}' not sorted by priority"

    def test_write_yaml(self, sample_xml_path):
        converter = BREConverter(sample_xml_path)
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as f:
            output_path = f.name
        
        try:
            converter.write_yaml(output_path)
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0
        finally:
            os.unlink(output_path)

    def test_conversion_report(self, sample_xml_path):
        converter = BREConverter(sample_xml_path)
        report = converter.get_conversion_report()
        assert report['rulesets_converted'] == 2
        assert report['total_rules'] > 0
        assert report['decision_tables'] == 1
        assert report['lookups'] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
