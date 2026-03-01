"""
Tests for Open-Source Rules Engine
===================================
Validates that the engine produces the same outputs as Ab Initio BRE
for equivalent input data — this is the core regression test suite.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.rules_engine.engine import RulesEngine


@pytest.fixture
def engine():
    """Create a loaded rules engine."""
    yaml_path = os.path.join(
        os.path.dirname(__file__), '..', 'config', 'converted_rules.yaml'
    )
    eng = RulesEngine()
    eng.load_rules(yaml_path)
    return eng


class TestRiskClassification:
    """Test Transaction Risk Classification ruleset — must match Ab Initio BRE output."""

    def test_high_value_gets_high_risk(self, engine):
        """$15K domestic → HIGH risk (Ab Initio rule: high_value_transaction)"""
        record = {
            'transaction_amount': 15000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'PREMIUM',
            'account_age_years': 5,
            'annual_volume': 200000,
        }
        result = engine.evaluate(record)
        assert result['risk_level'] == 'HIGH'
        assert result['risk_score'] == 85
        assert result['review_required'] == True

    def test_suspicious_country_gets_critical(self, engine):
        """Sanctioned country → CRITICAL (Ab Initio rule: suspicious_country_override)"""
        record = {
            'transaction_amount': 500.00,
            'country_code': 'IR',  # Iran — in suspicious_countries lookup
            'is_international': 'Y',
            'customer_segment': 'STANDARD',
            'account_age_years': 1,
            'annual_volume': 10000,
        }
        result = engine.evaluate(record)
        assert result['risk_level'] == 'CRITICAL'
        assert result['risk_score'] == 99
        assert result['alert_compliance'] == True

    def test_low_value_gets_low_risk(self, engine):
        """$950 domestic → LOW risk (Ab Initio rule: low_value_transaction)"""
        record = {
            'transaction_amount': 950.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'NEW',
            'account_age_years': 0,
            'annual_volume': 5000,
        }
        result = engine.evaluate(record)
        assert result['risk_level'] == 'LOW'
        assert result['risk_score'] == 10
        assert result['review_required'] == False

    def test_international_medium_gets_high(self, engine):
        """$7.5K international → HIGH (Ab Initio rule: international_medium_value)"""
        record = {
            'transaction_amount': 7500.00,
            'country_code': 'DE',
            'is_international': 'Y',
            'customer_segment': 'STANDARD',
            'account_age_years': 2,
            'annual_volume': 50000,
        }
        result = engine.evaluate(record)
        assert result['risk_level'] == 'HIGH'
        assert result['risk_score'] == 75

    def test_domestic_medium_gets_medium(self, engine):
        """$7.5K domestic → MEDIUM (Ab Initio rule: medium_value_domestic)"""
        record = {
            'transaction_amount': 7500.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'STANDARD',
            'account_age_years': 2,
            'annual_volume': 50000,
        }
        result = engine.evaluate(record)
        assert result['risk_level'] == 'MEDIUM'
        assert result['risk_score'] == 50


class TestFeeCalculation:
    """Test Fee Calculation ruleset — must match Ab Initio BRE fee outputs."""

    def test_premium_customer_fee(self, engine):
        """PREMIUM segment → 0.5% fee (Ab Initio rule: premium_customer_fee)"""
        record = {
            'transaction_amount': 10000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'PREMIUM',
            'account_age_years': 5,
            'annual_volume': 200000,
        }
        result = engine.evaluate(record)
        assert result['fee_rate'] == 0.005
        assert result['fee_amount'] == 50.0  # 10000 * 0.005
        assert result['fee_type'] == 'DISCOUNTED'

    def test_standard_customer_fee(self, engine):
        """STANDARD segment → 1.5% fee (Ab Initio rule: standard_customer_fee)"""
        record = {
            'transaction_amount': 10000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'STANDARD',
            'account_age_years': 3,
            'annual_volume': 80000,
        }
        result = engine.evaluate(record)
        assert result['fee_rate'] == 0.015
        assert result['fee_amount'] == 150.0

    def test_new_customer_fee(self, engine):
        """NEW segment → 2.0% fee (Ab Initio rule: new_customer_fee)"""
        record = {
            'transaction_amount': 5000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'NEW',
            'account_age_years': 0,
            'annual_volume': 5000,
        }
        result = engine.evaluate(record)
        assert result['fee_rate'] == 0.020
        assert result['fee_amount'] == 100.0

    def test_international_surcharge_applied(self, engine):
        """International txn → 1% surcharge added (Ab Initio rule: international_surcharge)"""
        record = {
            'transaction_amount': 10000.00,
            'country_code': 'DE',
            'is_international': 'Y',
            'customer_segment': 'STANDARD',
            'account_age_years': 3,
            'annual_volume': 80000,
        }
        result = engine.evaluate(record)
        assert result['intl_surcharge'] == 100.0
        assert result['intl_surcharge_applied'] == True

    def test_domestic_no_surcharge(self, engine):
        """Domestic txn → no international surcharge"""
        record = {
            'transaction_amount': 10000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'STANDARD',
            'account_age_years': 3,
            'annual_volume': 80000,
        }
        result = engine.evaluate(record)
        assert 'intl_surcharge' not in result or result.get('intl_surcharge') is None


class TestDecisionTable:
    """Test Customer Tier Decision Table — must match Ab Initio BRE lookup."""

    def test_new_low_volume_gets_bronze(self, engine):
        record = {
            'transaction_amount': 1000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'NEW',
            'account_age_years': 0.5,
            'annual_volume': 20000,
        }
        result = engine.evaluate(record)
        assert result.get('loyalty_tier') == 'BRONZE'
        assert result.get('discount_pct') == 0

    def test_mature_high_volume_gets_platinum(self, engine):
        record = {
            'transaction_amount': 50000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'PREMIUM',
            'account_age_years': 8,
            'annual_volume': 500000,
        }
        result = engine.evaluate(record)
        assert result.get('loyalty_tier') == 'PLATINUM'
        assert result.get('discount_pct') == 20
        assert result.get('priority_processing') == True

    def test_mid_age_medium_volume_gets_silver(self, engine):
        record = {
            'transaction_amount': 5000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'STANDARD',
            'account_age_years': 2,
            'annual_volume': 50000,
        }
        result = engine.evaluate(record)
        assert result.get('loyalty_tier') == 'SILVER'
        assert result.get('discount_pct') == 5


class TestAuditTrail:
    """Test rule execution audit trail — equivalent to Ab Initio BRE logging."""

    def test_rules_fired_populated(self, engine):
        record = {
            'transaction_amount': 15000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'PREMIUM',
            'account_age_years': 5,
            'annual_volume': 200000,
        }
        result = engine.evaluate(record)
        assert '__rules_fired' in result
        assert len(result['__rules_fired']) > 0

    def test_execution_time_tracked(self, engine):
        record = {
            'transaction_amount': 1000.00,
            'country_code': 'US',
            'is_international': 'N',
            'customer_segment': 'STANDARD',
            'account_age_years': 1,
            'annual_volume': 10000,
        }
        result = engine.evaluate(record)
        assert '__execution_time_ms' in result
        assert result['__execution_time_ms'] >= 0


class TestEngineValidation:
    """Test engine validation and error handling."""

    def test_validate_no_issues(self, engine):
        issues = engine.validate()
        assert len(issues) == 0, f"Unexpected issues: {issues}"

    def test_stats(self, engine):
        stats = engine.get_stats()
        assert stats['rulesets'] > 0
        assert stats['total_rules'] > 0
        assert stats['compiled_expressions'] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
