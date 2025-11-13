#!/usr/bin/env python3
"""
Data Quality Validator for Tiki & Lazada ELT Pipeline
===================================================
Comprehensive data validation và quality checks

Features:
- Schema validation
- Data integrity checks
- Business rule validation
- Anomaly detection
- Quality scoring và reporting

Author: ELT Team
Version: 1.0.0
"""

import logging
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from decimal import Decimal
import json
import pandas as pd
import numpy as np
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ValidationRule:
    """Data validation rule definition"""
    rule_id: str
    table_name: str
    column_name: Optional[str]
    rule_type: str  # not_null, unique, range, format, business
    rule_definition: Dict[str, Any]
    severity: str = 'ERROR'  # ERROR, WARNING, INFO
    description: str = ''
    is_active: bool = True

@dataclass
class ValidationResult:
    """Result của một validation check"""
    rule_id: str
    table_name: str
    column_name: Optional[str]
    rule_type: str
    status: str  # PASSED, FAILED, ERROR
    records_checked: int = 0
    records_failed: int = 0
    pass_rate: float = 0.0
    error_message: Optional[str] = None
    sample_failures: List[Dict[str, Any]] = None
    execution_time: float = 0.0

class TikiLazadaDataValidator:
    """
    Data quality validator cho Tiki & Lazada pipeline
    """

    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.validation_rules = []
        self.results = []

        # Load default validation rules
        self._load_default_rules()

    def connect_db(self):
        """Database connection"""
        return psycopg2.connect(**self.db_config)

    def _load_default_rules(self):
        """Load default validation rules"""

        # Staging table rules
        staging_rules = [
            ValidationRule(
                rule_id='STG_001',
                table_name='staging.raw_products',
                column_name='source_platform',
                rule_type='not_null',
                rule_definition={},
                description='Source platform must not be null'
            ),
            ValidationRule(
                rule_id='STG_002',
                table_name='staging.raw_products',
                column_name='source_platform',
                rule_type='format',
                rule_definition={'allowed_values': ['tiki', 'lazada']},
                description='Source platform must be tiki or lazada'
            ),
            ValidationRule(
                rule_id='STG_003',
                table_name='staging.raw_products',
                column_name='raw_data',
                rule_type='not_null',
                rule_definition={},
                description='Raw data must not be null'
            ),
            ValidationRule(
                rule_id='STG_004',
                table_name='staging.raw_products',
                column_name='data_hash',
                rule_type='unique',
                rule_definition={'within_days': 1},
                description='Data hash should be unique within a day (dedup check)'
            )
        ]

        # ODS table rules
        ods_rules = [
            ValidationRule(
                rule_id='ODS_001',
                table_name='ods.products',
                column_name='title',
                rule_type='not_null',
                rule_definition={},
                description='Product title must not be null'
            ),
            ValidationRule(
                rule_id='ODS_002',
                table_name='ods.products',
                column_name='title',
                rule_type='format',
                rule_definition={'min_length': 5, 'max_length': 1000},
                description='Product title length check'
            ),
            ValidationRule(
                rule_id='ODS_003',
                table_name='ods.products',
                column_name='current_price',
                rule_type='range',
                rule_definition={'min_value': 1000, 'max_value': 1000000000},
                description='Current price should be reasonable (1K - 1B VND)'
            ),
            ValidationRule(
                rule_id='ODS_004',
                table_name='ods.products',
                column_name='rating',
                rule_type='range',
                rule_definition={'min_value': 0, 'max_value': 5},
                description='Rating should be between 0-5'
            ),
            ValidationRule(
                rule_id='ODS_005',
                table_name='ods.products',
                column_name='review_count',
                rule_type='range',
                rule_definition={'min_value': 0, 'max_value': 1000000},
                description='Review count should be non-negative and reasonable'
            ),
            ValidationRule(
                rule_id='ODS_006',
                table_name='ods.products',
                column_name='product_url',
                rule_type='format',
                rule_definition={'pattern': r'^https?://'},
                description='Product URL should be valid HTTP(S) URL'
            ),
            ValidationRule(
                rule_id='ODS_007',
                table_name='ods.products',
                column_name=None,
                rule_type='business',
                rule_definition={
                    'rule': 'price_consistency',
                    'description': 'Original price should be >= current price'
                },
                description='Price consistency check'
            ),
            ValidationRule(
                rule_id='ODS_008',
                table_name='ods.products',
                column_name=None,
                rule_type='business',
                rule_definition={
                    'rule': 'platform_url_match',
                    'description': 'URL should match source platform domain'
                },
                description='Platform URL consistency'
            )
        ]

        # Data freshness rules
        freshness_rules = [
            ValidationRule(
                rule_id='FRESH_001',
                table_name='staging.raw_products',
                column_name='created_at',
                rule_type='business',
                rule_definition={
                    'rule': 'data_freshness',
                    'max_age_hours': 25,
                    'min_records_per_day': 10
                },
                description='Staging data should be fresh (within 25 hours)'
            ),
            ValidationRule(
                rule_id='FRESH_002',
                table_name='ods.products',
                column_name='last_updated',
                rule_type='business',
                rule_definition={
                    'rule': 'daily_update_check',
                    'min_updated_records': 5
                },
                description='Minimum records should be updated daily'
            )
        ]

        # Combine all rules
        self.validation_rules = staging_rules + ods_rules + freshness_rules

    def add_custom_rule(self, rule: ValidationRule):
        """Add custom validation rule"""
        self.validation_rules.append(rule)
        logger.info(f"Added custom rule: {rule.rule_id}")

    def run_validation(self, rule_filter: Optional[str] = None) -> List[ValidationResult]:
        """Run all validation rules"""
        logger.info("🔍 Starting data validation...")

        self.results = []
        rules_to_run = [r for r in self.validation_rules if r.is_active]

        if rule_filter:
            rules_to_run = [r for r in rules_to_run if rule_filter in r.rule_id]

        logger.info(f"Running {len(rules_to_run)} validation rules...")

        with self.connect_db() as conn:
            for rule in rules_to_run:
                try:
                    start_time = datetime.now()
                    result = self._execute_rule(conn, rule)
                    result.execution_time = (datetime.now() - start_time).total_seconds()

                    self.results.append(result)

                    status_emoji = "✅" if result.status == "PASSED" else "❌"
                    logger.info(f"{status_emoji} {rule.rule_id}: {result.status} ({result.pass_rate:.1f}%)")

                except Exception as e:
                    error_result = ValidationResult(
                        rule_id=rule.rule_id,
                        table_name=rule.table_name,
                        column_name=rule.column_name,
                        rule_type=rule.rule_type,
                        status='ERROR',
                        error_message=str(e)
                    )
                    self.results.append(error_result)
                    logger.error(f"❌ {rule.rule_id}: ERROR - {str(e)}")

        self._save_results()
        return self.results

    def _execute_rule(self, conn, rule: ValidationRule) -> ValidationResult:
        """Execute a single validation rule"""

        if rule.rule_type == 'not_null':
            return self._check_not_null(conn, rule)
        elif rule.rule_type == 'unique':
            return self._check_unique(conn, rule)
        elif rule.rule_type == 'range':
            return self._check_range(conn, rule)
        elif rule.rule_type == 'format':
            return self._check_format(conn, rule)
        elif rule.rule_type == 'business':
            return self._check_business_rule(conn, rule)
        else:
            raise ValueError(f"Unknown rule type: {rule.rule_type}")

    def _check_not_null(self, conn, rule: ValidationRule) -> ValidationResult:
        """Check not null constraint"""
        cursor = conn.cursor(cursor_factory=DictCursor)

        # Count total records
        cursor.execute(f"SELECT COUNT(*) FROM {rule.table_name}")
        total_records = cursor.fetchone()[0]

        # Count null records
        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE {rule.column_name} IS NULL
        """)
        null_records = cursor.fetchone()[0]

        passed_records = total_records - null_records
        pass_rate = (passed_records / total_records * 100) if total_records > 0 else 100

        return ValidationResult(
            rule_id=rule.rule_id,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_type=rule.rule_type,
            status='PASSED' if null_records == 0 else 'FAILED',
            records_checked=total_records,
            records_failed=null_records,
            pass_rate=pass_rate
        )

    def _check_unique(self, conn, rule: ValidationRule) -> ValidationResult:
        """Check uniqueness constraint"""
        cursor = conn.cursor(cursor_factory=DictCursor)

        where_clause = ""
        if 'within_days' in rule.rule_definition:
            days = rule.rule_definition['within_days']
            where_clause = f"WHERE created_at >= CURRENT_DATE - INTERVAL '{days} days'"

        # Count total records
        cursor.execute(f"SELECT COUNT(*) FROM {rule.table_name} {where_clause}")
        total_records = cursor.fetchone()[0]

        # Count unique records
        cursor.execute(f"""
            SELECT COUNT(DISTINCT {rule.column_name}) FROM {rule.table_name} {where_clause}
        """)
        unique_records = cursor.fetchone()[0]

        duplicate_records = total_records - unique_records
        pass_rate = (unique_records / total_records * 100) if total_records > 0 else 100

        return ValidationResult(
            rule_id=rule.rule_id,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_type=rule.rule_type,
            status='PASSED' if duplicate_records == 0 else 'FAILED',
            records_checked=total_records,
            records_failed=duplicate_records,
            pass_rate=pass_rate
        )

    def _check_range(self, conn, rule: ValidationRule) -> ValidationResult:
        """Check value range constraint"""
        cursor = conn.cursor(cursor_factory=DictCursor)

        min_val = rule.rule_definition.get('min_value')
        max_val = rule.rule_definition.get('max_value')

        # Count total non-null records
        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE {rule.column_name} IS NOT NULL
        """)
        total_records = cursor.fetchone()[0]

        # Build range condition
        conditions = []
        if min_val is not None:
            conditions.append(f"{rule.column_name} < {min_val}")
        if max_val is not None:
            conditions.append(f"{rule.column_name} > {max_val}")

        if conditions:
            range_condition = " OR ".join(conditions)
            cursor.execute(f"""
                SELECT COUNT(*) FROM {rule.table_name}
                WHERE {rule.column_name} IS NOT NULL AND ({range_condition})
            """)
            failed_records = cursor.fetchone()[0]
        else:
            failed_records = 0

        passed_records = total_records - failed_records
        pass_rate = (passed_records / total_records * 100) if total_records > 0 else 100

        return ValidationResult(
            rule_id=rule.rule_id,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_type=rule.rule_type,
            status='PASSED' if failed_records == 0 else 'FAILED',
            records_checked=total_records,
            records_failed=failed_records,
            pass_rate=pass_rate
        )

    def _check_format(self, conn, rule: ValidationRule) -> ValidationResult:
        """Check format/pattern constraints"""
        cursor = conn.cursor(cursor_factory=DictCursor)

        # Count total non-null records
        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE {rule.column_name} IS NOT NULL
        """)
        total_records = cursor.fetchone()[0]

        failed_records = 0

        if 'allowed_values' in rule.rule_definition:
            # Check allowed values
            allowed_values = rule.rule_definition['allowed_values']
            values_str = "', '".join(allowed_values)
            cursor.execute(f"""
                SELECT COUNT(*) FROM {rule.table_name}
                WHERE {rule.column_name} IS NOT NULL
                AND {rule.column_name} NOT IN ('{values_str}')
            """)
            failed_records = cursor.fetchone()[0]

        elif 'pattern' in rule.rule_definition:
            # Regex pattern check
            pattern = rule.rule_definition['pattern']
            cursor.execute(f"""
                SELECT COUNT(*) FROM {rule.table_name}
                WHERE {rule.column_name} IS NOT NULL
                AND {rule.column_name} !~ %s
            """, (pattern,))
            failed_records = cursor.fetchone()[0]

        elif 'min_length' in rule.rule_definition or 'max_length' in rule.rule_definition:
            # Length check
            conditions = []
            if 'min_length' in rule.rule_definition:
                min_len = rule.rule_definition['min_length']
                conditions.append(f"LENGTH({rule.column_name}) < {min_len}")
            if 'max_length' in rule.rule_definition:
                max_len = rule.rule_definition['max_length']
                conditions.append(f"LENGTH({rule.column_name}) > {max_len}")

            if conditions:
                length_condition = " OR ".join(conditions)
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {rule.table_name}
                    WHERE {rule.column_name} IS NOT NULL AND ({length_condition})
                """)
                failed_records = cursor.fetchone()[0]

        passed_records = total_records - failed_records
        pass_rate = (passed_records / total_records * 100) if total_records > 0 else 100

        return ValidationResult(
            rule_id=rule.rule_id,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_type=rule.rule_type,
            status='PASSED' if failed_records == 0 else 'FAILED',
            records_checked=total_records,
            records_failed=failed_records,
            pass_rate=pass_rate
        )

    def _check_business_rule(self, conn, rule: ValidationRule) -> ValidationResult:
        """Check custom business rules"""
        cursor = conn.cursor(cursor_factory=DictCursor)

        business_rule = rule.rule_definition.get('rule')

        if business_rule == 'price_consistency':
            return self._check_price_consistency(cursor, rule)
        elif business_rule == 'platform_url_match':
            return self._check_platform_url_match(cursor, rule)
        elif business_rule == 'data_freshness':
            return self._check_data_freshness(cursor, rule)
        elif business_rule == 'daily_update_check':
            return self._check_daily_updates(cursor, rule)
        else:
            raise ValueError(f"Unknown business rule: {business_rule}")

    def _check_price_consistency(self, cursor, rule: ValidationRule) -> ValidationResult:
        """Check price consistency (original >= current)"""
        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE current_price IS NOT NULL AND original_price IS NOT NULL
        """)
        total_records = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE current_price IS NOT NULL
            AND original_price IS NOT NULL
            AND original_price < current_price
        """)
        failed_records = cursor.fetchone()[0]

        passed_records = total_records - failed_records
        pass_rate = (passed_records / total_records * 100) if total_records > 0 else 100

        return ValidationResult(
            rule_id=rule.rule_id,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_type=rule.rule_type,
            status='PASSED' if failed_records == 0 else 'FAILED',
            records_checked=total_records,
            records_failed=failed_records,
            pass_rate=pass_rate
        )

    def _check_platform_url_match(self, cursor, rule: ValidationRule) -> ValidationResult:
        """Check platform URL consistency"""
        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE product_url IS NOT NULL AND source_platform IS NOT NULL
        """)
        total_records = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE product_url IS NOT NULL
            AND source_platform IS NOT NULL
            AND NOT (
                (source_platform = 'tiki' AND product_url LIKE '%tiki.vn%') OR
                (source_platform = 'lazada' AND product_url LIKE '%lazada.vn%')
            )
        """)
        failed_records = cursor.fetchone()[0]

        passed_records = total_records - failed_records
        pass_rate = (passed_records / total_records * 100) if total_records > 0 else 100

        return ValidationResult(
            rule_id=rule.rule_id,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_type=rule.rule_type,
            status='PASSED' if failed_records == 0 else 'FAILED',
            records_checked=total_records,
            records_failed=failed_records,
            pass_rate=pass_rate
        )

    def _check_data_freshness(self, cursor, rule: ValidationRule) -> ValidationResult:
        """Check data freshness"""
        max_age_hours = rule.rule_definition.get('max_age_hours', 24)
        min_records = rule.rule_definition.get('min_records_per_day', 1)

        # Check records in last N hours
        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE {rule.column_name} >= NOW() - INTERVAL '{max_age_hours} hours'
        """)
        fresh_records = cursor.fetchone()[0]

        status = 'PASSED' if fresh_records >= min_records else 'FAILED'

        return ValidationResult(
            rule_id=rule.rule_id,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_type=rule.rule_type,
            status=status,
            records_checked=fresh_records,
            records_failed=0 if status == 'PASSED' else 1,
            pass_rate=100.0 if status == 'PASSED' else 0.0
        )

    def _check_daily_updates(self, cursor, rule: ValidationRule) -> ValidationResult:
        """Check daily update requirements"""
        min_updated = rule.rule_definition.get('min_updated_records', 1)

        cursor.execute(f"""
            SELECT COUNT(*) FROM {rule.table_name}
            WHERE DATE({rule.column_name}) = CURRENT_DATE
        """)
        updated_today = cursor.fetchone()[0]

        status = 'PASSED' if updated_today >= min_updated else 'FAILED'

        return ValidationResult(
            rule_id=rule.rule_id,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_type=rule.rule_type,
            status=status,
            records_checked=updated_today,
            records_failed=0 if status == 'PASSED' else 1,
            pass_rate=100.0 if status == 'PASSED' else 0.0
        )

    def _save_results(self):
        """Save validation results to database"""
        with self.connect_db() as conn:
            cursor = conn.cursor()

            # Create results table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS control.data_quality_results (
                    result_id SERIAL PRIMARY KEY,
                    rule_id VARCHAR(50) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    column_name VARCHAR(255),
                    rule_type VARCHAR(50) NOT NULL,
                    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    status VARCHAR(20) NOT NULL,
                    records_checked INTEGER DEFAULT 0,
                    records_failed INTEGER DEFAULT 0,
                    pass_rate DECIMAL(5,2) DEFAULT 0,
                    execution_time DECIMAL(8,3) DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Insert results
            for result in self.results:
                cursor.execute("""
                    INSERT INTO control.data_quality_results
                    (rule_id, table_name, column_name, rule_type, status,
                     records_checked, records_failed, pass_rate, execution_time, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    result.rule_id,
                    result.table_name,
                    result.column_name,
                    result.rule_type,
                    result.status,
                    result.records_checked,
                    result.records_failed,
                    result.pass_rate,
                    result.execution_time,
                    result.error_message
                ))

            conn.commit()
            logger.info(f"💾 Saved {len(self.results)} validation results")

    def generate_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive quality report"""
        if not self.results:
            logger.warning("No validation results available")
            return {}

        # Calculate overall statistics
        total_rules = len(self.results)
        passed_rules = len([r for r in self.results if r.status == 'PASSED'])
        failed_rules = len([r for r in self.results if r.status == 'FAILED'])
        error_rules = len([r for r in self.results if r.status == 'ERROR'])

        overall_score = (passed_rules / total_rules * 100) if total_rules > 0 else 0

        # Group by severity (assuming all current rules are ERROR level)
        critical_failures = failed_rules  # All failures are critical for now

        # Platform breakdown
        platform_results = {}
        for result in self.results:
            if 'tiki' in result.rule_id.lower() or 'platform' in result.description.lower():
                platform = 'mixed'  # These rules check both platforms
            else:
                platform = 'general'

            if platform not in platform_results:
                platform_results[platform] = {'passed': 0, 'failed': 0, 'total': 0}

            platform_results[platform]['total'] += 1
            if result.status == 'PASSED':
                platform_results[platform]['passed'] += 1
            elif result.status == 'FAILED':
                platform_results[platform]['failed'] += 1

        # Table breakdown
        table_results = {}
        for result in self.results:
            table = result.table_name
            if table not in table_results:
                table_results[table] = {'passed': 0, 'failed': 0, 'total': 0}

            table_results[table]['total'] += 1
            if result.status == 'PASSED':
                table_results[table]['passed'] += 1
            elif result.status == 'FAILED':
                table_results[table]['failed'] += 1

        report = {
            'validation_timestamp': datetime.now().isoformat(),
            'overall_statistics': {
                'total_rules': total_rules,
                'passed_rules': passed_rules,
                'failed_rules': failed_rules,
                'error_rules': error_rules,
                'overall_score': round(overall_score, 2),
                'critical_failures': critical_failures
            },
            'quality_status': self._determine_quality_status(overall_score, critical_failures),
            'table_breakdown': table_results,
            'platform_breakdown': platform_results,
            'detailed_results': [asdict(result) for result in self.results],
            'recommendations': self._generate_recommendations()
        }

        return report

    def _determine_quality_status(self, overall_score: float, critical_failures: int) -> str:
        """Determine overall quality status"""
        if critical_failures > 0:
            return 'CRITICAL'
        elif overall_score >= 95:
            return 'EXCELLENT'
        elif overall_score >= 85:
            return 'GOOD'
        elif overall_score >= 70:
            return 'ACCEPTABLE'
        else:
            return 'POOR'

    def _generate_recommendations(self) -> List[str]:
        """Generate improvement recommendations"""
        recommendations = []

        failed_results = [r for r in self.results if r.status == 'FAILED']

        if failed_results:
            recommendations.append("Review and fix failed validation rules")

            # Specific recommendations based on failed rules
            for result in failed_results:
                if 'price' in result.rule_id.lower():
                    recommendations.append("Review price data quality and validation logic")
                elif 'null' in result.rule_type:
                    recommendations.append(f"Address missing data in {result.column_name}")
                elif 'fresh' in result.rule_id.lower():
                    recommendations.append("Check data pipeline scheduling and execution")

        if not recommendations:
            recommendations.append("Data quality is good - maintain current processes")

        return recommendations

def main():
    """Main entry point for validation"""
    import argparse

    parser = argparse.ArgumentParser(description='Data Quality Validator')
    parser.add_argument('--db-host', default='localhost', help='Database host')
    parser.add_argument('--db-name', default='ecommerce_dss', help='Database name')
    parser.add_argument('--db-user', default='dss_user', help='Database user')
    parser.add_argument('--db-password', default='dss_password_123', help='Database password')
    parser.add_argument('--rule-filter', help='Filter rules by ID pattern')
    parser.add_argument('--report-path', help='Path to save quality report')

    args = parser.parse_args()

    # Database config
    db_config = {
        'host': args.db_host,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_password
    }

    # Run validation
    validator = TikiLazadaDataValidator(db_config)
    results = validator.run_validation(rule_filter=args.rule_filter)

    # Generate report
    report = validator.generate_quality_report()

    print(f"\n🎯 Data Quality Summary:")
    print(f"Overall Score: {report['overall_statistics']['overall_score']:.1f}%")
    print(f"Status: {report['quality_status']}")
    print(f"Rules: {report['overall_statistics']['passed_rules']}/{report['overall_statistics']['total_rules']} passed")

    if args.report_path:
        with open(args.report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"📊 Report saved to: {args.report_path}")

if __name__ == '__main__':
    main()