#!/usr/bin/env python3
"""
Advanced Data Quality Operator for Airflow
Comprehensive data validation, profiling, and anomaly detection
"""

import logging
import json
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta

# Airflow imports
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

# Data processing
import pandas as pd
import numpy as np
from scipy import stats
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.exceptions import ValidationError

# Configure logging
logger = logging.getLogger(__name__)

class DataQualityOperator(BaseOperator):
    """
    Advanced Data Quality Operator with comprehensive validation rules
    
    Features:
    - Schema validation
    - Data profiling
    - Statistical anomaly detection
    - Business rule validation
    - Great Expectations integration
    - Automated alerting
    """
    
    template_fields = ['tables', 'sql_queries', 'validation_date']
    
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        tables: Optional[List[str]] = None,
        sql_queries: Optional[Dict[str, str]] = None,
        validation_rules: Optional[Dict[str, Any]] = None,
        quality_threshold: float = 0.95,
        enable_profiling: bool = True,
        enable_anomaly_detection: bool = True,
        alert_on_failure: bool = True,
        validation_date: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tables = tables or []
        self.sql_queries = sql_queries or {}
        self.validation_rules = validation_rules or {}
        self.quality_threshold = quality_threshold
        self.enable_profiling = enable_profiling
        self.enable_anomaly_detection = enable_anomaly_detection
        self.alert_on_failure = alert_on_failure
        self.validation_date = validation_date
        
        # Initialize validation results storage
        self.validation_results = {
            'timestamp': datetime.now(),
            'tables_validated': 0,
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'quality_score': 0.0,
            'alerts': [],
            'detailed_results': {}
        }

    def execute(self, context: Context) -> Dict[str, Any]:
        """Main execution method"""
        logger.info("🔍 Starting comprehensive data quality validation...")
        
        try:
            # Get database connection
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            
            # Validate each table
            for table in self.tables:
                logger.info(f"🔎 Validating table: {table}")
                table_results = self._validate_table(postgres_hook, table, context)
                self.validation_results['detailed_results'][table] = table_results
                self.validation_results['tables_validated'] += 1
            
            # Run custom SQL validations
            for query_name, sql_query in self.sql_queries.items():
                logger.info(f"🔎 Running custom validation: {query_name}")
                custom_results = self._run_custom_validation(postgres_hook, query_name, sql_query)
                self.validation_results['detailed_results'][f"custom_{query_name}"] = custom_results
            
            # Calculate overall quality score
            self._calculate_quality_score()
            
            # Store results in database
            self._store_validation_results(postgres_hook, context)
            
            # Check if quality threshold is met
            if self.validation_results['quality_score'] < self.quality_threshold:
                self._handle_quality_failure(context)
            
            # Send alerts if configured
            if self.alert_on_failure and self.validation_results['failed_checks'] > 0:
                self._send_alerts(context)
            
            logger.info(f"✅ Data quality validation completed. Score: {self.validation_results['quality_score']:.2%}")
            return self.validation_results
            
        except Exception as e:
            logger.error(f"❌ Data quality validation failed: {e}")
            raise AirflowException(f"Data quality validation failed: {e}")

    def _validate_table(self, postgres_hook: PostgresHook, table: str, context: Context) -> Dict[str, Any]:
        """Comprehensive table validation"""
        table_results = {
            'table_name': table,
            'validation_timestamp': datetime.now(),
            'checks_performed': [],
            'passed_checks': 0,
            'failed_checks': 0,
            'warnings': [],
            'profile_data': {},
            'anomalies': []
        }
        
        try:
            # Get table data
            df = self._get_table_data(postgres_hook, table)
            
            if df.empty:
                table_results['warnings'].append("Table is empty")
                return table_results
            
            # 1. Schema Validation
            schema_results = self._validate_schema(df, table)
            table_results['checks_performed'].append(schema_results)
            
            # 2. Data Completeness Checks
            completeness_results = self._validate_completeness(df, table)
            table_results['checks_performed'].append(completeness_results)
            
            # 3. Data Accuracy Checks
            accuracy_results = self._validate_accuracy(df, table)
            table_results['checks_performed'].append(accuracy_results)
            
            # 4. Data Consistency Checks
            consistency_results = self._validate_consistency(df, table)
            table_results['checks_performed'].append(consistency_results)
            
            # 5. Business Rules Validation
            business_rules_results = self._validate_business_rules(df, table)
            table_results['checks_performed'].append(business_rules_results)
            
            # 6. Data Profiling (if enabled)
            if self.enable_profiling:
                profile_data = self._profile_data(df, table)
                table_results['profile_data'] = profile_data
            
            # 7. Anomaly Detection (if enabled)
            if self.enable_anomaly_detection:
                anomalies = self._detect_anomalies(df, table)
                table_results['anomalies'] = anomalies
            
            # Calculate table-level results
            for check in table_results['checks_performed']:
                if check['passed']:
                    table_results['passed_checks'] += 1
                else:
                    table_results['failed_checks'] += 1
            
            # Update overall results
            self.validation_results['total_checks'] += len(table_results['checks_performed'])
            self.validation_results['passed_checks'] += table_results['passed_checks']
            self.validation_results['failed_checks'] += table_results['failed_checks']
            
            return table_results
            
        except Exception as e:
            logger.error(f"❌ Table validation failed for {table}: {e}")
            table_results['warnings'].append(f"Validation error: {str(e)}")
            return table_results

    def _get_table_data(self, postgres_hook: PostgresHook, table: str, limit: int = 10000) -> pd.DataFrame:
        """Get table data with optional filtering"""
        
        # Base query
        query = f"SELECT * FROM {table}"
        
        # Add date filtering if validation_date is provided
        if self.validation_date:
            if 'created_at' in postgres_hook.get_records(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' AND column_name = 'created_at'"):
                query += f" WHERE DATE(created_at) = '{self.validation_date}'"
            elif 'processing_date' in postgres_hook.get_records(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' AND column_name = 'processing_date'"):
                query += f" WHERE processing_date = '{self.validation_date}'"
        
        # Add limit to avoid memory issues
        query += f" LIMIT {limit}"
        
        return postgres_hook.get_pandas_df(query)

    def _validate_schema(self, df: pd.DataFrame, table: str) -> Dict[str, Any]:
        """Validate table schema"""
        check_result = {
            'check_name': 'schema_validation',
            'table': table,
            'passed': True,
            'details': {},
            'timestamp': datetime.now()
        }
        
        try:
            # Expected schemas for different tables
            expected_schemas = {
                'products_processed': {
                    'required_columns': ['product_id', 'product_category_clean', 'processed_at'],
                    'column_types': {
                        'product_weight_g': ['float64', 'Float64'],
                        'product_volume_cm3': ['float64', 'Float64'],
                        'product_quality_score': ['float64', 'Float64']
                    }
                },
                'customers_processed': {
                    'required_columns': ['customer_id', 'customer_state', 'processed_at'],
                    'column_types': {
                        'customer_zip_code_clean': ['object', 'string']
                    }
                },
                'analytics_products_hourly': {
                    'required_columns': ['aggregation_date', 'aggregation_hour', 'metric_type'],
                    'column_types': {
                        'aggregation_hour': ['int64', 'Int64']
                    }
                }
            }
            
            if table in expected_schemas:
                schema = expected_schemas[table]
                
                # Check required columns
                missing_columns = set(schema['required_columns']) - set(df.columns)
                if missing_columns:
                    check_result['passed'] = False
                    check_result['details']['missing_columns'] = list(missing_columns)
                
                # Check column types
                type_issues = []
                for col, expected_types in schema.get('column_types', {}).items():
                    if col in df.columns:
                        actual_type = str(df[col].dtype)
                        if actual_type not in expected_types:
                            type_issues.append({
                                'column': col,
                                'expected': expected_types,
                                'actual': actual_type
                            })
                
                if type_issues:
                    check_result['passed'] = False
                    check_result['details']['type_issues'] = type_issues
                
                # Record schema info
                check_result['details']['total_columns'] = len(df.columns)
                check_result['details']['total_rows'] = len(df)
                check_result['details']['columns_found'] = list(df.columns)
            
        except Exception as e:
            check_result['passed'] = False
            check_result['details']['error'] = str(e)
        
        return check_result

    def _validate_completeness(self, df: pd.DataFrame, table: str) -> Dict[str, Any]:
        """Validate data completeness"""
        check_result = {
            'check_name': 'completeness_validation',
            'table': table,
            'passed': True,
            'details': {},
            'timestamp': datetime.now()
        }
        
        try:
            # Calculate null percentages
            null_percentages = (df.isnull().sum() / len(df) * 100).round(2)
            
            # Define acceptable null thresholds per column type
            critical_columns = ['id', 'customer_id', 'product_id', 'order_id']  # Should have 0% nulls
            important_columns = ['timestamp', 'created_at', 'processed_at']      # Should have < 1% nulls
            
            completeness_issues = []
            
            for column in df.columns:
                null_pct = null_percentages[column]
                
                if any(critical_col in column.lower() for critical_col in critical_columns):
                    if null_pct > 0:
                        completeness_issues.append({
                            'column': column,
                            'null_percentage': null_pct,
                            'severity': 'critical',
                            'threshold': 0
                        })
                elif any(important_col in column.lower() for important_col in important_columns):
                    if null_pct > 1:
                        completeness_issues.append({
                            'column': column,
                            'null_percentage': null_pct,
                            'severity': 'high',
                            'threshold': 1
                        })
                elif null_pct > 20:  # General threshold for other columns
                    completeness_issues.append({
                        'column': column,
                        'null_percentage': null_pct,
                        'severity': 'medium',
                        'threshold': 20
                    })
            
            if completeness_issues:
                check_result['passed'] = False
                check_result['details']['completeness_issues'] = completeness_issues
            
            # Overall completeness score
            overall_completeness = (100 - null_percentages.mean()).round(2)
            check_result['details']['overall_completeness_score'] = overall_completeness
            check_result['details']['null_percentages'] = null_percentages.to_dict()
            
        except Exception as e:
            check_result['passed'] = False
            check_result['details']['error'] = str(e)
        
        return check_result

    def _validate_accuracy(self, df: pd.DataFrame, table: str) -> Dict[str, Any]:
        """Validate data accuracy"""
        check_result = {
            'check_name': 'accuracy_validation',
            'table': table,
            'passed': True,
            'details': {},
            'timestamp': datetime.now()
        }
        
        try:
            accuracy_issues = []
            
            # Table-specific accuracy checks
            if 'product' in table:
                # Product weight should be positive
                if 'product_weight_g' in df.columns:
                    negative_weights = (df['product_weight_g'] < 0).sum()
                    if negative_weights > 0:
                        accuracy_issues.append({
                            'check': 'product_weight_positive',
                            'failed_records': negative_weights,
                            'percentage': (negative_weights / len(df) * 100).round(2)
                        })
                
                # Product dimensions should be reasonable
                dimension_cols = ['product_length_cm', 'product_width_cm', 'product_height_cm']
                for col in dimension_cols:
                    if col in df.columns:
                        unreasonable_dims = ((df[col] < 0) | (df[col] > 1000)).sum()
                        if unreasonable_dims > 0:
                            accuracy_issues.append({
                                'check': f'{col}_reasonable_range',
                                'failed_records': unreasonable_dims,
                                'percentage': (unreasonable_dims / len(df) * 100).round(2)
                            })
            
            elif 'customer' in table:
                # ZIP codes should be 5 digits
                if 'customer_zip_code_clean' in df.columns:
                    invalid_zips = (~df['customer_zip_code_clean'].str.match(r'^\d{5}$')).sum()
                    if invalid_zips > 0:
                        accuracy_issues.append({
                            'check': 'zip_code_format',
                            'failed_records': invalid_zips,
                            'percentage': (invalid_zips / len(df) * 100).round(2)
                        })
            
            # Generic checks for all tables
            
            # Check for duplicate records
            if len(df.columns) > 0:
                duplicates = df.duplicated().sum()
                if duplicates > 0:
                    accuracy_issues.append({
                        'check': 'duplicate_records',
                        'failed_records': duplicates,
                        'percentage': (duplicates / len(df) * 100).round(2)
                    })
            
            # Check for suspicious outliers in numeric columns
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if col in df.columns:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    outliers = ((df[col] < (Q1 - 3 * IQR)) | (df[col] > (Q3 + 3 * IQR))).sum()
                    
                    if outliers / len(df) > 0.05:  # More than 5% outliers
                        accuracy_issues.append({
                            'check': f'{col}_extreme_outliers',
                            'failed_records': outliers,
                            'percentage': (outliers / len(df) * 100).round(2)
                        })
            
            if accuracy_issues:
                check_result['passed'] = False
                check_result['details']['accuracy_issues'] = accuracy_issues
            
        except Exception as e:
            check_result['passed'] = False
            check_result['details']['error'] = str(e)
        
        return check_result

    def _validate_consistency(self, df: pd.DataFrame, table: str) -> Dict[str, Any]:
        """Validate data consistency"""
        check_result = {
            'check_name': 'consistency_validation',
            'table': table,
            'passed': True,
            'details': {},
            'timestamp': datetime.now()
        }
        
        try:
            consistency_issues = []
            
            # Date consistency checks
            date_columns = [col for col in df.columns if 'date' in col.lower() or 'timestamp' in col.lower()]
            
            for date_col in date_columns:
                if df[date_col].dtype == 'object':
                    # Try to convert to datetime to check format consistency
                    try:
                        pd.to_datetime(df[date_col], errors='raise')
                    except:
                        invalid_dates = pd.to_datetime(df[date_col], errors='coerce').isnull().sum()
                        if invalid_dates > 0:
                            consistency_issues.append({
                                'check': f'{date_col}_format_consistency',
                                'failed_records': invalid_dates,
                                'percentage': (invalid_dates / len(df) * 100).round(2)
                            })
            
            # Category consistency checks
            categorical_cols = df.select_dtypes(include=['object']).columns
            for col in categorical_cols:
                if col in df.columns:
                    # Check for inconsistent casing
                    unique_values = df[col].dropna().unique()
                    if len(unique_values) > 1:
                        lower_values = set(str(v).lower() for v in unique_values)
                        if len(lower_values) < len(unique_values):
                            consistency_issues.append({
                                'check': f'{col}_case_consistency',
                                'details': f'Found {len(unique_values)} unique values, {len(lower_values)} when lowercased'
                            })
            
            # Referential integrity (basic checks)
            if 'customer_id' in df.columns and 'order_id' in df.columns:
                # Check if customer_id pattern is consistent
                id_pattern_consistency = df['customer_id'].str.match(r'^[a-zA-Z0-9_-]+$').all()
                if not id_pattern_consistency:
                    consistency_issues.append({
                        'check': 'customer_id_pattern_consistency',
                        'details': 'Customer IDs do not follow consistent pattern'
                    })
            
            if consistency_issues:
                check_result['passed'] = False
                check_result['details']['consistency_issues'] = consistency_issues
            
        except Exception as e:
            check_result['passed'] = False
            check_result['details']['error'] = str(e)
        
        return check_result

    def _validate_business_rules(self, df: pd.DataFrame, table: str) -> Dict[str, Any]:
        """Validate business rules"""
        check_result = {
            'check_name': 'business_rules_validation',
            'table': table,
            'passed': True,
            'details': {},
            'timestamp': datetime.now()
        }
        
        try:
            business_rule_failures = []
            
            # Get table-specific business rules
            table_rules = self.validation_rules.get(table, {})
            
            # Apply custom business rules if defined
            for rule_name, rule_config in table_rules.items():
                try:
                    if rule_config['type'] == 'sql_condition':
                        # Evaluate SQL-like conditions
                        condition = rule_config['condition']
                        failed_count = len(df.query(f"not ({condition})"))
                        
                        if failed_count > 0:
                            business_rule_failures.append({
                                'rule': rule_name,
                                'condition': condition,
                                'failed_records': failed_count,
                                'percentage': (failed_count / len(df) * 100).round(2)
                            })
                    
                    elif rule_config['type'] == 'range_check':
                        column = rule_config['column']
                        min_val = rule_config.get('min')
                        max_val = rule_config.get('max')
                        
                        if column in df.columns:
                            out_of_range = 0
                            if min_val is not None:
                                out_of_range += (df[column] < min_val).sum()
                            if max_val is not None:
                                out_of_range += (df[column] > max_val).sum()
                            
                            if out_of_range > 0:
                                business_rule_failures.append({
                                    'rule': rule_name,
                                    'column': column,
                                    'range': f"[{min_val}, {max_val}]",
                                    'failed_records': out_of_range,
                                    'percentage': (out_of_range / len(df) * 100).round(2)
                                })
                
                except Exception as rule_error:
                    logger.warning(f"Business rule {rule_name} failed to execute: {rule_error}")
            
            # Default business rules for common patterns
            if 'products' in table:
                # Product quality score should be between 0 and 10
                if 'product_quality_score' in df.columns:
                    invalid_scores = ((df['product_quality_score'] < 0) | (df['product_quality_score'] > 10)).sum()
                    if invalid_scores > 0:
                        business_rule_failures.append({
                            'rule': 'product_quality_score_range',
                            'failed_records': invalid_scores,
                            'percentage': (invalid_scores / len(df) * 100).round(2)
                        })
            
            if business_rule_failures:
                check_result['passed'] = False
                check_result['details']['business_rule_failures'] = business_rule_failures
            
        except Exception as e:
            check_result['passed'] = False
            check_result['details']['error'] = str(e)
        
        return check_result

    def _profile_data(self, df: pd.DataFrame, table: str) -> Dict[str, Any]:
        """Generate comprehensive data profile"""
        profile = {
            'table': table,
            'profiling_timestamp': datetime.now(),
            'basic_stats': {},
            'column_profiles': {},
            'correlations': {}
        }
        
        try:
            # Basic statistics
            profile['basic_stats'] = {
                'row_count': len(df),
                'column_count': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
                'duplicate_rows': df.duplicated().sum()
            }
            
            # Column-level profiling
            for column in df.columns:
                col_profile = {
                    'data_type': str(df[column].dtype),
                    'null_count': df[column].isnull().sum(),
                    'null_percentage': (df[column].isnull().sum() / len(df) * 100).round(2),
                    'unique_count': df[column].nunique(),
                    'unique_percentage': (df[column].nunique() / len(df) * 100).round(2)
                }
                
                # Numeric column profiling
                if pd.api.types.is_numeric_dtype(df[column]):
                    col_profile.update({
                        'min': float(df[column].min()) if pd.notna(df[column].min()) else None,
                        'max': float(df[column].max()) if pd.notna(df[column].max()) else None,
                        'mean': float(df[column].mean()) if pd.notna(df[column].mean()) else None,
                        'median': float(df[column].median()) if pd.notna(df[column].median()) else None,
                        'std': float(df[column].std()) if pd.notna(df[column].std()) else None,
                        'quartiles': {
                            'q1': float(df[column].quantile(0.25)) if pd.notna(df[column].quantile(0.25)) else None,
                            'q3': float(df[column].quantile(0.75)) if pd.notna(df[column].quantile(0.75)) else None
                        }
                    })
                
                # Text column profiling
                elif pd.api.types.is_string_dtype(df[column]) or df[column].dtype == 'object':
                    non_null_values = df[column].dropna()
                    if not non_null_values.empty:
                        col_profile.update({
                            'avg_length': non_null_values.str.len().mean(),
                            'min_length': non_null_values.str.len().min(),
                            'max_length': non_null_values.str.len().max(),
                            'most_common': non_null_values.value_counts().head(5).to_dict()
                        })
                
                profile['column_profiles'][column] = col_profile
            
            # Correlation analysis for numeric columns
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            if len(numeric_columns) > 1:
                corr_matrix = df[numeric_columns].corr()
                # Find high correlations (> 0.8 or < -0.8)
                high_corr_pairs = []
                for i in range(len(corr_matrix.columns)):
                    for j in range(i+1, len(corr_matrix.columns)):
                        corr_val = corr_matrix.iloc[i, j]
                        if abs(corr_val) > 0.8:
                            high_corr_pairs.append({
                                'column1': corr_matrix.columns[i],
                                'column2': corr_matrix.columns[j],
                                'correlation': float(corr_val)
                            })
                
                profile['correlations'] = {
                    'high_correlation_pairs': high_corr_pairs,
                    'correlation_matrix': corr_matrix.round(3).to_dict()
                }
            
        except Exception as e:
            logger.warning(f"Data profiling error for {table}: {e}")
            profile['profiling_error'] = str(e)
        
        return profile

    def _detect_anomalies(self, df: pd.DataFrame, table: str) -> List[Dict[str, Any]]:
        """Detect statistical anomalies in data"""
        anomalies = []
        
        try:
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            
            for column in numeric_columns:
                if df[column].notna().sum() > 10:  # Need sufficient data points
                    
                    # Statistical outlier detection using Z-score
                    z_scores = np.abs(stats.zscore(df[column].dropna()))
                    outliers_count = (z_scores > 3).sum()
                    
                    if outliers_count > 0:
                        anomalies.append({
                            'type': 'statistical_outlier',
                            'column': column,
                            'method': 'z_score',
                            'threshold': 3,
                            'anomaly_count': int(outliers_count),
                            'anomaly_percentage': (outliers_count / len(df) * 100).round(2)
                        })
                    
                    # Distribution change detection (if we have historical data)
                    # This would require storing historical statistics
                    
                    # Sudden spikes detection
                    if 'timestamp' in df.columns or 'created_at' in df.columns:
                        time_col = 'timestamp' if 'timestamp' in df.columns else 'created_at'
                        
                        # Group by time periods and look for sudden changes
                        try:
                            df_time = df.copy()
                            df_time[time_col] = pd.to_datetime(df_time[time_col])
                            hourly_stats = df_time.groupby(df_time[time_col].dt.hour)[column].agg(['count', 'mean', 'std'])
                            
                            # Look for hours with unusual activity
                            mean_count = hourly_stats['count'].mean()
                            std_count = hourly_stats['count'].std()
                            
                            unusual_hours = hourly_stats[hourly_stats['count'] > (mean_count + 2 * std_count)]
                            
                            if len(unusual_hours) > 0:
                                anomalies.append({
                                    'type': 'temporal_spike',
                                    'column': column,
                                    'time_column': time_col,
                                    'unusual_periods': unusual_hours.index.tolist(),
                                    'details': 'Unusual activity levels detected in specific time periods'
                                })
                        
                        except Exception as temporal_error:
                            logger.debug(f"Temporal anomaly detection failed for {column}: {temporal_error}")
        
        except Exception as e:
            logger.warning(f"Anomaly detection error for {table}: {e}")
            anomalies.append({
                'type': 'detection_error',
                'error': str(e)
            })
        
        return anomalies

    def _run_custom_validation(self, postgres_hook: PostgresHook, query_name: str, sql_query: str) -> Dict[str, Any]:
        """Run custom SQL validation query"""
        check_result = {
            'check_name': f'custom_{query_name}',
            'query': sql_query,
            'passed': True,
            'details': {},
            'timestamp': datetime.now()
        }
        
        try:
            result = postgres_hook.get_pandas_df(sql_query)
            
            # Interpret results based on query structure
            if len(result) == 1 and len(result.columns) == 1:
                # Single value result - interpret as pass/fail count
                value = result.iloc[0, 0]
                if isinstance(value, (int, float)):
                    if value > 0:
                        check_result['passed'] = False
                        check_result['details']['failed_records'] = int(value)
                    else:
                        check_result['details']['status'] = 'All records passed validation'
            else:
                # Multiple results - store for analysis
                check_result['details']['result_count'] = len(result)
                check_result['details']['sample_results'] = result.head(10).to_dict('records')
            
            self.validation_results['total_checks'] += 1
            if check_result['passed']:
                self.validation_results['passed_checks'] += 1
            else:
                self.validation_results['failed_checks'] += 1
            
        except Exception as e:
            check_result['passed'] = False
            check_result['details']['error'] = str(e)
            self.validation_results['total_checks'] += 1
            self.validation_results['failed_checks'] += 1
        
        return check_result

    def _calculate_quality_score(self):
        """Calculate overall data quality score"""
        if self.validation_results['total_checks'] > 0:
            self.validation_results['quality_score'] = (
                self.validation_results['passed_checks'] / 
                self.validation_results['total_checks']
            )
        else:
            self.validation_results['quality_score'] = 0.0

    def _store_validation_results(self, postgres_hook: PostgresHook, context: Context):
        """Store validation results in database"""
        try:
            # Create results summary record
            insert_query = """
            INSERT INTO data_quality_results (
                dag_id, task_id, execution_date, run_id,
                tables_validated, total_checks, passed_checks, failed_checks,
                quality_score, validation_results, created_at
            ) VALUES (
                %(dag_id)s, %(task_id)s, %(execution_date)s, %(run_id)s,
                %(tables_validated)s, %(total_checks)s, %(passed_checks)s, %(failed_checks)s,
                %(quality_score)s, %(validation_results)s, %(created_at)s
            )
            """
            
            params = {
                'dag_id': context['dag'].dag_id,
                'task_id': context['task'].task_id,
                'execution_date': context['execution_date'],
                'run_id': context['dag_run'].run_id,
                'tables_validated': self.validation_results['tables_validated'],
                'total_checks': self.validation_results['total_checks'],
                'passed_checks': self.validation_results['passed_checks'],
                'failed_checks': self.validation_results['failed_checks'],
                'quality_score': self.validation_results['quality_score'],
                'validation_results': json.dumps(self.validation_results, default=str),
                'created_at': datetime.now()
            }
            
            postgres_hook.run(insert_query, parameters=params)
            logger.info("✅ Validation results stored in database")
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to store validation results: {e}")

    def _handle_quality_failure(self, context: Context):
        """Handle data quality failure scenarios"""
        failure_msg = (
            f"❌ Data quality check failed!\n"
            f"Quality Score: {self.validation_results['quality_score']:.2%}\n"
            f"Threshold: {self.quality_threshold:.2%}\n"
            f"Failed Checks: {self.validation_results['failed_checks']}\n"
            f"Total Checks: {self.validation_results['total_checks']}"
        )
        
        logger.error(failure_msg)
        
        # Add to alerts
        self.validation_results['alerts'].append({
            'type': 'quality_failure',
            'severity': 'high',
            'message': failure_msg,
            'timestamp': datetime.now()
        })
        
        # Raise exception to fail the task
        raise AirflowException(failure_msg)

    def _send_alerts(self, context: Context):
        """Send data quality alerts"""
        try:
            # This would integrate with your alerting system
            # For now, just log the alerts
            
            alert_summary = {
                'dag_id': context['dag'].dag_id,
                'task_id': context['task'].task_id,
                'execution_date': context['execution_date'],
                'quality_score': self.validation_results['quality_score'],
                'failed_checks': self.validation_results['failed_checks'],
                'alerts': self.validation_results['alerts']
            }
            
            logger.warning(f"🚨 Data Quality Alerts: {json.dumps(alert_summary, default=str, indent=2)}")
            
            # Here you would send to Slack, email, PagerDuty, etc.
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to send alerts: {e}")