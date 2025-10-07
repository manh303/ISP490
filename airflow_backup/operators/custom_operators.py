#!/usr/bin/env python3
"""
Custom Airflow Operators for E-commerce DSS Pipeline
================================================
Specialized operators for data processing, ML, and monitoring tasks
"""

import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException

import pymongo
import redis
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from sklearn.base import BaseEstimator
import joblib
import numpy as np

# ================================
# BASE OPERATOR
# ================================

class BaseDSSOperator(BaseOperator):
    """Base operator with common functionality for DSS pipeline"""

    template_fields = ['execution_date', 'dag_run']

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        mongo_uri: str = None,
        redis_url: str = None,
        kafka_servers: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.mongo_uri = mongo_uri or "mongodb://admin:admin_password@mongodb:27017/"
        self.redis_url = redis_url or "redis://redis:6379/0"
        self.kafka_servers = kafka_servers or "kafka:29092"

    def get_postgres_hook(self):
        """Get PostgreSQL hook"""
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)

    def get_mongo_client(self):
        """Get MongoDB client"""
        return pymongo.MongoClient(self.mongo_uri)

    def get_redis_client(self):
        """Get Redis client"""
        return redis.from_url(self.redis_url)

    def send_alert(self, message: str, severity: str = "INFO", metadata: Dict = None):
        """Send alert to monitoring system"""
        try:
            mongo_client = self.get_mongo_client()
            db = mongo_client['ecommerce_dss']

            alert_doc = {
                'alert_id': f"{self.task_id}_{datetime.now().isoformat()}",
                'alert_type': 'operator_alert',
                'severity': severity,
                'title': f'{self.task_id} - {severity}',
                'message': message,
                'component': 'airflow',
                'source': self.task_id,
                'triggered_at': datetime.now(),
                'is_active': True,
                'metadata': metadata or {}
            }

            db.alerts.insert_one(alert_doc)
            logging.info(f"Alert sent from {self.task_id}: {message}")

        except Exception as e:
            logging.error(f"Failed to send alert from {self.task_id}: {str(e)}")

# ================================
# DATA COLLECTION OPERATORS
# ================================

class KafkaTopicOperator(BaseDSSOperator):
    """Operator to manage Kafka topics"""

    @apply_defaults
    def __init__(
        self,
        topics: List[Dict[str, Any]],
        action: str = 'create',  # create, delete, list
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.topics = topics
        self.action = action

    def execute(self, context):
        """Execute topic management"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers,
                client_id=f'airflow_{self.task_id}'
            )

            if self.action == 'create':
                return self._create_topics(admin_client, context)
            elif self.action == 'delete':
                return self._delete_topics(admin_client, context)
            elif self.action == 'list':
                return self._list_topics(admin_client, context)
            else:
                raise AirflowException(f"Unsupported action: {self.action}")

        except Exception as e:
            self.send_alert(f"Kafka topic operation failed: {str(e)}", "HIGH")
            raise

    def _create_topics(self, admin_client, context):
        """Create Kafka topics"""
        topic_objects = []

        for topic_config in self.topics:
            topic = NewTopic(
                name=topic_config['name'],
                num_partitions=topic_config.get('partitions', 3),
                replication_factor=topic_config.get('replication_factor', 1)
            )
            topic_objects.append(topic)

        try:
            admin_client.create_topics(topic_objects)

            # Verify creation
            topic_metadata = admin_client.list_topics()
            created_topics = [t.name for t in topic_objects if t.name in topic_metadata.topics]

            result = {
                'action': 'create',
                'requested_topics': len(topic_objects),
                'created_topics': len(created_topics),
                'topic_names': created_topics
            }

            self.send_alert(f"Created {len(created_topics)} Kafka topics", "INFO")
            return result

        except Exception as e:
            logging.error(f"Failed to create topics: {str(e)}")
            # Check which topics already exist
            existing_topics = []
            try:
                topic_metadata = admin_client.list_topics()
                existing_topics = [t.name for t in topic_objects if t.name in topic_metadata.topics]
            except:
                pass

            return {
                'action': 'create',
                'error': str(e),
                'existing_topics': existing_topics
            }

    def _delete_topics(self, admin_client, context):
        """Delete Kafka topics"""
        topic_names = [topic['name'] for topic in self.topics]

        try:
            admin_client.delete_topics(topic_names)

            result = {
                'action': 'delete',
                'deleted_topics': topic_names
            }

            self.send_alert(f"Deleted {len(topic_names)} Kafka topics", "INFO")
            return result

        except Exception as e:
            logging.error(f"Failed to delete topics: {str(e)}")
            return {
                'action': 'delete',
                'error': str(e),
                'requested_topics': topic_names
            }

    def _list_topics(self, admin_client, context):
        """List Kafka topics"""
        try:
            topic_metadata = admin_client.list_topics()
            topics = list(topic_metadata.topics.keys())

            result = {
                'action': 'list',
                'topic_count': len(topics),
                'topics': topics
            }

            return result

        except Exception as e:
            logging.error(f"Failed to list topics: {str(e)}")
            return {
                'action': 'list',
                'error': str(e)
            }

class ApiDataCollectorOperator(BaseDSSOperator):
    """Operator to collect data from REST APIs"""

    @apply_defaults
    def __init__(
        self,
        api_configs: List[Dict[str, Any]],
        storage_location: str = 'mongodb',  # mongodb, postgres, file
        batch_size: int = 1000,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_configs = api_configs
        self.storage_location = storage_location
        self.batch_size = batch_size

    def execute(self, context):
        """Execute API data collection"""
        collection_results = {}

        try:
            for config in self.api_configs:
                result = self._collect_from_api(config, context)
                collection_results[config['name']] = result

            # Store summary
            total_records = sum(r.get('records_collected', 0) for r in collection_results.values())
            successful_apis = len([r for r in collection_results.values() if r.get('status') == 'success'])

            self.send_alert(
                f"API data collection: {total_records} records from {successful_apis}/{len(self.api_configs)} APIs",
                "INFO"
            )

            return collection_results

        except Exception as e:
            self.send_alert(f"API data collection failed: {str(e)}", "HIGH")
            raise

    def _collect_from_api(self, config: Dict[str, Any], context) -> Dict[str, Any]:
        """Collect data from a single API"""
        try:
            # Make HTTP request
            http_hook = HttpHook(
                method=config.get('method', 'GET'),
                http_conn_id=config.get('conn_id', 'http_default')
            )

            response = http_hook.run(
                endpoint=config['endpoint'],
                headers=config.get('headers', {}),
                data=config.get('data'),
                extra_options=config.get('extra_options', {})
            )

            # Parse response
            if config.get('response_type', 'json') == 'json':
                data = response.json()
            else:
                data = response.text

            # Transform data if needed
            if 'transform_function' in config:
                data = config['transform_function'](data)

            # Store data
            if isinstance(data, list):
                records = data
            else:
                records = [data]

            self._store_data(records, config, context)

            return {
                'status': 'success',
                'records_collected': len(records),
                'api_name': config['name'],
                'timestamp': datetime.now()
            }

        except Exception as e:
            logging.error(f"Failed to collect from API {config['name']}: {str(e)}")
            return {
                'status': 'failed',
                'api_name': config['name'],
                'error': str(e),
                'timestamp': datetime.now()
            }

    def _store_data(self, records: List[Dict], config: Dict, context):
        """Store collected data"""
        if self.storage_location == 'mongodb':
            self._store_in_mongodb(records, config, context)
        elif self.storage_location == 'postgres':
            self._store_in_postgres(records, config, context)
        elif self.storage_location == 'file':
            self._store_in_file(records, config, context)

    def _store_in_mongodb(self, records: List[Dict], config: Dict, context):
        """Store data in MongoDB"""
        mongo_client = self.get_mongo_client()
        db = mongo_client['ecommerce_dss']

        collection_name = config.get('collection', f"api_data_{config['name']}")
        collection = db[collection_name]

        # Add metadata to records
        for record in records:
            record.update({
                'collected_at': datetime.now(),
                'collection_source': config['name'],
                'dag_run_id': context['dag_run'].run_id,
                'task_id': self.task_id
            })

        # Insert in batches
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            collection.insert_many(batch)

    def _store_in_postgres(self, records: List[Dict], config: Dict, context):
        """Store data in PostgreSQL"""
        if not records:
            return

        df = pd.DataFrame(records)

        # Add metadata columns
        df['collected_at'] = datetime.now()
        df['collection_source'] = config['name']
        df['dag_run_id'] = context['dag_run'].run_id
        df['task_id'] = self.task_id

        # Store in database
        hook = self.get_postgres_hook()
        engine = hook.get_sqlalchemy_engine()

        table_name = config.get('table', f"api_data_{config['name']}")
        df.to_sql(table_name, engine, if_exists='append', index=False)

    def _store_in_file(self, records: List[Dict], config: Dict, context):
        """Store data in file"""
        import os

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{config['name']}_data_{timestamp}.json"
        filepath = f"/app/data/collected/{filename}"

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, 'w') as f:
            json.dump(records, f, indent=2, default=str)

# ================================
# ML OPERATORS
# ================================

class MLModelTrainingOperator(BaseDSSOperator):
    """Operator for training machine learning models"""

    @apply_defaults
    def __init__(
        self,
        model_config: Dict[str, Any],
        feature_query: str = None,
        target_column: str = None,
        test_size: float = 0.2,
        model_path: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.model_config = model_config
        self.feature_query = feature_query
        self.target_column = target_column
        self.test_size = test_size
        self.model_path = model_path or f"/app/models/{self.model_config['name']}.pkl"

    def execute(self, context):
        """Execute model training"""
        try:
            # Load data
            data = self._load_training_data(context)

            # Prepare features
            X, y = self._prepare_features(data)

            # Split data
            from sklearn.model_selection import train_test_split
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=self.test_size, random_state=42
            )

            # Train model
            model = self._create_model()
            model.fit(X_train, y_train)

            # Evaluate model
            metrics = self._evaluate_model(model, X_test, y_test)

            # Save model
            self._save_model(model, metrics, context)

            result = {
                'model_name': self.model_config['name'],
                'training_samples': len(X_train),
                'test_samples': len(X_test),
                'metrics': metrics,
                'model_path': self.model_path,
                'trained_at': datetime.now()
            }

            self.send_alert(
                f"Model {self.model_config['name']} trained successfully. Performance: {metrics}",
                "INFO"
            )

            return result

        except Exception as e:
            self.send_alert(f"Model training failed: {str(e)}", "HIGH")
            raise

    def _load_training_data(self, context) -> pd.DataFrame:
        """Load training data"""
        if self.feature_query:
            # Load from PostgreSQL
            hook = self.get_postgres_hook()
            engine = hook.get_sqlalchemy_engine()
            return pd.read_sql(self.feature_query, engine)
        else:
            # Load from MongoDB
            mongo_client = self.get_mongo_client()
            db = mongo_client['ecommerce_dss']

            collection_name = self.model_config.get('data_collection', 'ml_features')
            docs = list(db[collection_name].find({}, {'_id': 0}))
            return pd.DataFrame(docs)

    def _prepare_features(self, data: pd.DataFrame):
        """Prepare features and target"""
        feature_columns = self.model_config.get('feature_columns', [])
        target_column = self.target_column or self.model_config.get('target_column')

        if not feature_columns:
            # Use all numeric columns except target
            feature_columns = [col for col in data.select_dtypes(include=[np.number]).columns
                             if col != target_column]

        X = data[feature_columns].fillna(0)
        y = data[target_column]

        return X, y

    def _create_model(self) -> BaseEstimator:
        """Create model instance"""
        model_type = self.model_config['type']
        model_params = self.model_config.get('parameters', {})

        if model_type == 'random_forest':
            from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
            task_type = self.model_config.get('task', 'regression')

            if task_type == 'regression':
                return RandomForestRegressor(**model_params)
            else:
                return RandomForestClassifier(**model_params)

        elif model_type == 'linear':
            from sklearn.linear_model import LinearRegression, LogisticRegression
            task_type = self.model_config.get('task', 'regression')

            if task_type == 'regression':
                return LinearRegression(**model_params)
            else:
                return LogisticRegression(**model_params)

        elif model_type == 'gradient_boosting':
            from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
            task_type = self.model_config.get('task', 'regression')

            if task_type == 'regression':
                return GradientBoostingRegressor(**model_params)
            else:
                return GradientBoostingClassifier(**model_params)

        else:
            raise AirflowException(f"Unsupported model type: {model_type}")

    def _evaluate_model(self, model: BaseEstimator, X_test: pd.DataFrame, y_test: pd.Series) -> Dict:
        """Evaluate trained model"""
        y_pred = model.predict(X_test)

        task_type = self.model_config.get('task', 'regression')

        if task_type == 'regression':
            from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

            metrics = {
                'mae': float(mean_absolute_error(y_test, y_pred)),
                'mse': float(mean_squared_error(y_test, y_pred)),
                'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
                'r2': float(r2_score(y_test, y_pred))
            }
        else:
            from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

            metrics = {
                'accuracy': float(accuracy_score(y_test, y_pred)),
                'precision': float(precision_score(y_test, y_pred, average='weighted')),
                'recall': float(recall_score(y_test, y_pred, average='weighted')),
                'f1': float(f1_score(y_test, y_pred, average='weighted'))
            }

        return metrics

    def _save_model(self, model: BaseEstimator, metrics: Dict, context):
        """Save trained model"""
        import os

        # Save model file
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        joblib.dump(model, self.model_path)

        # Save metadata to MongoDB
        mongo_client = self.get_mongo_client()
        db = mongo_client['ecommerce_dss']

        model_metadata = {
            'model_name': self.model_config['name'],
            'model_version': self.model_config.get('version', '1.0'),
            'model_type': self.model_config['type'],
            'task_type': self.model_config.get('task', 'regression'),
            'trained_at': datetime.now(),
            'performance_metrics': metrics,
            'model_path': self.model_path,
            'feature_columns': list(model.feature_names_in_) if hasattr(model, 'feature_names_in_') else [],
            'dag_run_id': context['dag_run'].run_id,
            'task_id': self.task_id,
            'training_config': self.model_config
        }

        db.ml_models.insert_one(model_metadata)

class MLPredictionOperator(BaseDSSOperator):
    """Operator for generating ML predictions"""

    @apply_defaults
    def __init__(
        self,
        model_name: str,
        input_data_source: str = 'postgres',  # postgres, mongodb
        input_query: str = None,
        batch_size: int = 1000,
        output_collection: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.model_name = model_name
        self.input_data_source = input_data_source
        self.input_query = input_query
        self.batch_size = batch_size
        self.output_collection = output_collection or f"predictions_{model_name}"

    def execute(self, context):
        """Execute prediction generation"""
        try:
            # Load model
            model, model_metadata = self._load_model()

            # Load input data
            input_data = self._load_input_data()

            # Generate predictions
            predictions = self._generate_predictions(model, input_data, model_metadata)

            # Store predictions
            self._store_predictions(predictions, context)

            result = {
                'model_name': self.model_name,
                'predictions_generated': len(predictions),
                'input_samples': len(input_data),
                'model_version': model_metadata.get('model_version'),
                'predicted_at': datetime.now()
            }

            self.send_alert(
                f"Generated {len(predictions)} predictions using {self.model_name}",
                "INFO"
            )

            return result

        except Exception as e:
            self.send_alert(f"ML prediction generation failed: {str(e)}", "HIGH")
            raise

    def _load_model(self):
        """Load trained model and metadata"""
        mongo_client = self.get_mongo_client()
        db = mongo_client['ecommerce_dss']

        # Get latest model metadata
        model_doc = db.ml_models.find_one(
            {'model_name': self.model_name},
            sort=[('trained_at', -1)]
        )

        if not model_doc:
            raise AirflowException(f"No trained model found for {self.model_name}")

        # Load model file
        model_path = model_doc['model_path']
        if not os.path.exists(model_path):
            raise AirflowException(f"Model file not found: {model_path}")

        model = joblib.load(model_path)

        return model, model_doc

    def _load_input_data(self) -> pd.DataFrame:
        """Load input data for predictions"""
        if self.input_data_source == 'postgres':
            hook = self.get_postgres_hook()
            engine = hook.get_sqlalchemy_engine()

            query = self.input_query or f"SELECT * FROM ml_features LIMIT {self.batch_size}"
            return pd.read_sql(query, engine)

        elif self.input_data_source == 'mongodb':
            mongo_client = self.get_mongo_client()
            db = mongo_client['ecommerce_dss']

            collection_name = self.input_query or 'ml_features'
            docs = list(db[collection_name].find({}, {'_id': 0}).limit(self.batch_size))
            return pd.DataFrame(docs)

        else:
            raise AirflowException(f"Unsupported input data source: {self.input_data_source}")

    def _generate_predictions(self, model, input_data: pd.DataFrame, model_metadata: Dict) -> List[Dict]:
        """Generate predictions"""
        predictions = []

        # Prepare features
        feature_columns = model_metadata.get('feature_columns', [])
        if feature_columns:
            X = input_data[feature_columns].fillna(0)
        else:
            # Use all numeric columns
            X = input_data.select_dtypes(include=[np.number]).fillna(0)

        # Generate predictions
        y_pred = model.predict(X)

        # Calculate confidence scores (simplified)
        if hasattr(model, 'predict_proba'):
            # For classifiers
            probas = model.predict_proba(X)
            confidence_scores = probas.max(axis=1)
        else:
            # For regressors - simplified confidence based on prediction consistency
            confidence_scores = np.full(len(y_pred), 0.85)

        # Create prediction documents
        for i, (idx, row) in enumerate(input_data.iterrows()):
            pred_doc = {
                'model_name': self.model_name,
                'model_version': model_metadata.get('model_version'),
                'prediction': float(y_pred[i]),
                'confidence_score': float(confidence_scores[i]),
                'input_features': {col: float(row[col]) if pd.notna(row[col]) else 0
                                 for col in feature_columns},
                'predicted_at': datetime.now(),
                'prediction_type': model_metadata.get('task_type', 'regression')
            }

            # Add identifier if available
            if 'customer_id' in row:
                pred_doc['customer_id'] = row['customer_id']
            if 'product_id' in row:
                pred_doc['product_id'] = row['product_id']

            predictions.append(pred_doc)

        return predictions

    def _store_predictions(self, predictions: List[Dict], context):
        """Store predictions in MongoDB"""
        mongo_client = self.get_mongo_client()
        db = mongo_client['ecommerce_dss']

        # Add task metadata
        for pred in predictions:
            pred.update({
                'dag_run_id': context['dag_run'].run_id,
                'task_id': self.task_id
            })

        # Store in batches
        collection = db[self.output_collection]
        for i in range(0, len(predictions), self.batch_size):
            batch = predictions[i:i + self.batch_size]
            collection.insert_many(batch)

# ================================
# MONITORING OPERATORS
# ================================

class DataQualityOperator(BaseDSSOperator):
    """Operator for data quality checks"""

    @apply_defaults
    def __init__(
        self,
        quality_checks: List[Dict[str, Any]],
        threshold: float = 0.8,  # Overall quality threshold
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.quality_checks = quality_checks
        self.threshold = threshold

    def execute(self, context):
        """Execute data quality checks"""
        try:
            results = {}

            for check in self.quality_checks:
                check_result = self._execute_quality_check(check)
                results[check['name']] = check_result

            # Calculate overall quality score
            total_checks = len(self.quality_checks)
            passed_checks = len([r for r in results.values() if r.get('passed', False)])
            quality_score = (passed_checks / total_checks) if total_checks > 0 else 0

            results['summary'] = {
                'total_checks': total_checks,
                'passed_checks': passed_checks,
                'quality_score': quality_score,
                'threshold': self.threshold,
                'meets_threshold': quality_score >= self.threshold
            }

            # Store results in MongoDB
            self._store_quality_results(results, context)

            # Send alerts based on quality score
            if quality_score >= 0.9:
                severity = "INFO"
                message = f"Excellent data quality: {quality_score:.1%}"
            elif quality_score >= self.threshold:
                severity = "MEDIUM"
                message = f"Good data quality: {quality_score:.1%}"
            else:
                severity = "HIGH"
                message = f"Poor data quality: {quality_score:.1%} - Below threshold {self.threshold:.1%}"

            self.send_alert(message, severity, {'quality_score': quality_score})

            # Fail task if quality is below threshold and strict mode
            if quality_score < self.threshold and self.quality_checks[0].get('strict', False):
                raise AirflowException(f"Data quality below threshold: {quality_score:.1%} < {self.threshold:.1%}")

            return results

        except Exception as e:
            self.send_alert(f"Data quality check failed: {str(e)}", "CRITICAL")
            raise

    def _execute_quality_check(self, check: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single quality check"""
        try:
            check_type = check['type']

            if check_type == 'sql':
                return self._execute_sql_check(check)
            elif check_type == 'mongodb':
                return self._execute_mongodb_check(check)
            elif check_type == 'custom':
                return self._execute_custom_check(check)
            else:
                raise AirflowException(f"Unsupported check type: {check_type}")

        except Exception as e:
            return {
                'check_name': check['name'],
                'passed': False,
                'error': str(e),
                'timestamp': datetime.now()
            }

    def _execute_sql_check(self, check: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SQL-based quality check"""
        hook = self.get_postgres_hook()

        query = check['query']
        expected_operator = check.get('operator', '>')
        expected_value = check.get('expected_value', 0)

        # Execute query
        records = hook.get_records(query)

        if not records or not records[0]:
            actual_value = 0
        else:
            actual_value = records[0][0]

        # Evaluate condition
        passed = self._evaluate_condition(actual_value, expected_operator, expected_value)

        return {
            'check_name': check['name'],
            'check_type': 'sql',
            'query': query,
            'actual_value': actual_value,
            'expected_operator': expected_operator,
            'expected_value': expected_value,
            'passed': passed,
            'timestamp': datetime.now()
        }

    def _execute_mongodb_check(self, check: Dict[str, Any]) -> Dict[str, Any]:
        """Execute MongoDB-based quality check"""
        mongo_client = self.get_mongo_client()
        db = mongo_client['ecommerce_dss']

        collection_name = check['collection']
        aggregation_pipeline = check.get('pipeline', [])
        expected_operator = check.get('operator', '>')
        expected_value = check.get('expected_value', 0)

        collection = db[collection_name]

        if aggregation_pipeline:
            # Use aggregation pipeline
            results = list(collection.aggregate(aggregation_pipeline))
            actual_value = results[0]['value'] if results and 'value' in results[0] else 0
        else:
            # Simple count
            actual_value = collection.count_documents(check.get('filter', {}))

        # Evaluate condition
        passed = self._evaluate_condition(actual_value, expected_operator, expected_value)

        return {
            'check_name': check['name'],
            'check_type': 'mongodb',
            'collection': collection_name,
            'actual_value': actual_value,
            'expected_operator': expected_operator,
            'expected_value': expected_value,
            'passed': passed,
            'timestamp': datetime.now()
        }

    def _execute_custom_check(self, check: Dict[str, Any]) -> Dict[str, Any]:
        """Execute custom quality check"""
        check_function = check['function']
        check_args = check.get('args', {})

        # Execute custom function
        result = check_function(**check_args)

        if isinstance(result, dict):
            result.update({
                'check_name': check['name'],
                'check_type': 'custom',
                'timestamp': datetime.now()
            })
            return result
        else:
            return {
                'check_name': check['name'],
                'check_type': 'custom',
                'passed': bool(result),
                'actual_value': result,
                'timestamp': datetime.now()
            }

    def _evaluate_condition(self, actual: Union[int, float], operator: str, expected: Union[int, float]) -> bool:
        """Evaluate quality check condition"""
        if operator == '>':
            return actual > expected
        elif operator == '>=':
            return actual >= expected
        elif operator == '<':
            return actual < expected
        elif operator == '<=':
            return actual <= expected
        elif operator == '==':
            return actual == expected
        elif operator == '!=':
            return actual != expected
        else:
            raise AirflowException(f"Unsupported operator: {operator}")

    def _store_quality_results(self, results: Dict, context):
        """Store quality check results"""
        mongo_client = self.get_mongo_client()
        db = mongo_client['ecommerce_dss']

        quality_doc = {
            'timestamp': datetime.now(),
            'dag_run_id': context['dag_run'].run_id,
            'task_id': self.task_id,
            'execution_date': context['execution_date'],
            'quality_results': results
        }

        db.data_quality_results.insert_one(quality_doc)

# ================================
# UTILITY OPERATORS
# ================================

class SlackAlertOperator(BaseDSSOperator):
    """Operator to send Slack notifications"""

    @apply_defaults
    def __init__(
        self,
        slack_conn_id: str = 'slack_default',
        channel: str = '#data-alerts',
        message: str = None,
        message_template: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.slack_conn_id = slack_conn_id
        self.channel = channel
        self.message = message
        self.message_template = message_template

    def execute(self, context):
        """Send Slack notification"""
        try:
            from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

            # Prepare message
            if self.message_template:
                # Render template with context
                message = self.message_template.format(**context)
            else:
                message = self.message or f"Task {self.task_id} completed"

            # Send Slack message
            slack_operator = SlackWebhookOperator(
                task_id=f"{self.task_id}_slack_send",
                http_conn_id=self.slack_conn_id,
                message=message,
                channel=self.channel
            )

            slack_operator.execute(context)

            return {
                'status': 'sent',
                'channel': self.channel,
                'message_length': len(message),
                'timestamp': datetime.now()
            }

        except Exception as e:
            logging.error(f"Failed to send Slack notification: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now()
            }