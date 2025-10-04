#!/usr/bin/env python3
"""
Schema Registry and Validation System
Manages data schemas across the entire pipeline with version control and validation
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from pathlib import Path
from enum import Enum
import hashlib

# Data processing and validation
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field, validator, ValidationError
from pydantic.types import UUID4
import jsonschema
from jsonschema import validate, Draft7Validator
import great_expectations as ge

# Database connections
from sqlalchemy import create_engine, text
from pymongo import MongoClient
import redis

# Configure logging
logger = logging.getLogger(__name__)

class SchemaType(str, Enum):
    """Schema types supported by the system"""
    KAFKA = "kafka"
    POSTGRES = "postgres"
    MONGODB = "mongodb"
    API = "api"
    CSV = "csv"
    PARQUET = "parquet"

class ValidationLevel(str, Enum):
    """Validation strictness levels"""
    STRICT = "strict"        # All validations must pass
    MODERATE = "moderate"    # Critical validations must pass, warnings allowed
    LENIENT = "lenient"     # Only structural validations required

class SchemaStatus(str, Enum):
    """Schema version status"""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"

class SchemaDefinition(BaseModel):
    """Pydantic model for schema definitions"""
    
    schema_id: str = Field(..., description="Unique schema identifier")
    schema_name: str = Field(..., description="Human-readable schema name")
    schema_type: SchemaType = Field(..., description="Type of schema")
    version: str = Field(..., description="Schema version (semantic versioning)")
    description: Optional[str] = Field(None, description="Schema description")
    
    # Schema content
    json_schema: Dict[str, Any] = Field(..., description="JSON schema definition")
    sample_data: Optional[Dict[str, Any]] = Field(None, description="Sample valid data")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = Field(..., description="Schema creator")
    status: SchemaStatus = Field(default=SchemaStatus.DRAFT)
    
    # Compatibility and dependencies
    compatible_versions: List[str] = Field(default_factory=list)
    breaking_changes: List[str] = Field(default_factory=list)
    dependencies: List[str] = Field(default_factory=list)
    
    # Validation rules
    validation_level: ValidationLevel = Field(default=ValidationLevel.MODERATE)
    custom_validators: Optional[Dict[str, Any]] = Field(None)
    
    @validator('version')
    def validate_version_format(cls, v):
        """Validate semantic version format"""
        import re
        if not re.match(r'^\d+\.\d+\.\d+$', v):
            raise ValueError('Version must follow semantic versioning (x.y.z)')
        return v
    
    @validator('json_schema')
    def validate_json_schema(cls, v):
        """Validate that JSON schema is valid"""
        try:
            Draft7Validator.check_schema(v)
        except jsonschema.SchemaError as e:
            raise ValueError(f'Invalid JSON schema: {e}')
        return v

class SchemaRegistry:
    """
    Centralized schema registry for managing data schemas across the pipeline
    """
    
    def __init__(self, 
                 registry_backend: str = "postgres",
                 postgres_url: Optional[str] = None,
                 mongodb_url: Optional[str] = None,
                 redis_url: Optional[str] = None,
                 schema_storage_path: str = "/app/schemas"):
        
        self.registry_backend = registry_backend
        self.schema_storage_path = Path(schema_storage_path)
        self.schema_storage_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize connections
        self.postgres_engine = None
        self.mongo_client = None
        self.redis_client = None
        
        if postgres_url:
            self.postgres_engine = create_engine(postgres_url)
        if mongodb_url:
            self.mongo_client = MongoClient(mongodb_url)
        if redis_url:
            import redis
            self.redis_client = redis.from_url(redis_url)
        
        # Schema cache
        self.schema_cache = {}
        self.validators_cache = {}
        
        # Initialize registry
        self._initialize_registry()

    def _initialize_registry(self):
        """Initialize schema registry storage"""
        
        if self.registry_backend == "postgres" and self.postgres_engine:
            self._init_postgres_registry()
        elif self.registry_backend == "mongodb" and self.mongo_client:
            self._init_mongodb_registry()
        
        # Load default schemas
        self._load_default_schemas()

    def _init_postgres_registry(self):
        """Initialize PostgreSQL schema registry tables"""
        
        try:
            with self.postgres_engine.connect() as conn:
                # Create schema registry table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS schema_registry (
                        schema_id VARCHAR(255) PRIMARY KEY,
                        schema_name VARCHAR(255) NOT NULL,
                        schema_type VARCHAR(50) NOT NULL,
                        version VARCHAR(50) NOT NULL,
                        description TEXT,
                        json_schema JSONB NOT NULL,
                        sample_data JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by VARCHAR(255) NOT NULL,
                        status VARCHAR(50) DEFAULT 'draft',
                        compatible_versions JSONB DEFAULT '[]',
                        breaking_changes JSONB DEFAULT '[]',
                        dependencies JSONB DEFAULT '[]',
                        validation_level VARCHAR(50) DEFAULT 'moderate',
                        custom_validators JSONB,
                        schema_hash VARCHAR(64),
                        UNIQUE(schema_name, version)
                    )
                """))
                
                # Create schema validation history table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS schema_validation_history (
                        id SERIAL PRIMARY KEY,
                        schema_id VARCHAR(255) REFERENCES schema_registry(schema_id),
                        validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        data_source VARCHAR(255),
                        validation_result JSONB,
                        passed_validations INTEGER DEFAULT 0,
                        failed_validations INTEGER DEFAULT 0,
                        validation_errors JSONB
                    )
                """))
                
                # Create indexes
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_schema_type ON schema_registry(schema_type)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_schema_status ON schema_registry(status)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_validation_timestamp ON schema_validation_history(validation_timestamp)"))
                
                conn.commit()
                
            logger.info("✅ PostgreSQL schema registry initialized")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize PostgreSQL registry: {e}")

    def _init_mongodb_registry(self):
        """Initialize MongoDB schema registry collections"""
        
        try:
            db = self.mongo_client.schema_registry
            
            # Create indexes
            db.schemas.create_index("schema_id", unique=True)
            db.schemas.create_index([("schema_name", 1), ("version", 1)], unique=True)
            db.schemas.create_index("schema_type")
            db.schemas.create_index("status")
            
            db.validation_history.create_index("validation_timestamp")
            db.validation_history.create_index("schema_id")
            
            logger.info("✅ MongoDB schema registry initialized")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize MongoDB registry: {e}")

    def _load_default_schemas(self):
        """Load default schemas for common data structures"""
        
        default_schemas = {
            # Kafka message schemas
            "products_stream_v1.0.0": {
                "schema_name": "products_stream",
                "schema_type": SchemaType.KAFKA,
                "version": "1.0.0",
                "description": "Product data streaming schema",
                "json_schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "required": ["product_id", "timestamp"],
                    "properties": {
                        "product_id": {"type": "string", "pattern": "^[a-zA-Z0-9_-]+$"},
                        "product_category_name": {"type": "string"},
                        "product_name_length": {"type": "integer", "minimum": 0},
                        "product_description_length": {"type": "integer", "minimum": 0},
                        "product_photos_qty": {"type": "integer", "minimum": 0},
                        "product_weight_g": {"type": "number", "minimum": 0},
                        "product_length_cm": {"type": "number", "minimum": 0},
                        "product_height_cm": {"type": "number", "minimum": 0},
                        "product_width_cm": {"type": "number", "minimum": 0},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "source": {"type": "string", "enum": ["olist", "synthetic", "api"]}
                    },
                    "additionalProperties": False
                },
                "validation_level": ValidationLevel.STRICT,
                "created_by": "system"
            },
            
            "customers_stream_v1.0.0": {
                "schema_name": "customers_stream",
                "schema_type": SchemaType.KAFKA,
                "version": "1.0.0",
                "description": "Customer data streaming schema",
                "json_schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "required": ["customer_id", "timestamp"],
                    "properties": {
                        "customer_id": {"type": "string", "pattern": "^[a-zA-Z0-9_-]+$"},
                        "customer_unique_id": {"type": "string"},
                        "customer_zip_code_prefix": {"type": "string", "pattern": "^[0-9]{5}$"},
                        "customer_city": {"type": "string"},
                        "customer_state": {"type": "string", "minLength": 2, "maxLength": 2},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "source": {"type": "string", "enum": ["olist", "synthetic", "api"]}
                    },
                    "additionalProperties": False
                },
                "validation_level": ValidationLevel.STRICT,
                "created_by": "system"
            },
            
            "orders_stream_v1.0.0": {
                "schema_name": "orders_stream",
                "schema_type": SchemaType.KAFKA,
                "version": "1.0.0",
                "description": "Order data streaming schema",
                "json_schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "required": ["order_id", "customer_id", "order_status", "timestamp"],
                    "properties": {
                        "order_id": {"type": "string", "pattern": "^[a-zA-Z0-9_-]+$"},
                        "customer_id": {"type": "string", "pattern": "^[a-zA-Z0-9_-]+$"},
                        "order_status": {
                            "type": "string",
                            "enum": ["delivered", "shipped", "processing", "invoiced", "canceled", "unavailable", "approved"]
                        },
                        "order_purchase_timestamp": {"type": "string", "format": "date-time"},
                        "order_approved_at": {"type": ["string", "null"], "format": "date-time"},
                        "order_delivered_carrier_date": {"type": ["string", "null"], "format": "date-time"},
                        "order_delivered_customer_date": {"type": ["string", "null"], "format": "date-time"},
                        "order_estimated_delivery_date": {"type": ["string", "null"], "format": "date-time"},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "source": {"type": "string", "enum": ["olist", "synthetic", "api"]}
                    },
                    "additionalProperties": False
                },
                "validation_level": ValidationLevel.STRICT,
                "created_by": "system"
            },
            
            # API schemas
            "api_customer_create_v1.0.0": {
                "schema_name": "api_customer_create",
                "schema_type": SchemaType.API,
                "version": "1.0.0",
                "description": "API schema for creating customers",
                "json_schema": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "required": ["customer_id", "email", "first_name", "last_name", "country"],
                    "properties": {
                        "customer_id": {"type": "string", "maxLength": 50},
                        "email": {"type": "string", "format": "email", "maxLength": 255},
                        "first_name": {"type": "string", "maxLength": 100},
                        "last_name": {"type": "string", "maxLength": 100},
                        "phone": {"type": ["string", "null"], "maxLength": 20},
                        "country": {"type": "string", "maxLength": 100},
                        "state": {"type": ["string", "null"], "maxLength": 100},
                        "city": {"type": ["string", "null"], "maxLength": 100},
                        "marketing_consent": {"type": "boolean", "default": False}
                    },
                    "additionalProperties": False
                },
                "validation_level": ValidationLevel.STRICT,
                "created_by": "system"
            }
        }
        
        # Register default schemas
        for schema_id, schema_data in default_schemas.items():
            try:
                schema_def = SchemaDefinition(
                    schema_id=schema_id,
                    **schema_data
                )
                self.register_schema(schema_def, auto_activate=True)
            except Exception as e:
                logger.warning(f"⚠️ Failed to load default schema {schema_id}: {e}")

    def register_schema(self, schema_def: SchemaDefinition, auto_activate: bool = False) -> bool:
        """Register a new schema version"""
        
        try:
            # Calculate schema hash for change detection
            schema_hash = self._calculate_schema_hash(schema_def.json_schema)
            
            # Check for compatibility with existing versions
            compatibility_check = self._check_compatibility(schema_def)
            
            # Store schema
            if self.registry_backend == "postgres" and self.postgres_engine:
                self._store_schema_postgres(schema_def, schema_hash)
            elif self.registry_backend == "mongodb" and self.mongo_client:
                self._store_schema_mongodb(schema_def, schema_hash)
            else:
                # Fallback to file storage
                self._store_schema_file(schema_def, schema_hash)
            
            # Auto-activate if requested
            if auto_activate:
                self.activate_schema(schema_def.schema_id)
            
            # Clear cache
            self._clear_cache(schema_def.schema_name)
            
            logger.info(f"✅ Schema registered: {schema_def.schema_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to register schema {schema_def.schema_id}: {e}")
            return False

    def _calculate_schema_hash(self, json_schema: Dict[str, Any]) -> str:
        """Calculate hash of schema for change detection"""
        schema_str = json.dumps(json_schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()

    def _check_compatibility(self, schema_def: SchemaDefinition) -> Dict[str, Any]:
        """Check schema compatibility with existing versions"""
        
        compatibility_result = {
            'is_compatible': True,
            'breaking_changes': [],
            'warnings': []
        }
        
        try:
            # Get existing versions
            existing_versions = self.list_schema_versions(schema_def.schema_name)
            
            if not existing_versions:
                return compatibility_result
            
            # Get latest active version
            latest_version = self.get_schema(schema_def.schema_name, status=SchemaStatus.ACTIVE)
            
            if not latest_version:
                return compatibility_result
            
            # Compare schemas
            breaking_changes = self._detect_breaking_changes(
                latest_version.json_schema,
                schema_def.json_schema
            )
            
            if breaking_changes:
                compatibility_result['is_compatible'] = False
                compatibility_result['breaking_changes'] = breaking_changes
            
        except Exception as e:
            logger.warning(f"⚠️ Compatibility check failed: {e}")
            compatibility_result['warnings'].append(f"Compatibility check failed: {e}")
        
        return compatibility_result

    def _detect_breaking_changes(self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> List[str]:
        """Detect breaking changes between schema versions"""
        
        breaking_changes = []
        
        try:
            old_props = old_schema.get('properties', {})
            new_props = new_schema.get('properties', {})
            
            old_required = set(old_schema.get('required', []))
            new_required = set(new_schema.get('required', []))
            
            # Check for removed required fields
            removed_required = old_required - new_required
            if removed_required:
                breaking_changes.append(f"Removed required fields: {list(removed_required)}")
            
            # Check for removed properties
            removed_props = set(old_props.keys()) - set(new_props.keys())
            if removed_props:
                breaking_changes.append(f"Removed properties: {list(removed_props)}")
            
            # Check for type changes
            for prop_name in old_props.keys():
                if prop_name in new_props:
                    old_type = old_props[prop_name].get('type')
                    new_type = new_props[prop_name].get('type')
                    
                    if old_type != new_type:
                        breaking_changes.append(f"Type changed for {prop_name}: {old_type} -> {new_type}")
            
            # Check for stricter constraints
            for prop_name in old_props.keys():
                if prop_name in new_props:
                    old_prop = old_props[prop_name]
                    new_prop = new_props[prop_name]
                    
                    # Check string length constraints
                    if 'maxLength' in new_prop and 'maxLength' in old_prop:
                        if new_prop['maxLength'] < old_prop['maxLength']:
                            breaking_changes.append(f"Stricter maxLength for {prop_name}")
                    
                    # Check numeric constraints
                    if 'minimum' in new_prop and 'minimum' in old_prop:
                        if new_prop['minimum'] > old_prop['minimum']:
                            breaking_changes.append(f"Higher minimum value for {prop_name}")
        
        except Exception as e:
            logger.warning(f"⚠️ Breaking change detection error: {e}")
        
        return breaking_changes

    def _store_schema_postgres(self, schema_def: SchemaDefinition, schema_hash: str):
        """Store schema in PostgreSQL"""
        
        with self.postgres_engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO schema_registry (
                    schema_id, schema_name, schema_type, version, description,
                    json_schema, sample_data, created_at, created_by, status,
                    compatible_versions, breaking_changes, dependencies,
                    validation_level, custom_validators, schema_hash
                ) VALUES (
                    :schema_id, :schema_name, :schema_type, :version, :description,
                    :json_schema, :sample_data, :created_at, :created_by, :status,
                    :compatible_versions, :breaking_changes, :dependencies,
                    :validation_level, :custom_validators, :schema_hash
                )
                ON CONFLICT (schema_id) DO UPDATE SET
                    json_schema = EXCLUDED.json_schema,
                    sample_data = EXCLUDED.sample_data,
                    status = EXCLUDED.status,
                    schema_hash = EXCLUDED.schema_hash
            """), {
                'schema_id': schema_def.schema_id,
                'schema_name': schema_def.schema_name,
                'schema_type': schema_def.schema_type.value,
                'version': schema_def.version,
                'description': schema_def.description,
                'json_schema': json.dumps(schema_def.json_schema),
                'sample_data': json.dumps(schema_def.sample_data) if schema_def.sample_data else None,
                'created_at': schema_def.created_at,
                'created_by': schema_def.created_by,
                'status': schema_def.status.value,
                'compatible_versions': json.dumps(schema_def.compatible_versions),
                'breaking_changes': json.dumps(schema_def.breaking_changes),
                'dependencies': json.dumps(schema_def.dependencies),
                'validation_level': schema_def.validation_level.value,
                'custom_validators': json.dumps(schema_def.custom_validators) if schema_def.custom_validators else None,
                'schema_hash': schema_hash
            })
            conn.commit()

    def _store_schema_mongodb(self, schema_def: SchemaDefinition, schema_hash: str):
        """Store schema in MongoDB"""
        
        db = self.mongo_client.schema_registry
        
        schema_doc = {
            'schema_id': schema_def.schema_id,
            'schema_name': schema_def.schema_name,
            'schema_type': schema_def.schema_type.value,
            'version': schema_def.version,
            'description': schema_def.description,
            'json_schema': schema_def.json_schema,
            'sample_data': schema_def.sample_data,
            'created_at': schema_def.created_at,
            'created_by': schema_def.created_by,
            'status': schema_def.status.value,
            'compatible_versions': schema_def.compatible_versions,
            'breaking_changes': schema_def.breaking_changes,
            'dependencies': schema_def.dependencies,
            'validation_level': schema_def.validation_level.value,
            'custom_validators': schema_def.custom_validators,
            'schema_hash': schema_hash
        }
        
        db.schemas.replace_one(
            {'schema_id': schema_def.schema_id},
            schema_doc,
            upsert=True
        )

    def _store_schema_file(self, schema_def: SchemaDefinition, schema_hash: str):
        """Store schema as file (fallback method)"""
        
        schema_file = self.schema_storage_path / f"{schema_def.schema_id}.json"
        
        schema_data = schema_def.dict()
        schema_data['schema_hash'] = schema_hash
        
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f, indent=2, default=str)

    def get_schema(self, schema_name: str, version: Optional[str] = None, 
                  status: Optional[SchemaStatus] = None) -> Optional[SchemaDefinition]:
        """Get schema by name and optional version/status"""
        
        # Check cache first
        cache_key = f"{schema_name}_{version or 'latest'}_{status.value if status else 'any'}"
        
        if cache_key in self.schema_cache:
            return self.schema_cache[cache_key]
        
        try:
            schema_data = None
            
            if self.registry_backend == "postgres" and self.postgres_engine:
                schema_data = self._get_schema_postgres(schema_name, version, status)
            elif self.registry_backend == "mongodb" and self.mongo_client:
                schema_data = self._get_schema_mongodb(schema_name, version, status)
            else:
                schema_data = self._get_schema_file(schema_name, version, status)
            
            if schema_data:
                schema_def = SchemaDefinition(**schema_data)
                self.schema_cache[cache_key] = schema_def
                return schema_def
            
        except Exception as e:
            logger.error(f"❌ Failed to get schema {schema_name}: {e}")
        
        return None

    def _get_schema_postgres(self, schema_name: str, version: Optional[str], 
                           status: Optional[SchemaStatus]) -> Optional[Dict[str, Any]]:
        """Get schema from PostgreSQL"""
        
        query = "SELECT * FROM schema_registry WHERE schema_name = :schema_name"
        params = {'schema_name': schema_name}
        
        if version:
            query += " AND version = :version"
            params['version'] = version
        
        if status:
            query += " AND status = :status"
            params['status'] = status.value
        
        query += " ORDER BY created_at DESC LIMIT 1"
        
        with self.postgres_engine.connect() as conn:
            result = conn.execute(text(query), params).fetchone()
            
            if result:
                return {
                    'schema_id': result.schema_id,
                    'schema_name': result.schema_name,
                    'schema_type': SchemaType(result.schema_type),
                    'version': result.version,
                    'description': result.description,
                    'json_schema': json.loads(result.json_schema),
                    'sample_data': json.loads(result.sample_data) if result.sample_data else None,
                    'created_at': result.created_at,
                    'created_by': result.created_by,
                    'status': SchemaStatus(result.status),
                    'compatible_versions': json.loads(result.compatible_versions),
                    'breaking_changes': json.loads(result.breaking_changes),
                    'dependencies': json.loads(result.dependencies),
                    'validation_level': ValidationLevel(result.validation_level),
                    'custom_validators': json.loads(result.custom_validators) if result.custom_validators else None
                }
        
        return None

    def validate_data(self, data: Union[Dict[str, Any], pd.DataFrame], 
                     schema_name: str, version: Optional[str] = None) -> Dict[str, Any]:
        """Validate data against schema"""
        
        validation_result = {
            'is_valid': False,
            'schema_name': schema_name,
            'schema_version': version,
            'validation_timestamp': datetime.now(),
            'errors': [],
            'warnings': [],
            'passed_validations': 0,
            'failed_validations': 0,
            'validation_details': {}
        }
        
        try:
            # Get schema
            schema_def = self.get_schema(schema_name, version, SchemaStatus.ACTIVE)
            
            if not schema_def:
                validation_result['errors'].append(f"Schema not found: {schema_name}")
                return validation_result
            
            validation_result['schema_version'] = schema_def.version
            
            # Convert DataFrame to dict if needed
            if isinstance(data, pd.DataFrame):
                data_dict = data.to_dict('records')
                
                # Validate each record
                all_valid = True
                total_passed = 0
                total_failed = 0
                
                for i, record in enumerate(data_dict):
                    record_result = self._validate_single_record(record, schema_def)
                    
                    if not record_result['is_valid']:
                        all_valid = False
                        validation_result['errors'].extend([
                            f"Record {i}: {error}" for error in record_result['errors']
                        ])
                    
                    total_passed += record_result['passed_validations']
                    total_failed += record_result['failed_validations']
                
                validation_result['is_valid'] = all_valid
                validation_result['passed_validations'] = total_passed
                validation_result['failed_validations'] = total_failed
                
            else:
                # Validate single record
                record_result = self._validate_single_record(data, schema_def)
                validation_result.update(record_result)
            
            # Store validation history
            self._store_validation_history(validation_result, schema_def.schema_id)
            
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
            validation_result['errors'].append(f"Validation error: {e}")
        
        return validation_result

    def _validate_single_record(self, data: Dict[str, Any], schema_def: SchemaDefinition) -> Dict[str, Any]:
        """Validate a single data record"""
        
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'passed_validations': 0,
            'failed_validations': 0
        }
        
        try:
            # JSON Schema validation
            validator = Draft7Validator(schema_def.json_schema)
            errors = list(validator.iter_errors(data))
            
            if errors:
                result['is_valid'] = False
                result['failed_validations'] += len(errors)
                
                for error in errors:
                    if schema_def.validation_level == ValidationLevel.STRICT:
                        result['errors'].append(f"Schema validation: {error.message}")
                    elif schema_def.validation_level == ValidationLevel.MODERATE:
                        if error.validator in ['required', 'type']:
                            result['errors'].append(f"Schema validation: {error.message}")
                        else:
                            result['warnings'].append(f"Schema validation: {error.message}")
                            result['is_valid'] = True  # Don't fail for warnings
                    else:  # LENIENT
                        if error.validator in ['required', 'type']:
                            result['warnings'].append(f"Schema validation: {error.message}")
                            result['is_valid'] = True  # Don't fail in lenient mode
            else:
                result['passed_validations'] += 1
            
            # Custom validations
            if schema_def.custom_validators:
                custom_result = self._run_custom_validations(data, schema_def.custom_validators)
                
                if not custom_result['is_valid']:
                    result['is_valid'] = False
                    result['errors'].extend(custom_result['errors'])
                
                result['passed_validations'] += custom_result['passed_validations']
                result['failed_validations'] += custom_result['failed_validations']
        
        except Exception as e:
            result['is_valid'] = False
            result['errors'].append(f"Validation exception: {e}")
            result['failed_validations'] += 1
        
        return result

    def _run_custom_validations(self, data: Dict[str, Any], custom_validators: Dict[str, Any]) -> Dict[str, Any]:
        """Run custom validation rules"""
        
        result = {
            'is_valid': True,
            'errors': [],
            'passed_validations': 0,
            'failed_validations': 0
        }
        
        for validator_name, validator_config in custom_validators.items():
            try:
                if validator_config.get('type') == 'python_expression':
                    expression = validator_config.get('expression')
                    if expression:
                        # Safely evaluate expression
                        if eval(expression, {"data": data, "__builtins__": {}}):
                            result['passed_validations'] += 1
                        else:
                            result['is_valid'] = False
                            result['failed_validations'] += 1
                            result['errors'].append(f"Custom validation failed: {validator_name}")
                
                elif validator_config.get('type') == 'range_check':
                    field = validator_config.get('field')
                    min_val = validator_config.get('min')
                    max_val = validator_config.get('max')
                    
                    if field in data:
                        value = data[field]
                        if (min_val is not None and value < min_val) or (max_val is not None and value > max_val):
                            result['is_valid'] = False
                            result['failed_validations'] += 1
                            result['errors'].append(f"Range validation failed for {field}: {value}")
                        else:
                            result['passed_validations'] += 1
                
            except Exception as e:
                result['is_valid'] = False
                result['failed_validations'] += 1
                result['errors'].append(f"Custom validator {validator_name} failed: {e}")
        
        return result

    def _store_validation_history(self, validation_result: Dict[str, Any], schema_id: str):
        """Store validation history"""
        
        try:
            if self.registry_backend == "postgres" and self.postgres_engine:
                with self.postgres_engine.connect() as conn:
                    conn.execute(text("""
                        INSERT INTO schema_validation_history (
                            schema_id, validation_timestamp, validation_result,
                            passed_validations, failed_validations, validation_errors
                        ) VALUES (
                            :schema_id, :validation_timestamp, :validation_result,
                            :passed_validations, :failed_validations, :validation_errors
                        )
                    """), {
                        'schema_id': schema_id,
                        'validation_timestamp': validation_result['validation_timestamp'],
                        'validation_result': json.dumps({'is_valid': validation_result['is_valid']}),
                        'passed_validations': validation_result['passed_validations'],
                        'failed_validations': validation_result['failed_validations'],
                        'validation_errors': json.dumps(validation_result['errors'])
                    })
                    conn.commit()
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to store validation history: {e}")

    def activate_schema(self, schema_id: str) -> bool:
        """Activate a schema version"""
        return self._update_schema_status(schema_id, SchemaStatus.ACTIVE)

    def deprecate_schema(self, schema_id: str) -> bool:
        """Deprecate a schema version"""
        return self._update_schema_status(schema_id, SchemaStatus.DEPRECATED)

    def _update_schema_status(self, schema_id: str, status: SchemaStatus) -> bool:
        """Update schema status"""
        
        try:
            if self.registry_backend == "postgres" and self.postgres_engine:
                with self.postgres_engine.connect() as conn:
                    result = conn.execute(text("""
                        UPDATE schema_registry 
                        SET status = :status 
                        WHERE schema_id = :schema_id
                    """), {
                        'status': status.value,
                        'schema_id': schema_id
                    })
                    conn.commit()
                    
                    if result.rowcount > 0:
                        self._clear_cache()
                        logger.info(f"✅ Schema {schema_id} status updated to {status.value}")
                        return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update schema status: {e}")
        
        return False

    def list_schema_versions(self, schema_name: str) -> List[str]:
        """List all versions of a schema"""
        
        try:
            if self.registry_backend == "postgres" and self.postgres_engine:
                with self.postgres_engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT version FROM schema_registry 
                        WHERE schema_name = :schema_name 
                        ORDER BY created_at DESC
                    """), {'schema_name': schema_name})
                    
                    return [row.version for row in result]
            
        except Exception as e:
            logger.error(f"❌ Failed to list schema versions: {e}")
        
        return []

    def get_validation_statistics(self, schema_name: str, 
                                days: int = 7) -> Dict[str, Any]:
        """Get validation statistics for a schema"""
        
        try:
            if self.registry_backend == "postgres" and self.postgres_engine:
                with self.postgres_engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT 
                            COUNT(*) as total_validations,
                            SUM(passed_validations) as total_passed,
                            SUM(failed_validations) as total_failed,
                            AVG(CASE WHEN failed_validations = 0 THEN 1.0 ELSE 0.0 END) as success_rate
                        FROM schema_validation_history vh
                        JOIN schema_registry sr ON vh.schema_id = sr.schema_id
                        WHERE sr.schema_name = :schema_name
                        AND vh.validation_timestamp >= NOW() - INTERVAL ':days days'
                    """), {
                        'schema_name': schema_name,
                        'days': days
                    }).fetchone()
                    
                    if result:
                        return {
                            'schema_name': schema_name,
                            'period_days': days,
                            'total_validations': result.total_validations or 0,
                            'total_passed': result.total_passed or 0,
                            'total_failed': result.total_failed or 0,
                            'success_rate': float(result.success_rate or 0),
                            'generated_at': datetime.now()
                        }
            
        except Exception as e:
            logger.error(f"❌ Failed to get validation statistics: {e}")
        
        return {}

    def _clear_cache(self, schema_name: Optional[str] = None):
        """Clear schema cache"""
        
        if schema_name:
            keys_to_remove = [k for k in self.schema_cache.keys() if schema_name in k]
            for key in keys_to_remove:
                del self.schema_cache[key]
        else:
            self.schema_cache.clear()
        
        self.validators_cache.clear()

# Utility functions for easy integration
def create_schema_registry(registry_config: Dict[str, Any]) -> SchemaRegistry:
    """Factory function to create schema registry"""
    return SchemaRegistry(**registry_config)

def validate_kafka_message(message: Dict[str, Any], topic: str, 
                         registry: SchemaRegistry) -> Dict[str, Any]:
    """Validate Kafka message against registered schema"""
    
    # Map topic to schema name
    topic_schema_mapping = {
        'products_stream': 'products_stream',
        'customers_stream': 'customers_stream',
        'orders_stream': 'orders_stream'
    }
    
    schema_name = topic_schema_mapping.get(topic)
    
    if not schema_name:
        return {
            'is_valid': False,
            'errors': [f'No schema mapping found for topic: {topic}']
        }
    
    return registry.validate_data(message, schema_name)

def validate_api_request(request_data: Dict[str, Any], endpoint: str,
                        registry: SchemaRegistry) -> Dict[str, Any]:
    """Validate API request against registered schema"""
    
    # Map endpoint to schema name
    endpoint_schema_mapping = {
        '/api/v1/customers': 'api_customer_create',
        '/api/v1/products': 'api_product_create',
        '/api/v1/orders': 'api_order_create'
    }
    
    schema_name = endpoint_schema_mapping.get(endpoint)
    
    if not schema_name:
        return {
            'is_valid': False,
            'errors': [f'No schema mapping found for endpoint: {endpoint}']
        }
    
    return registry.validate_data(request_data, schema_name)