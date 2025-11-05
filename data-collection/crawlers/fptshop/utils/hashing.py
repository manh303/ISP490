"""Hashing utilities for deduplication."""

import hashlib
import json
from typing import Dict, Any


def hash_product(product_data: Dict[str, Any]) -> str:
    """
    Generate a hash for product data to detect duplicates.

    Args:
        product_data: Product information dictionary

    Returns:
        SHA256 hash string
    """
    # Extract key fields for hashing
    key_fields = {
        'id': product_data.get('id'),
        'name': product_data.get('name'),
        'url': product_data.get('url'),
        'source': product_data.get('source')
    }

    # Remove None values
    key_fields = {k: v for k, v in key_fields.items() if v is not None}

    # Sort and serialize
    serialized = json.dumps(key_fields, sort_keys=True, ensure_ascii=False)

    # Generate hash
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()


def hash_url(url: str) -> str:
    """Generate SHA256 hash for URL."""
    return hashlib.sha256(url.encode('utf-8')).hexdigest()