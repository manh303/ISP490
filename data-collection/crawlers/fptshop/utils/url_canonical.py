"""URL canonicalization utilities for Lazada crawling."""

import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


def canonicalize_product_url(url: str) -> str:
    """
    Canonicalize Lazada product URL by removing unnecessary parameters.

    Args:
        url: Original product URL

    Returns:
        Canonicalized URL
    """
    parsed = urlparse(url)

    # Keep only essential parameters
    essential_params = ['pid', 'sku', 'itemId']
    query_params = parse_qs(parsed.query)

    # Filter to keep only essential parameters
    filtered_params = {
        k: v for k, v in query_params.items()
        if k in essential_params
    }

    # Rebuild URL
    new_query = urlencode(filtered_params, doseq=True)
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        ''  # Remove fragment
    ))


def extract_product_id(url: str) -> str:
    """Extract product ID from Lazada URL."""
    # Try to extract from path first
    path_match = re.search(r'/products/([^/]+)', url)
    if path_match:
        return path_match.group(1)

    # Try to extract from query parameters
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)

    for param in ['pid', 'itemId', 'sku']:
        if param in query_params:
            return query_params[param][0]

    return None