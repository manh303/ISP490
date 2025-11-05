"""Manifest management for crawling runs."""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List


class CrawlManifest:
    """Manages manifest files for crawling runs."""

    def __init__(self, run_id: str, manifest_dir: str = "manifests"):
        self.run_id = run_id
        self.manifest_dir = Path(manifest_dir)
        self.manifest_dir.mkdir(exist_ok=True)
        self.manifest_file = self.manifest_dir / f"{run_id}.json"

        self.manifest = {
            "run_id": run_id,
            "created_at": datetime.now().isoformat(),
            "status": "started",
            "stats": {
                "urls_crawled": 0,
                "products_found": 0,
                "errors": 0
            },
            "urls": [],
            "errors": []
        }

    def update_stats(self, urls_crawled: int = 0, products_found: int = 0, errors: int = 0):
        """Update crawling statistics."""
        self.manifest["stats"]["urls_crawled"] += urls_crawled
        self.manifest["stats"]["products_found"] += products_found
        self.manifest["stats"]["errors"] += errors
        self.save()

    def add_url(self, url: str, status: str, products_count: int = 0):
        """Add crawled URL to manifest."""
        self.manifest["urls"].append({
            "url": url,
            "status": status,
            "products_count": products_count,
            "crawled_at": datetime.now().isoformat()
        })
        self.save()

    def add_error(self, url: str, error: str):
        """Add error to manifest."""
        self.manifest["errors"].append({
            "url": url,
            "error": error,
            "timestamp": datetime.now().isoformat()
        })
        self.save()

    def set_status(self, status: str):
        """Update run status."""
        self.manifest["status"] = status
        if status == "completed":
            self.manifest["completed_at"] = datetime.now().isoformat()
        self.save()

    def save(self):
        """Save manifest to file."""
        with open(self.manifest_file, 'w', encoding='utf-8') as f:
            json.dump(self.manifest, f, indent=2, ensure_ascii=False)

    def load(self) -> Dict[str, Any]:
        """Load manifest from file."""
        if self.manifest_file.exists():
            with open(self.manifest_file, 'r', encoding='utf-8') as f:
                self.manifest = json.load(f)
        return self.manifest