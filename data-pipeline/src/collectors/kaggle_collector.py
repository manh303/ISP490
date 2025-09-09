import os
import pandas as pd
import kaggle
from pathlib import Path
from utils.config import Config
from loguru import logger

class KaggleCollector:
    def __init__(self):
        self.config = Config()
        
        # Check if credentials exist in app directory (Docker container root)
        kaggle_json_path = Path("/app/kaggle.json")
        
        try:
            # Create .kaggle directory in user home if it doesn't exist
            kaggle_dir = Path.home() / '.kaggle'
            kaggle_dir.mkdir(exist_ok=True)
            
            # Copy kaggle.json to user home .kaggle directory
            target_path = kaggle_dir / 'kaggle.json'
            if kaggle_json_path.exists():
                import shutil
                shutil.copy2(kaggle_json_path, target_path)
                # Set file permissions
                target_path.chmod(0o600)
                logger.info(f"Copied kaggle.json to {target_path}")
            else:
                raise FileNotFoundError(f"Kaggle credentials not found at {kaggle_json_path}")
            
            # Verify Kaggle authentication
            kaggle.api.authenticate()
            logger.info("✅ Successfully authenticated with Kaggle API")
        except Exception as e:
            logger.error(f"❌ Failed to authenticate with Kaggle API: {e}")
            raise
        
    def download_dataset(self, dataset_name: str, download_path: str = None):
        """
        Download dataset từ Kaggle
        Args:
            dataset_name: Tên dataset (e.g., 'carrie1/ecommerce-data')
            download_path: Path để save data
        """
        if not download_path:
            # Save to mounted volume directory that maps to local system
            download_path = Path("C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/raw/kaggle_datasets")
        
        download_path = Path(download_path)
        
        try:
            # Create directory without changing permissions
            download_path.mkdir(parents=True, exist_ok=True)
            
            # Create dataset-specific subdirectory
            dataset_dir = download_path / dataset_name.split('/')[-1]
            dataset_dir.mkdir(exist_ok=True)
            
            logger.info(f"Downloading dataset: {dataset_name}")
            logger.info(f"Saving to: {dataset_dir}")
            
            kaggle.api.dataset_download_files(
                dataset_name, 
                path=str(dataset_dir),  # Save to dataset-specific subdirectory
                unzip=True
            )
            logger.success(f"Dataset downloaded to: {dataset_dir}")
            return dataset_dir
        except Exception as e:
            logger.error(f"Error downloading dataset: {e}")
            return None
    
    def list_files_in_dataset(self, download_path: str):
        """List all files trong downloaded dataset"""
        path = Path(download_path)
        if path.exists():
            files = list(path.glob("*"))
            logger.info(f"Files in dataset: {[f.name for f in files]}")
            return files
        return []
    
    def load_csv_file(self, file_path: str, **kwargs):
        """Load CSV file với pandas"""
        try:
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"Loaded CSV: {file_path}, Shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV {file_path}: {e}")
            return None