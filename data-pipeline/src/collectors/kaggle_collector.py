import os
import pandas as pd
import kaggle
from pathlib import Path
from src.utils.config import Config
from loguru import logger

class KaggleCollector:
    def __init__(self):
        self.config = Config()
        # Authenticate with Kaggle
        os.environ['KAGGLE_USERNAME'] = self.config.KAGGLE_USERNAME
        os.environ['KAGGLE_KEY'] = self.config.KAGGLE_KEY
        
    def download_dataset(self, dataset_name: str, download_path: str = None):
        """
        Download dataset từ Kaggle
        Args:
            dataset_name: Tên dataset (e.g., 'carrie1/ecommerce-data')
            download_path: Path để save data
        """
        if not download_path:
            download_path = Path(self.config.RAW_DATA_PATH) / "kaggle_datasets"
        
        download_path = Path(download_path)
        download_path.mkdir(parents=True, exist_ok=True)
        
        try:
            logger.info(f"Downloading dataset: {dataset_name}")
            kaggle.api.dataset_download_files(
                dataset_name, 
                path=download_path, 
                unzip=True
            )
            logger.success(f"Dataset downloaded to: {download_path}")
            return download_path
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