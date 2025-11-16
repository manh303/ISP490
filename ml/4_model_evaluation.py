# -*- coding: utf-8 -*-
"""
Step 4: Model Evaluation & Selection
Compare models and select best performers
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
import numpy as np
import json
from pathlib import Path
from utils.logger import get_logger
import yaml

logger = get_logger("model_evaluation")

# Load config
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


class ModelEvaluator:
    """Model evaluation and selection"""
    
    def __init__(self, config: dict):
        self.config = config
        self.best_models = {}
    
    def load_results(self, results_file: Path) -> dict:
        """Load results from JSON"""
        with open(results_file, 'r') as f:
            return json.load(f)
    
    def rank_models(self, results: dict, task: str = 'demand') -> pd.DataFrame:
        """Rank models by metrics"""
        logger.info(f"\nRanking {task} models...")
        
        # Convert to DataFrame
        df = pd.DataFrame(results).T
        df.index.name = 'model'
        df = df.reset_index()
        
        if task == 'demand':
            # For regression: lower MAE/RMSE is better, higher R2 is better
            df['score'] = (
                (1 - df['mae'] / df['mae'].max()) * 0.25 +
                (1 - df['rmse'] / df['rmse'].max()) * 0.25 +
                (100 - df['mape']) / 100 * 0.25 +
                df['r2'] / df['r2'].max() * 0.25
            )
        else:  # recommendation
            # For clustering: higher silhouette is better, lower davies_bouldin is better
            if 'silhouette' in df.columns:
                df['score'] = (
                    (df['silhouette'] + 1) / 2 * 0.5 +  # Normalize to 0-1
                    (1 - df['davies_bouldin'] / df['davies_bouldin'].max()) * 0.5
                )
            else:
                df['score'] = 0
        
        df = df.sort_values('score', ascending=False)
        
        logger.info("\nModel Rankings:")
        logger.info(df[['model', 'score']].to_string(index=False))
        
        return df
    
    def get_best_model(self, df: pd.DataFrame, top_n: int = 3) -> dict:
        """Get best model(s)"""
        best = {
            'rank_1': df.iloc[0]['model'],
            'rank_2': df.iloc[1]['model'] if len(df) > 1 else None,
            'rank_3': df.iloc[2]['model'] if len(df) > 2 else None,
            'scores': df[['model', 'score']].head(top_n).to_dict('records')
        }
        return best
    
    def create_comparison_report(self, demand_results: dict, rec_results: dict) -> str:
        """Create comparison report"""
        logger.info("\n" + "="*60)
        logger.info("MODEL COMPARISON REPORT")
        logger.info("="*60)
        
        report = "\n" + "="*60 + "\n"
        report += "MODEL COMPARISON REPORT\n"
        report += "="*60 + "\n\n"
        
        # Demand prediction
        report += "## DEMAND PREDICTION MODELS\n\n"
        demand_df = pd.DataFrame(demand_results).T
        demand_df.index.name = 'Model'
        
        report += demand_df.to_string()
        report += "\n\n"
        
        # Best demand model
        best_demand = demand_df.sort_values('r2', ascending=False).iloc[0]
        report += f"**Best Demand Model:** {demand_df.index[0]}\n"
        report += f"  - MAE: {best_demand['mae']:.4f}\n"
        report += f"  - RMSE: {best_demand['rmse']:.4f}\n"
        report += f"  - MAPE: {best_demand['mape']:.4f}%\n"
        report += f"  - R²: {best_demand['r2']:.4f}\n\n"
        
        # Recommendation systems
        report += "## PRODUCT RECOMMENDATION MODELS\n\n"
        rec_df = pd.DataFrame(rec_results).T
        rec_df.index.name = 'Model'
        
        report += rec_df.to_string()
        report += "\n\n"
        
        return report
    
    def save_report(self, report: str, output_dir: Path):
        """Save report to file"""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(output_dir / 'model_comparison_report.txt', 'w') as f:
            f.write(report)
        
        logger.info(f"[OK] Saved report to {output_dir / 'model_comparison_report.txt'}")


def main():
    """Main pipeline"""
    try:
        logger.info("[ML PIPELINE] Step 4: Model Evaluation & Selection")
        
        results_dir = Path(config['output']['metrics_dir'])
        
        # Load results
        demand_results = {}
        rec_results = {}
        
        if (results_dir / 'demand_results.json').exists():
            with open(results_dir / 'demand_results.json', 'r') as f:
                demand_results = json.load(f)
            logger.info("[OK] Loaded demand results")
        
        if (results_dir / 'recommendation_results.json').exists():
            with open(results_dir / 'recommendation_results.json', 'r') as f:
                rec_results = json.load(f)
            logger.info("[OK] Loaded recommendation results")
        
        # Evaluate
        evaluator = ModelEvaluator(config)
        
        # Rank demand models
        logger.info("\n" + "="*60)
        logger.info("DEMAND PREDICTION MODEL RANKING")
        logger.info("="*60)
        
        demand_ranking = evaluator.rank_models(demand_results, task='demand')
        best_demand = evaluator.get_best_model(demand_ranking)
        
        logger.info(f"\n[OK] Best Demand Model: {best_demand['rank_1']}")
        logger.info(f"  Top 3 Models: {', '.join([m['model'] for m in best_demand['scores']])}")
        
        # Rank recommendation models
        logger.info("\n" + "="*60)
        logger.info("PRODUCT RECOMMENDATION MODEL RANKING")
        logger.info("="*60)
        
        rec_ranking = evaluator.rank_models(rec_results, task='recommendation')
        best_rec = evaluator.get_best_model(rec_ranking)
        
        logger.info(f"\n[OK] Best Recommendation Model: {best_rec['rank_1']}")
        logger.info(f"  Top 3 Models: {', '.join([m['model'] for m in best_rec['scores']])}")
        
        # Create report
        report = evaluator.create_comparison_report(demand_results, rec_results)
        
        # Save results
        summary = {
            'best_demand_model': best_demand['rank_1'],
            'best_recommendation_model': best_rec['rank_1'],
            'demand_top_3': [m['model'] for m in best_demand['scores']],
            'recommendation_top_3': [m['model'] for m in best_rec['scores']],
            'timestamp': pd.Timestamp.now().isoformat()
        }
        
        with open(results_dir / 'model_selection_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        evaluator.save_report(report, results_dir)
        
        logger.info("\n" + "="*60)
        logger.info("[OK] MODEL EVALUATION COMPLETED")
        logger.info("="*60)
        
        # Print summary
        logger.info("\nFINAL SELECTION:")
        logger.info(f"  Best Demand Prediction Model: {best_demand['rank_1']}")
        logger.info(f"  Best Recommendation Model: {best_rec['rank_1']}")
        
    except Exception as e:
        logger.error(f"\n[ERROR] FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
