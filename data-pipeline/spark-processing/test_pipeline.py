#!/usr/bin/env python3
"""
Quick test to run pipeline in Spark container
"""

import subprocess
import sys

def run_pipeline():
    """Run the standardization pipeline"""
    try:
        # Copy script to container
        print(" Copying script to container...")
        result = subprocess.run([
            'docker', 'cp',
            'standardization_pipeline_local.py',
            'spark-master:/app/scripts/'
        ], capture_output=True, text=True)

        if result.returncode != 0:
            print(f" Failed to copy script: {result.stderr}")
            return False

        # Run pipeline
        print(" Running pipeline...")
        result = subprocess.run([
            'docker', 'exec', 'spark-master',
            'python', '/app/scripts/standardization_pipeline_local.py'
        ], capture_output=True, text=True)

        print(" Pipeline Output:")
        print(result.stdout)

        if result.stderr:
            print(" Pipeline Errors:")
            print(result.stderr)

        if result.returncode == 0:
            print(" Pipeline completed successfully!")

            # Show results
            print("\n Generated files:")
            subprocess.run([
                'docker', 'exec', 'spark-master',
                'ls', '-la', '/app/processed_data/'
            ])

            return True
        else:
            print(" Pipeline failed!")
            return False

    except Exception as e:
        print(f" Error running pipeline: {e}")
        return False

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)