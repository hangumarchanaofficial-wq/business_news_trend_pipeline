"""
Entry point — runs the complete Business News Trend Pipeline.

Usage:
    python main.py                  # full run (scrape + ETL)
    python main.py --skip-scraping  # ETL only (use existing raw data)
"""

import sys
from src.orchestration.pipeline_runner import run_pipeline

import os
def main():
    skip = "--skip-scraping" in sys.argv
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    run_pipeline(skip_scraping=skip)

if __name__ == "__main__":
    main()