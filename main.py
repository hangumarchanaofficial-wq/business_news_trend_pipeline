"""
Entry point — runs the complete Business News Trend Pipeline on EC2.

Usage:
    python main.py                  # full run (scrape + ETL + S3 sync)
    python main.py --skip-scraping  # ETL only (use existing raw data from S3)
"""

import sys
import os

from src.orchestration.pipeline_runner import run_pipeline


def main():
    skip = "--skip-scraping" in sys.argv
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = ""
    run_pipeline(skip_scraping=skip)


if __name__ == "__main__":
    main()
