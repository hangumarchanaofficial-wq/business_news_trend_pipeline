# Sri Lankan Business News Trend Pipeline

An end-to-end ETL pipeline that scrapes business news from 6 Sri Lankan
sources, processes them with PySpark, and loads enriched data into a
star-schema SQLite warehouse — deployed on **AWS EC2** with **S3** as
the data lake.

## Architecture

## Pipeline Stages

| Stage | Module | Description |
|-------|--------|-------------|
| 1. Ingestion | `src/ingestion/` | Playwright scrapes 6 sources → JSON → S3 |
| 2. Staging | `src/staging/` | JSON → Spark DF, validate schema, deduplicate |
| 3. Transformation | `src/transformation/` | Clean text, sentiment (VADER), keywords (RAKE), topic classification |
| 4. Aggregation | `src/aggregation/` | Topic-daily, keyword-daily, daily summary |
| 5. Loading | `src/loading/` | SQLite star schema with 3 dims + 4 facts |

## AWS Services

- **EC2** (t2.medium, Ubuntu 22.04): compute for scraping + Spark
- **S3**: durable data lake for raw articles + warehouse backup
- **IAM**: instance role with S3 read/write permissions

## Quick Start (EC2)

```bash
# 1. Launch EC2 t2.medium (Ubuntu 22.04), attach IAM role with S3 access
# 2. SSH into the instance
ssh -i your-key.pem ubuntu@<ec2-ip>

# 3. Clone the repo
git clone https://github.com/hangumarchanaofficial-wq/business_news_trend_pipeline.git
cd business_news_trend_pipeline

# 4. Run setup
chmod +x setup_ec2.sh
./setup_ec2.sh

# 5. Configure AWS
aws configure   # enter Access Key, Secret Key, region (ap-south-1)

# 6. Activate venv and run
source venv/bin/activate
python main.py                  # full run: scrape + ETL
python main.py --skip-scraping  # ETL only (use existing S3 data)