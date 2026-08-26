# pga-tour-pyspark-pipeline

A PySpark data pipeline for processing PGA Tour golf data, run on a local Docker Spark cluster, feeding a Power BI dashboard.

## Overview

The pipeline creates two data sets, Course Difficulty and Player Season Stats, that the dashboard uses to display metrics on the difficulty of each golf course where a PGA Tour tournament was played, and player stats from these tournaments including year to year trends in player performance.

## Tech Stack

- **PySpark** — data transformation
- **Docker** — local Spark cluster
- **Power BI** — dashboard / reporting layer

## Setup

1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start the Docker Spark cluster:
   ```bash
   chmod -R o+w data/processed   # one-time, see docker/README.md
   cd docker
   docker compose up -d
   ```
   Confirm it's healthy at the Spark Master UI: http://localhost:8080
   (see [docker/README.md](docker/README.md) for full details, including how to stop the cluster)

## Project Structure

```
data/
  raw/         # Raw Kaggle CSV source data (gitignored)
  processed/   # PySpark pipeline output (gitignored)
src/           # PySpark transformation scripts
docker/        # Docker Compose / Spark cluster config
notebooks/     # Exploratory Jupyter notebooks
```

## Data Source

PGA Tour statistics, 2015–2022 (~37k rows), sourced from Kaggle: https://www.kaggle.com/datasets/robikscube/pga-tour-golf-data-20152022
