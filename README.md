# pga-tour-pyspark-pipeline

A PySpark data pipeline for processing PGA Tour golf data, run on a local Docker Spark cluster, feeding a Power BI dashboard.

## Overview

<!-- TODO: finalize the questions this pipeline answers -->
TODO: describe what questions/insights this pipeline is meant to answer.

## Tech Stack

- **PySpark** — data transformation
- **Docker** — local Spark cluster
- **Power BI** — dashboard / reporting layer

## Setup

<!-- TODO: flesh out once the Docker Spark cluster config is finalized -->

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
   ```
   TODO
   ```

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

PGA Tour statistics, 2015–2022 (~37k rows), sourced from Kaggle.
