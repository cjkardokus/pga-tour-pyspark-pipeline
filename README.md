# pga-tour-pyspark-pipeline
A PySpark data pipeline for processing PGA Tour golf data, run on a local Docker Spark cluster, feeding a Power BI dashboard.

## Overview
The pipeline creates two data sets, Course Difficulty and Player Season Stats, that the dashboard uses to display metrics on the difficulty of each golf course where a PGA Tour tournament was played, and player stats from these tournaments including year to year trends in player performance.

## Dashboard
The interactive Power BI dashboard is available at [`dashboard/golf_analytics.pbix`](dashboard/golf_analytics.pbix). Requires [Power BI Desktop](https://powerbi.microsoft.com/desktop/) (free, Windows) to open and explore.

**Pages:**
- **Player Trends** — season-by-season strokes-gained trends for a selected player, including strokes gained data split into short game and long game
- **Total Strokes Gained Leader Boards** — rankings by cumulative strokes gained for the 2017 through 2022 seasons
- **Average Strokes Gained Leader Boards** — rankings by average strokes gained (10+ tournament minimum) for the 2017 through 2022 seasons
- **Course Difficulty** — courses ranked by average strokes gained relative to Tour expectation

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
4. Run the pipeline:
   ```bash
   python src/transform.py
   ```
   Output lands in `data/processed/` as both CSV and Parquet.

## Project Structure
```
dashboard/     # Power BI .pbix file
data/
  raw/         # Raw Kaggle CSV source data (gitignored)
  processed/   # PySpark pipeline output (gitignored)
src/           # PySpark transformation scripts
docker/        # Docker Compose / Spark cluster config
notebooks/     # Exploratory Jupyter notebooks
```

## Data Source
PGA Tour statistics, 2015–2022 (~37k rows), sourced from Kaggle: https://www.kaggle.com/datasets/robikscube/pga-tour-golf-data-20152022

Data is filtered to 2017 onward, since strokes-gained tracking was inconsistently populated in earlier seasons.

## Data Quality Notes
A few real data-quality issues were found and handled during pipeline development, worth noting for anyone reviewing or extending this:

- **Conflicting duplicate rows.** 21 player-tournament pairs (42 rows) had two contradictory records — different finish position and strokes-gained values for the same player in the same tournament, with no reliable way to determine which was correct. Both rows are dropped for any conflicting pair.
- **Small-sample ranking distortion.** Average strokes-gained rankings are restricted to players with 10+ tournaments in a season; without this, players with very few tournaments occasionally posted a misleadingly high average off a small sample, ranking above full-season regulars. Total (cumulative) strokes-gained rankings have no such restriction, since low-volume players naturally sort lower on a cumulative total.
- **ShotLink coverage gaps.** Strokes-gained data is missing for 17 of 81 courses — mainly major championships (run by the R&A/USGA, not the PGA Tour) and international/limited-field events, which historically lacked ShotLink (the PGA Tour's shot-tracking infrastructure) coverage. Coverage isn't static either — the Masters began SG tracking starting in 2022, so Augusta National shows partial rather than uniform coverage. These courses are ranked instead by average strokes over par, which doesn't depend on SG data.
- **Isolated player-record gaps.** A small number of individual player-seasons (e.g., Rory McIlroy's 2022 season) have null strokes-gained and finish data across every tournament that season, despite those same tournaments having complete data for other players — an isolated source-data gap rather than the broader ShotLink pattern above.
- **Power BI display limitation.** Sorting the course difficulty or SG leaderboard columns descending may temporarily show courses/rows without SG data in an unexpected position, due to how Power BI's table visual orders null values on click-sort.
