"""
Core PySpark transformation for the PGA Tour portfolio pipeline.

Reads the raw Kaggle export, applies the cleaning steps identified during
prior pandas exploration (notebooks/initial_exploration.ipynb), and builds
two aggregated output tables: player_season_stats and course_difficulty.

Run against the local Docker Spark cluster (docker/docker-compose.yml must
already be up):

    python src/transform.py

The driver runs locally on the host and submits work to spark-master /
spark-worker over spark://localhost:7077. See the SPARK_DATA_DIR / HOST_*
constants below for why the script uses two different notions of "the data
directory" -- that's not an accident.
"""

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
# The docker-compose setup bind-mounts ../data into BOTH containers at
# /opt/spark/work-dir/data. The driver process (this script) runs locally on
# the host, but the actual file reads/writes happen on the worker
# container(s) that execute the tasks -- so every spark.read/write call must
# use the container-side path, not a host-relative one.
SPARK_DATA_DIR = "/opt/spark/work-dir/data"

# For the small amount of plain-Python file cleanup we do after writing CSV
# output (see _write_single_csv_and_parquet), we're operating directly on
# the host filesystem, which is a different path but the *same underlying
# files* thanks to the bind mount.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
HOST_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------
# Explicit schema, no inferSchema -- avoids a slow extra scan of the file and
# avoids Spark silently guessing wrong types.
#
# NOTE: the raw CSV's header has three fully-empty junk columns (pandas
# labeled them Unnamed: 2/3/4, since they have no header text) sitting
# between `player` and `tournament name`. Spark's CSV reader maps an
# explicit schema to file columns *positionally*, so those three columns
# still have to be declared here to keep the rest of the schema aligned with
# the file -- they're dropped by name immediately after reading, in
# load_raw_data() below, and never appear in any DataFrame after that.
RAW_CSV_SCHEMA = StructType(
    [
        StructField("Player_initial_last", StringType(), True),
        StructField("tournament id", IntegerType(), True),
        StructField("player id", IntegerType(), True),
        StructField("hole_par", IntegerType(), True),
        StructField("strokes", IntegerType(), True),
        StructField("hole_DKP", DoubleType(), True),
        StructField("hole_FDP", DoubleType(), True),
        StructField("hole_SDP", IntegerType(), True),
        StructField("streak_DKP", IntegerType(), True),
        StructField("streak_FDP", DoubleType(), True),
        StructField("streak_SDP", IntegerType(), True),
        StructField("n_rounds", IntegerType(), True),
        StructField("made_cut", IntegerType(), True),
        StructField("pos", DoubleType(), True),
        StructField("finish_DKP", IntegerType(), True),
        StructField("finish_FDP", IntegerType(), True),
        StructField("finish_SDP", IntegerType(), True),
        StructField("total_DKP", DoubleType(), True),
        StructField("total_FDP", DoubleType(), True),
        StructField("total_SDP", IntegerType(), True),
        StructField("player", StringType(), True),
        StructField("Unnamed: 2", StringType(), True),  # junk, dropped after read
        StructField("Unnamed: 3", StringType(), True),  # junk, dropped after read
        StructField("Unnamed: 4", StringType(), True),  # junk, dropped after read
        StructField("tournament name", StringType(), True),
        StructField("course", StringType(), True),
        StructField("date", StringType(), True),
        StructField("purse", DoubleType(), True),
        StructField("season", IntegerType(), True),
        StructField("no_cut", IntegerType(), True),
        StructField("Finish", StringType(), True),
        StructField("sg_putt", DoubleType(), True),
        StructField("sg_arg", DoubleType(), True),
        StructField("sg_app", DoubleType(), True),
        StructField("sg_ott", DoubleType(), True),
        StructField("sg_t2g", DoubleType(), True),
        StructField("sg_total", DoubleType(), True),
    ]
)


# --------------------------------------------------------------------------
# Load
# --------------------------------------------------------------------------
def load_raw_data(spark: SparkSession) -> DataFrame:
    """Read the raw CSV with RAW_CSV_SCHEMA and drop the Unnamed junk columns."""
    return (
        spark.read.option("header", True)
        .schema(RAW_CSV_SCHEMA)
        .csv(f"{SPARK_DATA_DIR}/raw/pga_tour_raw.csv")
        .drop("Unnamed: 2", "Unnamed: 3", "Unnamed: 4")
    )


# --------------------------------------------------------------------------
# Cleaning steps
# --------------------------------------------------------------------------
def drop_conflicting_duplicate_pairs(df: DataFrame) -> DataFrame:
    """
    Step 1: drop conflicting duplicate (tournament id, player id) rows.

    Prior pandas exploration found 21 (tournament id, player id) pairs with
    two contradictory rows each (42 rows total) -- e.g. different `Finish`
    and `sg_total` values for what should be one player's result in one
    tournament, despite an identical `made_cut` flag. There's no reliable
    signal in the data for which of the two rows is correct, so we drop
    BOTH rows for any pair that shows up more than once rather than guess.

    Implementation: count rows per (tournament id, player id), flag any key
    with count > 1 as "conflicting", then left-anti join the original data
    against those conflicting keys. A left-anti join removes every row whose
    key matches -- not just the extras -- which is what "drop both" needs.
    """
    key_cols = ["tournament id", "player id"]
    before_count = df.count()

    conflicting_keys = df.groupBy(*key_cols).count().filter(F.col("count") > 1).select(*key_cols)

    deduped = df.join(conflicting_keys, on=key_cols, how="left_anti")
    dropped = before_count - deduped.count()
    print(f"Dropped {dropped} rows from conflicting duplicate (tournament id, player id) pairs (expected 42).")
    return deduped


def drop_pos_column(df: DataFrame) -> DataFrame:
    """
    Step 2: drop `pos` entirely.

    `Finish` is used as the authoritative finish-position field instead --
    it has fewer nulls, is more human-readable ("T32" vs a bare number), and
    on closer inspection `pos` isn't a clean derivative of `Finish`, so
    keeping both would just invite someone to trust the wrong one.
    """
    return df.drop("pos")


def parse_finish_position(df: DataFrame) -> DataFrame:
    """
    Step 3: derive a clean numeric `finish_position` from `Finish`.

    The original `Finish` column is kept as-is for reference/display.
    `Finish` values look like "1", "T32", "CUT", "WD", "DQ". We strip a
    leading "T" (tie marker) and cast to int for anything that's otherwise
    all digits; anything else (CUT/WD/DQ/null/etc.) becomes null since
    there's no numeric position to report.
    """
    stripped = F.regexp_replace(F.col("Finish"), "^T", "")
    is_numeric = stripped.rlike(r"^\d+$")
    return df.withColumn(
        "finish_position",
        F.when(is_numeric, stripped.cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType())),
    )


def filter_post_2016_seasons(df: DataFrame) -> DataFrame:
    """
    Step 4: restrict to season > 2016 (2017 onward).

    Prior null analysis showed this is the range where strokes-gained
    columns are consistently populated; earlier seasons have enough SG
    nulls to skew the per-player/per-course averages built below.
    """
    before_count = df.count()
    filtered = df.filter(F.col("season") > 2016)
    after_count = filtered.count()
    print(f"Row count before post-2016 filter: {before_count}")
    print(f"Row count after post-2016 filter:  {after_count}")
    return filtered


def clean_data(df: DataFrame) -> DataFrame:
    """Run all four cleaning steps in order. See each step's docstring above."""
    df = drop_conflicting_duplicate_pairs(df)
    df = drop_pos_column(df)
    df = parse_finish_position(df)
    df = filter_post_2016_seasons(df)
    return df


# --------------------------------------------------------------------------
# Output tables
# --------------------------------------------------------------------------
def build_player_season_stats(df: DataFrame) -> DataFrame:
    """
    One row per (season, player_id): season-level scoring/strokes-gained
    summary plus a season-over-season strokes-gained trend.

    Two window functions do work a plain groupBy can't:

      - sg_total_rank: each player's rank *within their season*, ordered by
        avg_sg_total descending (rank 1 = best SG season). Partitioning by
        season means ranks reset every year instead of competing across the
        whole multi-season dataset.
      - sg_total_prev_season / sg_total_delta: lag() looks at the previous
        row for the same player_id once rows are ordered by season -- i.e.
        that player's own prior-season avg_sg_total. Subtracting gives a
        year-over-year improvement/decline figure. A player's first season
        on tour has no prior row, so lag() naturally returns null there,
        which correctly propagates into a null sg_total_delta (no fabricated
        baseline).
    """
    per_player_season = df.groupBy(F.col("season"), F.col("player id").alias("player_id")).agg(
        # first("player") rather than grouping on it directly: player_id is
        # the real grouping key, and this is a defensive way to always get a
        # single player name per group even if name formatting ever varies.
        F.first("player").alias("player"),
        F.count(F.lit(1)).alias("tournaments_played"),
        F.avg("sg_putt").alias("avg_sg_putt"),
        F.avg("sg_arg").alias("avg_sg_arg"),
        F.avg("sg_app").alias("avg_sg_app"),
        F.avg("sg_ott").alias("avg_sg_ott"),
        F.avg("sg_t2g").alias("avg_sg_t2g"),
        F.avg("sg_total").alias("avg_sg_total"),
        # finish_position is null for CUT/WD/DQ rows; a null comparison
        # (e.g. null == 1) evaluates to null, which F.when() treats as
        # false and routes to otherwise(0) -- exactly what we want here.
        F.sum(F.when(F.col("finish_position") == 1, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("finish_position") <= 5, 1).otherwise(0)).alias("top_5_finishes"),
        F.sum(F.when(F.col("finish_position") <= 10, 1).otherwise(0)).alias("top_10_finishes"),
        F.sum("made_cut").alias("cuts_made"),
    )

    season_rank_window = Window.partitionBy("season").orderBy(F.desc("avg_sg_total"))
    player_trend_window = Window.partitionBy("player_id").orderBy("season")

    result = per_player_season.withColumn("sg_total_rank", F.rank().over(season_rank_window)).withColumn(
        "sg_total_prev_season", F.lag("avg_sg_total").over(player_trend_window)
    )
    result = result.withColumn("sg_total_delta", F.col("avg_sg_total") - F.col("sg_total_prev_season"))

    return result.select(
        "season",
        "player_id",
        "player",
        "tournaments_played",
        "avg_sg_putt",
        "avg_sg_arg",
        "avg_sg_app",
        "avg_sg_ott",
        "avg_sg_t2g",
        "avg_sg_total",
        "wins",
        "top_5_finishes",
        "top_10_finishes",
        "cuts_made",
        "sg_total_rank",
        "sg_total_prev_season",
        "sg_total_delta",
    )


def build_course_difficulty(df: DataFrame) -> DataFrame:
    """
    One row per course: hosting frequency, average scoring relative to par,
    and average strokes-gained by category, ranked hardest-to-easiest.

    difficulty_rank uses a single global window (Window.orderBy(...), no
    partitionBy) since "hardest course" is a ranking across the whole
    dataset, not within some other grouping -- and there's only one row per
    course to begin with, so collapsing to a single partition here is cheap.
    Ranked ascending on avg_sg_total: strokes-gained is relative to the
    field, so the most negative avg_sg_total means players did worst
    relative to expectation there -- rank 1 = hardest course.
    """
    per_course = df.groupBy("course").agg(
        F.countDistinct("tournament id").alias("tournaments_hosted"),
        F.avg(F.col("strokes") - F.col("hole_par")).alias("avg_strokes_vs_par"),
        F.avg("sg_total").alias("avg_sg_total"),
        F.avg("sg_putt").alias("avg_sg_putt"),
        F.avg("sg_arg").alias("avg_sg_arg"),
        F.avg("sg_app").alias("avg_sg_app"),
        F.avg("sg_ott").alias("avg_sg_ott"),
    )

    difficulty_window = Window.orderBy(F.asc("avg_sg_total"))
    return per_course.withColumn("difficulty_rank", F.rank().over(difficulty_window))


# --------------------------------------------------------------------------
# Write
# --------------------------------------------------------------------------
def _write_single_csv_and_parquet(df: DataFrame, name: str) -> None:
    """
    Write `df` to data/processed/ as both Parquet and a single flat CSV.

    Parquet is the "proper" output here -- compressed, typed, splittable
    columnar storage, and what a real downstream Spark/warehouse job would
    consume. CSV is written alongside it purely for convenience (a quick
    open in Excel/Power BI).

    Spark's CSV writer always produces a *directory* of part-files, even
    forced to one partition via coalesce(1) -- there's no writer option that
    produces a bare .csv file directly. So we write CSV to a throwaway
    directory and then promote the single part-file to a flat filename
    ourselves, cleaning up Spark's directory litter (_SUCCESS, checksums,
    etc.) afterward. coalesce(1) is safe here specifically because these are
    small, already-aggregated tables -- doing this on the raw, row-level
    dataset would kill parallelism.
    """
    df.write.mode("overwrite").parquet(f"{SPARK_DATA_DIR}/processed/{name}.parquet")

    tmp_name = f"_{name}_csv_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{SPARK_DATA_DIR}/processed/{tmp_name}")

    # From here on we operate on the *host* filesystem path rather than the
    # container path -- the driver runs locally on the host, and data/ is
    # bind-mounted 1:1 into the worker containers, so the file Spark just
    # wrote is visible here too, just under a different mount point.
    tmp_host_dir = HOST_PROCESSED_DIR / tmp_name
    final_host_path = HOST_PROCESSED_DIR / f"{name}.csv"

    part_file = next(tmp_host_dir.glob("part-*.csv"))
    if final_host_path.exists():
        final_host_path.unlink()
    part_file.rename(final_host_path)
    shutil.rmtree(tmp_host_dir)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("pga-tour-transform").master("spark://localhost:7077").getOrCreate()


def main() -> None:
    HOST_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session()
    try:
        raw_df = load_raw_data(spark)

        cleaned_df = clean_data(raw_df)
        # Reused as the source for both output tables below (plus its own
        # checkpoint count/show), so cache it to avoid redoing the dedup +
        # filter chain three times over.
        cleaned_df.cache()
        print(f"\nCleaned row count: {cleaned_df.count()}")
        cleaned_df.show(10, truncate=False)

        player_season_stats = build_player_season_stats(cleaned_df)
        print(f"\nplayer_season_stats row count: {player_season_stats.count()}")
        player_season_stats.show(10, truncate=False)

        course_difficulty = build_course_difficulty(cleaned_df)
        print(f"\ncourse_difficulty row count: {course_difficulty.count()}")
        course_difficulty.show(10, truncate=False)

        _write_single_csv_and_parquet(player_season_stats, "player_season_stats")
        _write_single_csv_and_parquet(course_difficulty, "course_difficulty")
        print("\nWrote player_season_stats and course_difficulty to data/processed/ (csv + parquet).")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
