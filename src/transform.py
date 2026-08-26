"""
Core PySpark transformation for the PGA Tour portfolio pipeline.

Reads the raw Kaggle export, applies the cleaning steps identified during
prior pandas exploration (notebooks/initial_exploration.ipynb), and builds
two aggregated output tables: player_season_stats and course_difficulty.

Run against the local Docker Spark cluster (docker/docker-compose.yml must
already be up):

    python src/transform.py

The driver runs locally on the host and submits work to spark-master /
spark-worker over spark://localhost:7077. See PROJECT_DATA_DIR below for why
a single absolute path works for both the driver and the executors here.
"""

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
# In client deploy mode, spark.read/write path resolution happens on the
# DRIVER's local filesystem (this process, running on the host), even though
# the actual task execution happens on the worker container(s). So the same
# absolute path has to resolve to the same file on both sides. The
# docker-compose setup achieves that by bind-mounting data/ into both
# containers at this exact host path, rather than remapping it to some
# container-only path -- see docker/docker-compose.yml.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROJECT_DATA_DIR = PROJECT_ROOT / "data"

# --------------------------------------------------------------------------
# Ranking thresholds
# --------------------------------------------------------------------------
# Minimum tournaments_played a player-season needs to be eligible for the
# avg_sg_*_rank columns. Without this, a player with a tiny sample (e.g. 1-3
# tournaments) can post a misleadingly high average and rank #1 ahead of
# full-season regulars. See build_player_season_stats() below.
MIN_TOURNAMENTS_FOR_RANKING = 10

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
        .csv(f"{PROJECT_DATA_DIR}/raw/pga_tour_raw.csv")
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

    Strokes-gained is reported two ways per category (putt/arg/app/ott/t2g/
    total): avg_sg_* (mean per tournament, "how good is this player on a
    given day") and sum_sg_* (season total, "how much value has this player
    banked all year"). Note "total" here means the sg_total category itself
    (overall strokes gained, distinct from putt/arg/app/ott/t2g) -- the sum
    columns use a "sum_" prefix rather than "total_" specifically to avoid
    "total_sg_total" reading as ambiguous between the aggregation and the
    category.

    Window functions do work a plain groupBy can't:

      - avg_sg_*_rank (6 columns): each player's rank *within their season*
        for that avg_sg_* column, ordered descending (rank 1 = best). Gated
        behind MIN_TOURNAMENTS_FOR_RANKING -- without a minimum sample size,
        a player with e.g. 1-3 tournaments can post a misleadingly high
        average and rank #1 ahead of full-season regulars. Rows below the
        threshold get a null rank here rather than being dropped from the
        table; their raw stats are still visible. Implemented by ranking
        only the qualifying subset, then left-joining that back onto the
        full table.
      - sum_sg_*_rank (6 columns): same rank() mechanism, but over the whole
        table with no threshold -- a cumulative season total naturally
        sorts low-volume players toward the bottom without needing to
        exclude them.
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
        F.sum("sg_putt").alias("sum_sg_putt"),
        F.sum("sg_arg").alias("sum_sg_arg"),
        F.sum("sg_app").alias("sum_sg_app"),
        F.sum("sg_ott").alias("sum_sg_ott"),
        F.sum("sg_t2g").alias("sum_sg_t2g"),
        F.sum("sg_total").alias("sum_sg_total"),
        # finish_position is null for CUT/WD/DQ rows; a null comparison
        # (e.g. null == 1) evaluates to null, which F.when() treats as
        # false and routes to otherwise(0) -- exactly what we want here.
        F.sum(F.when(F.col("finish_position") == 1, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("finish_position") <= 5, 1).otherwise(0)).alias("top_5_finishes"),
        F.sum(F.when(F.col("finish_position") <= 10, 1).otherwise(0)).alias("top_10_finishes"),
        F.sum("made_cut").alias("cuts_made"),
    )

    player_trend_window = Window.partitionBy("player_id").orderBy("season")
    result = per_player_season.withColumn(
        "sg_total_prev_season", F.lag("avg_sg_total").over(player_trend_window)
    )
    result = result.withColumn("sg_total_delta", F.col("avg_sg_total") - F.col("sg_total_prev_season"))

    # avg_sg_*_rank: ranked only among qualifying (tournaments_played >= 10)
    # rows, then left-joined back onto the full table so non-qualifying rows
    # get a null rank instead of being excluded.
    avg_sg_columns = ["avg_sg_putt", "avg_sg_arg", "avg_sg_app", "avg_sg_ott", "avg_sg_t2g", "avg_sg_total"]
    qualified = result.filter(F.col("tournaments_played") >= MIN_TOURNAMENTS_FOR_RANKING)
    for col_name in avg_sg_columns:
        rank_window = Window.partitionBy("season").orderBy(F.desc(col_name))
        qualified = qualified.withColumn(f"{col_name}_rank", F.rank().over(rank_window))
    avg_rank_columns = [f"{col_name}_rank" for col_name in avg_sg_columns]
    result = result.join(
        qualified.select("season", "player_id", *avg_rank_columns), on=["season", "player_id"], how="left"
    )

    # sum_sg_*_rank: ranked over every row, no qualification threshold.
    sum_sg_columns = ["sum_sg_putt", "sum_sg_arg", "sum_sg_app", "sum_sg_ott", "sum_sg_t2g", "sum_sg_total"]
    for col_name in sum_sg_columns:
        rank_window = Window.partitionBy("season").orderBy(F.desc(col_name))
        result = result.withColumn(f"{col_name}_rank", F.rank().over(rank_window))

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
        "sum_sg_putt",
        "sum_sg_arg",
        "sum_sg_app",
        "sum_sg_ott",
        "sum_sg_t2g",
        "sum_sg_total",
        "wins",
        "top_5_finishes",
        "top_10_finishes",
        "cuts_made",
        "avg_sg_putt_rank",
        "avg_sg_arg_rank",
        "avg_sg_app_rank",
        "avg_sg_ott_rank",
        "avg_sg_t2g_rank",
        "avg_sg_total_rank",
        "sum_sg_putt_rank",
        "sum_sg_arg_rank",
        "sum_sg_app_rank",
        "sum_sg_ott_rank",
        "sum_sg_t2g_rank",
        "sum_sg_total_rank",
        "sg_total_prev_season",
        "sg_total_delta",
    )


def build_course_difficulty(df: DataFrame) -> DataFrame:
    """
    One row per course: hosting frequency, average scoring relative to par,
    and average strokes-gained by category, ranked hardest-to-easiest.

    difficulty_rank and avg_strokes_vs_par_rank each use a single global
    window (Window.orderBy(...), no partitionBy) since "hardest course" is a
    ranking across the whole dataset, not within some other grouping -- and
    there's only one row per course to begin with, so collapsing to a single
    partition here is cheap. difficulty_rank is ranked ascending on
    avg_sg_total: strokes-gained is relative to the field, so the most
    negative avg_sg_total means players did worst relative to expectation
    there -- rank 1 = hardest course.

    ShotLink coverage gap: strokes-gained data only exists at regular
    domestic PGA Tour stops -- ShotLink is the Tour's shot-tracking
    hardware and it isn't deployed at major championships (The Open, U.S.
    Open) or international/limited-field events (WGC events, ZOZO
    Championship, CJ Cup, etc.). As of this writing that's 17 of 81
    courses with 100% null avg_sg_total across every row. This is a real,
    structural gap in the source data, not a data quality bug -- so those
    courses must never receive a low (or any) difficulty_rank just because
    Spark's default ascending sort treats nulls as "smallest". We use
    asc_nulls_last to push null avg_sg_total to the bottom of the window,
    then explicitly null out difficulty_rank for those rows so they read as
    "no SG-based rank" rather than a misleadingly low rank number.

    avg_strokes_vs_par_rank is a fallback difficulty signal that covers all
    81 courses, including the 17 missing SG data: avg_strokes_vs_par is
    built from `strokes` and `hole_par`, both fully populated regardless of
    ShotLink coverage. Ranked descending -- a higher strokes-vs-par means
    players took more strokes over par on average, i.e. the course played
    harder, so the highest value is rank 1.
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

    difficulty_window = Window.orderBy(F.asc_nulls_last("avg_sg_total"))
    strokes_vs_par_window = Window.orderBy(F.desc_nulls_last("avg_strokes_vs_par"))
    return per_course.withColumn(
        "difficulty_rank",
        F.when(
            F.col("avg_sg_total").isNotNull(), F.rank().over(difficulty_window)
        ),
    ).withColumn(
        "avg_strokes_vs_par_rank",
        F.when(
            F.col("avg_strokes_vs_par").isNotNull(),
            F.rank().over(strokes_vs_par_window),
        ),
    )


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
    processed_dir = PROJECT_DATA_DIR / "processed"
    df.write.mode("overwrite").parquet(f"{processed_dir}/{name}.parquet")

    tmp_name = f"_{name}_csv_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{processed_dir}/{tmp_name}")

    # The plain-Python cleanup below (promoting Spark's single part-file to
    # a flat filename) runs in this same process, on this same path -- no
    # separate host/container path to reconcile anymore.
    tmp_dir = processed_dir / tmp_name
    final_path = processed_dir / f"{name}.csv"

    part_file = next(tmp_dir.glob("part-*.csv"))
    if final_path.exists():
        final_path.unlink()
    part_file.rename(final_path)
    shutil.rmtree(tmp_dir)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("pga-tour-transform").master("spark://localhost:7077").getOrCreate()


def main() -> None:
    (PROJECT_DATA_DIR / "processed").mkdir(parents=True, exist_ok=True)

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
