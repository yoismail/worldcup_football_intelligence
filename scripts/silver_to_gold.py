from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum as spark_sum, avg, round, coalesce, current_timestamp
from pyspark.sql.window import Window
import logging
import sys

# LOGGING SETUP
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)


def section(title: str):
    logging.info(f"===== {title.upper()} =====")


# GCS CONFIG
GCS_BUCKET = "worldcup-football-bucket"
SILVER_PATH = f"gs://{GCS_BUCKET}/silver"
GOLD_PATH = f"gs://{GCS_BUCKET}/gold"


# SPARK SESSION
def create_spark_session():
    return SparkSession.builder.appName("SilverToGold_WorldCup") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()


# MAIN TRANSFORMATION LOGIC
if __name__ == "__main__":
    spark = create_spark_session()
    section("SILVER → GOLD TRANSFORMATION")

    try:
        # 1. LOAD SILVER DATA
        section("Loading Silver Layer Data")
        df_teams = spark.read.parquet(f"{SILVER_PATH}/teams")
        df_standings = spark.read.parquet(f"{SILVER_PATH}/standings")
        df_matches = spark.read.parquet(f"{SILVER_PATH}/matches")

        # 2. GOLD – TEAMS: Clean & Standardise
        section("Building Gold: Teams")
        df_teams_gold = df_teams.select(
            col("team_id"),
            col("team_name"),
            col("team_short_name"),
            col("team_founded"),
            col("team_country"),
            col("team_stadium"),
            col("team_location"),
            col("team_badge"),
            col("team_logo"),
            current_timestamp().alias("gold_loaded_at")
        ).distinct()

        # 3. GOLD – MATCHES: Calculate derived metrics
        section("Building Gold: Matches")
        df_matches_gold = df_matches.select(
            col("match_id"),
            col("match_date"),
            col("match_time_utc"),
            col("match_status"),
            col("venue_name"),
            col("venue_country"),
            col("match_round"),
            col("home_team_id"),
            col("home_team_name"),
            coalesce(col("home_score"), lit(0)).alias("home_score"),
            col("away_team_id"),
            col("away_team_name"),
            coalesce(col("away_score"), lit(0)).alias("away_score"),
            col("home_team_badge"),
            col("away_team_badge"),
            when(col("home_score") > col("away_score"), "Home Win")
            .when(col("home_score") < col("away_score"), "Away Win")
            .otherwise("Draw").alias("result"),
            (col("home_score") + col("away_score")).alias("total_goals"),
            (col("home_score") - col("away_score")).alias("home_goal_diff"),
            (col("away_score") - col("home_score")).alias("away_goal_diff"),
            current_timestamp().alias("gold_loaded_at")
        )

        # 4. GOLD – STANDINGS: Enrich & Advanced Stats
        section("Building Gold: Standings")
        window_rank = Window.partitionBy(
            "group_name").orderBy(col("position").asc())

        df_standings_gold = df_standings.select(
            col("standing_id"),
            col("position"),
            col("team_id"),
            col("team_name"),
            col("team_badge"),
            col("group_name"),
            col("played_games"),
            col("won"),
            col("drawn"),
            col("lost"),
            col("goals_for"),
            col("goals_against"),
            col("goal_difference"),
            col("points"),
            col("recent_form"),
            round(col("points") / col("played_games"),
                  2).alias("points_per_game"),
            round(col("won") / col("played_games")
                  * 100, 1).alias("win_percentage"),
            round(col("goals_for") / col("played_games"),
                  2).alias("goals_scored_per_game"),
            round(col("goals_against") / col("played_games"),
                  2).alias("goals_conceded_per_game"),
            spark_sum(col("points")).over(window_rank.rangeBetween(
                Window.unboundedPreceding, 0)).alias("running_total_points"),
            current_timestamp().alias("gold_loaded_at")
        ).orderBy("group_name", "position")

        # 5. WRITE GOLD PARQUET
        section("Writing Gold Layer to GCS")
        df_teams_gold.write.mode("overwrite").option(
            "compression", "snappy").parquet(f"{GOLD_PATH}/teams")
        df_standings_gold.write.mode("overwrite").option(
            "compression", "snappy").parquet(f"{GOLD_PATH}/standings")
        df_matches_gold.write.mode("overwrite").option(
            "compression", "snappy").parquet(f"{GOLD_PATH}/matches")

        logging.info("ALL GOLD TRANSFORMATIONS COMPLETE")

    except Exception as e:
        logging.error(f"FAILED: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
