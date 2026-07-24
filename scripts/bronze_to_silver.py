from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, current_timestamp
import logging
import sys
from datetime import datetime

# GCS CONFIG
GCS_BUCKET = "worldcup-football-bucket"
COMPETITION_CODE = "WC"
SEASON = "2026"

BRONZE_ROOT = f"gs://{GCS_BUCKET}/raw"
SILVER_ROOT = f"gs://{GCS_BUCKET}/silver"

TEAMS_RAW = f"{BRONZE_ROOT}/worldcup_teams.ndjson"
STANDINGS_RAW = f"{BRONZE_ROOT}/worldcup_standings.ndjson"
MATCHES_RAW = f"{BRONZE_ROOT}/worldcup_matches.ndjson"

SILVER_TEAMS = f"{SILVER_ROOT}/teams"
SILVER_STANDINGS = f"{SILVER_ROOT}/standings"
SILVER_MATCHES = f"{SILVER_ROOT}/matches"

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)


def section(title: str):
    logging.info(f"===== {title.upper()} =====")


# SPARK SESSION
def create_spark_session():
    return SparkSession.builder.appName("BronzeToSilver_WorldCup") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()


# TRANSFORMATIONS
def flatten_teams_data(spark):
    section("Flattening Teams Data")
    df = spark.read.json(TEAMS_RAW)

    if "teams" not in df.columns:
        logging.warning("No 'teams' array found — skipping")
        return None

    return df.select(explode("teams").alias("team")) \
             .select(
                 col("team.idTeam").alias("team_id"),
                 col("team.strTeam").alias("team_name"),
                 col("team.strTeamShort").alias("team_short_name"),
                 col("team.intFormedYear").alias("team_founded"),
                 col("team.strColour1").alias("team_colour_1"),
                 col("team.strColour2").alias("team_colour_2"),
                 col("team.strStadium").alias("team_stadium"),
                 col("team.strLocation").alias("team_location"),
                 col("team.strWebsite").alias("team_website"),
                 col("team.strLogo").alias("team_logo"),
                 col("team.strBadge").alias("team_badge"),
                 col("team.strCountry").alias("team_country")
    )


def flatten_standings_data(spark):
    section("Flattening Standings Data")
    df = spark.read.json(STANDINGS_RAW)

    if "table" not in df.columns:
        logging.warning("No 'table' array found — skipping")
        return None

    return df.select(explode("table").alias("standing")) \
             .select(
                 col("standing.idStanding").alias("standing_id"),
                 col("standing.intRank").alias("position"),
                 col("standing.idTeam").alias("team_id"),
                 col("standing.strTeam").alias("team_name"),
                 col("standing.strBadge").alias("team_badge"),
                 col("standing.intPlayed").alias("played_games"),
                 col("standing.intWin").alias("won"),
                 col("standing.intDraw").alias("drawn"),
                 col("standing.intLoss").alias("lost"),
                 col("standing.intGoalsFor").alias("goals_for"),
                 col("standing.intGoalsAgainst").alias("goals_against"),
                 col("standing.intGoalDifference").alias("goal_difference"),
                 col("standing.intPoints").alias("points"),
                 col("standing.strForm").alias("recent_form"),
                 col("standing.strDescription").alias("group_name")
    )


def flatten_matches_data(spark):
    section("Flattening Matches Data")
    df = spark.read.json(MATCHES_RAW)

    if "events" not in df.columns:
        logging.warning("No 'events' array found — skipping")
        return None

    return df.select(explode("events").alias("match")) \
             .select(
                 col("match.idEvent").alias("match_id"),
                 col("match.strEvent").alias("match_name"),
                 col("match.dateEvent").alias("match_date"),
                 col("match.strTimestamp").alias("match_utc_timestamp"),
                 col("match.strTime").alias("match_time_utc"),
                 col("match.strStatus").alias("match_status"),
                 col("match.strVenue").alias("venue_name"),
                 col("match.strCountry").alias("venue_country"),
                 col("match.intRound").alias("match_round"),
                 col("match.idHomeTeam").alias("home_team_id"),
                 col("match.strHomeTeam").alias("home_team_name"),
                 col("match.intHomeScore").alias("home_score"),
                 col("match.idAwayTeam").alias("away_team_id"),
                 col("match.strAwayTeam").alias("away_team_name"),
                 col("match.intAwayScore").alias("away_score"),
                 col("match.strHomeTeamBadge").alias("home_team_badge"),
                 col("match.strAwayTeamBadge").alias("away_team_badge"),
                 col("match.strThumb").alias("match_thumbnail"),
                 col("match.strPoster").alias("match_poster"),
                 col("match.strVideo").alias("match_video_url")
    )


def write_to_silver(df, output_path):
    section(f"Writing → {output_path}")
    if df is None:
        logging.warning("No data to write — skipping")
        return
    df.withColumn("ingested_at", current_timestamp()) \
      .write.mode("overwrite") \
      .option("compression", "snappy") \
      .parquet(output_path)
    logging.info("Written successfully")


# MAIN EXECUTION
if __name__ == "__main__":
    section("BRONZE → SILVER TRANSFORMATION")
    spark = create_spark_session()

    try:
        write_to_silver(flatten_teams_data(spark), SILVER_TEAMS)
        write_to_silver(flatten_standings_data(spark), SILVER_STANDINGS)
        write_to_silver(flatten_matches_data(spark), SILVER_MATCHES)
        logging.info("ALL SILVER TABLES COMPLETE")
    except Exception as e:
        logging.error(f"FAILED: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
