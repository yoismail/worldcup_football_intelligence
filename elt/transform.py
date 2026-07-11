from pyspark.sql.functions import explode, col, current_timestamp
from elt.spark_session import create_spark_session
import os
import logging
import sys
from elt.logger import setup_logging, section, timed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# CONFIG - from .env
COMPETITION_CODE = os.getenv("COMPETITION_CODE", "WC")
BRONZE_BASE = os.getenv("BRONZE_BASE", "/opt/airflow/data/bronze")
SILVER_BASE = os.getenv("SILVER_BASE", "/opt/airflow/data/silver")

# Build all paths dynamically
COMPETITION_PATH = os.path.join(
    BRONZE_BASE, "competitions", f"{COMPETITION_CODE}.ndjson")
MATCHES_PATH = os.path.join(BRONZE_BASE, "matches")
PLAYERS_PATH = os.path.join(BRONZE_BASE, "players")
STANDINGS_PATH = os.path.join(BRONZE_BASE, "standings")
TEAMS_PATH = os.path.join(BRONZE_BASE, "teams")
VENUES_PATH = os.path.join(BRONZE_BASE, "venues")

SILVER_COMPETITION_PATH = os.path.join(SILVER_BASE, "competitions")
SILVER_MATCHES_PATH = os.path.join(SILVER_BASE, "matches")
SILVER_PLAYERS_PATH = os.path.join(SILVER_BASE, "players")
SILVER_STANDINGS_PATH = os.path.join(SILVER_BASE, "standings")
SILVER_TEAMS_PATH = os.path.join(SILVER_BASE, "teams")
SILVER_VENUES_PATH = os.path.join(SILVER_BASE, "venues")


# SILVER LAYER Transformation Functions
def flatten_competition_data(spark):
    section("Flattening competition data")

    if not os.path.isfile(COMPETITION_PATH):
        logging.error(
            f"Competition data file does not exist: {COMPETITION_PATH}")
        sys.exit(1)
    logging.info(f"Competition data file found: {COMPETITION_PATH}")

    competition_df = spark.read.json(COMPETITION_PATH)

    df_exploded = competition_df.select(
        col("id").alias("competition_id"),
        col("name").alias("competition_name"),
        col("code").alias("competition_code"),
        col("type").alias("competition_type"),
        col("emblem").alias("competition_emblem"),
        col("lastUpdated").alias("competition_last_updated"),
        col("area.id").alias("area_id"),
        col("area.name").alias("area_name"),
        col("area.code").alias("area_code"),
        col("area.flag").alias("area_flag"),
        explode("seasons").alias("season")
    )

    df_flat = df_exploded.select(
        "*",
        col("season.id").alias("season_id"),
        col("season.startDate").alias("season_start_date"),
        col("season.endDate").alias("season_end_date"),
        col("season.currentMatchday").alias("season_current_matchday"),
        col("season.winner.id").alias("winner_id"),
        col("season.winner.name").alias("winner_name"),
        col("season.winner.shortName").alias("winner_short_name"),
        col("season.winner.tla").alias("winner_code"),
        col("season.winner.crest").alias("winner_crest"),
        col("season.winner.founded").alias("winner_founded"),
        col("season.winner.clubColors").alias("winner_colors"),
        col("season.winner.venue").alias("winner_venue"),
        col("season.winner.website").alias("winner_website")
    ).drop("season")

    df_flat.printSchema()
    logging.info(
        f"Flattened competitions: {df_flat.count()} rows | {len(df_flat.columns)} columns.")
    return df_flat


def flatten_matches_data(spark):
    section("Flattening matches data")

    if not os.path.isdir(MATCHES_PATH):
        logging.error(f"Matches folder not found: {MATCHES_PATH}")
        sys.exit(1)

    match_files = [
        os.path.join(MATCHES_PATH, f)
        for f in os.listdir(MATCHES_PATH)
        if f.endswith('.ndjson')
    ]

    if not match_files:
        logging.warning(f"No .ndjson files found in: {MATCHES_PATH}")
        return None

    logging.info(f"Found {len(match_files)} match file(s)")
    matches_df = spark.read.json(match_files)

    df_exploded = matches_df.select(
        col("filters.dateFrom").alias("filter_date_from"),
        col("filters.dateTo").alias("filter_date_to"),
        col("resultSet.count").alias("total_matches_in_file"),
        explode("matches").alias("match")
    )

    df_flat = df_exploded.select(
        "*",
        col("match.id").alias("match_id"),
        col("match.utcDate").alias("match_utc_date"),
        col("match.status").alias("match_status"),
        col("match.stage").alias("match_stage"),
        col("match.group").alias("match_group"),
        col("match.matchday").alias("match_matchday"),
        col("match.lastUpdated").alias("match_last_updated"),
        col("match.area.id").alias("area_id"),
        col("match.area.name").alias("area_name"),
        col("match.area.code").alias("area_code"),
        col("match.area.flag").alias("area_flag"),
        col("match.competition.id").alias("competition_id"),
        col("match.competition.name").alias("competition_name"),
        col("match.competition.code").alias("competition_code"),
        col("match.competition.type").alias("competition_type"),
        col("match.competition.emblem").alias("competition_emblem"),
        col("match.season.id").alias("season_id"),
        col("match.season.startDate").alias("season_start_date"),
        col("match.season.endDate").alias("season_end_date"),
        col("match.season.currentMatchday").alias("season_current_matchday"),
        col("match.homeTeam.id").alias("home_team_id"),
        col("match.homeTeam.name").alias("home_team_name"),
        col("match.homeTeam.shortName").alias("home_team_short_name"),
        col("match.homeTeam.tla").alias("home_team_tla"),
        col("match.homeTeam.crest").alias("home_team_crest"),
        col("match.awayTeam.id").alias("away_team_id"),
        col("match.awayTeam.name").alias("away_team_name"),
        col("match.awayTeam.shortName").alias("away_team_short_name"),
        col("match.awayTeam.tla").alias("away_team_tla"),
        col("match.awayTeam.crest").alias("away_team_crest"),
        col("match.score.duration").alias("score_duration"),
        col("match.score.fullTime.home").alias("full_time_home"),
        col("match.score.fullTime.away").alias("full_time_away"),
        col("match.score.halfTime.home").alias("half_time_home"),
        col("match.score.halfTime.away").alias("half_time_away"),
        col("match.score.winner").alias("score_winner")
    ).drop("match")

    logging.info(
        f"Flattened matches: {df_flat.count()} rows | {len(df_flat.columns)} columns")
    return df_flat


def flatten_player_data(spark):
    section("Flattening player data")

    if not os.path.isdir(PLAYERS_PATH):
        logging.error(f"Players folder not found: {PLAYERS_PATH}")
        sys.exit(1)

    player_files = [
        os.path.join(PLAYERS_PATH, f)
        for f in os.listdir(PLAYERS_PATH)
        if f.endswith('.ndjson')
    ]

    if not player_files:
        logging.warning(f"No .ndjson files found in: {PLAYERS_PATH}")
        return None

    logging.info(f"Found {len(player_files)} player file(s)")
    players_df = spark.read.json(player_files)

    df_exploded = players_df.select(
        col("id").alias("team_id"),
        col("name").alias("team_name"),
        col("shortName").alias("team_short_name"),
        col("tla").alias("team_tla"),
        col("founded").alias("team_founded"),
        col("clubColors").alias("team_colors"),
        col("venue").alias("team_venue"),
        col("address").alias("team_address"),
        col("website").alias("team_website"),
        col("crest").alias("team_crest"),
        col("lastUpdated").alias("team_last_updated"),
        col("area.id").alias("area_id"),
        col("area.name").alias("area_name"),
        col("area.code").alias("area_code"),
        col("area.flag").alias("area_flag"),
        col("coach.id").alias("coach_id"),
        col("coach.name").alias("coach_name"),
        col("coach.firstName").alias("coach_first_name"),
        col("coach.lastName").alias("coach_last_name"),
        col("coach.dateOfBirth").alias("coach_dob"),
        col("coach.nationality").alias("coach_nationality"),
        col("coach.contract.start").alias("coach_contract_start"),
        col("coach.contract.until").alias("coach_contract_end"),
        explode("squad").alias("player")
    )

    df_flat = df_exploded.select(
        "*",
        col("player.id").alias("player_id"),
        col("player.name").alias("player_name"),
        col("player.dateOfBirth").alias("player_dob"),
        col("player.nationality").alias("player_nationality"),
        col("player.position").alias("player_position")
    ).drop("player")

    logging.info(
        f"Flattened players: {df_flat.count()} rows | {len(df_flat.columns)} columns")
    return df_flat


def flatten_standings_data(spark):
    section("Flattening standings data")

    if not os.path.isdir(STANDINGS_PATH):
        logging.error(f"Standings folder not found: {STANDINGS_PATH}")
        sys.exit(1)

    standings_files = [
        os.path.join(STANDINGS_PATH, f)
        for f in os.listdir(STANDINGS_PATH)
        if f.endswith('.ndjson')
    ]

    if not standings_files:
        logging.warning(f"No .ndjson files found in: {STANDINGS_PATH}")
        return None

    logging.info(f"Found {len(standings_files)} standings file(s)")
    standings_df = spark.read.json(standings_files)

    df_explode_standings = standings_df.select(
        col("filters.season").alias("filter_season"),
        col("area.id").alias("area_id"),
        col("area.name").alias("area_name"),
        col("area.code").alias("area_code"),
        col("area.flag").alias("area_flag"),
        col("competition.id").alias("competition_id"),
        col("competition.name").alias("competition_name"),
        col("competition.code").alias("competition_code"),
        col("competition.type").alias("competition_type"),
        col("competition.emblem").alias("competition_emblem"),
        col("season.id").alias("season_id"),
        col("season.startDate").alias("season_start_date"),
        col("season.endDate").alias("season_end_date"),
        col("season.currentMatchday").alias("season_current_matchday"),
        col("season.winner").alias("season_winner"),
        explode("standings").alias("standing")
    )

    df_explode_table = df_explode_standings.select(
        "*",
        col("standing.group").alias("group_name"),
        col("standing.stage").alias("stage"),
        col("standing.type").alias("standing_type"),
        explode("standing.table").alias("team_record")
    ).drop("standing")

    df_flat = df_explode_table.select(
        "*",
        col("team_record.position").alias("position"),
        col("team_record.playedGames").alias("played_games"),
        col("team_record.won").alias("won"),
        col("team_record.draw").alias("draw"),
        col("team_record.lost").alias("lost"),
        col("team_record.points").alias("points"),
        col("team_record.goalsFor").alias("goals_for"),
        col("team_record.goalsAgainst").alias("goals_against"),
        col("team_record.goalDifference").alias("goal_difference"),
        col("team_record.form").alias("recent_form"),
        col("team_record.team.id").alias("team_id"),
        col("team_record.team.name").alias("team_name"),
        col("team_record.team.shortName").alias("team_short_name"),
        col("team_record.team.tla").alias("team_tla"),
        col("team_record.team.crest").alias("team_crest")
    ).drop("team_record")

    logging.info(
        f"Flattened standings: {df_flat.count()} rows | {len(df_flat.columns)} columns")
    return df_flat


def flatten_teams_data(spark):
    section("Flattening teams data")

    if not os.path.isdir(TEAMS_PATH):
        logging.error(f"Teams folder not found: {TEAMS_PATH}")
        sys.exit(1)

    team_files = [
        os.path.join(TEAMS_PATH, f)
        for f in os.listdir(TEAMS_PATH)
        if f.endswith('.ndjson')
    ]

    if not team_files:
        logging.warning(f"No .ndjson files found in: {TEAMS_PATH}")
        return None

    logging.info(f"Found {len(team_files)} team file(s)")
    teams_df = spark.read.json(team_files)

    df_exploded = teams_df.select(
        col("filters.season").alias("filter_season"),
        col("count").alias("total_teams_in_file"),
        col("competition.id").alias("competition_id"),
        col("competition.name").alias("competition_name"),
        col("competition.code").alias("competition_code"),
        col("competition.type").alias("competition_type"),
        col("competition.emblem").alias("competition_emblem"),
        col("season.id").alias("season_id"),
        col("season.startDate").alias("season_start_date"),
        col("season.endDate").alias("season_end_date"),
        col("season.currentMatchday").alias("season_current_matchday"),
        col("season.winner").alias("season_winner"),
        explode("teams").alias("team")
    )

    df_flat = df_exploded.select(
        "*",
        col("team.id").alias("team_id"),
        col("team.name").alias("team_name"),
        col("team.shortName").alias("team_short_name"),
        col("team.tla").alias("team_tla"),
        col("team.founded").alias("team_founded"),
        col("team.clubColors").alias("team_colors"),
        col("team.venue").alias("team_venue"),
        col("team.address").alias("team_address"),
        col("team.website").alias("team_website"),
        col("team.crest").alias("team_crest"),
        col("team.lastUpdated").alias("team_last_updated"),
        col("team.area.id").alias("area_id"),
        col("team.area.name").alias("area_name"),
        col("team.area.code").alias("area_code"),
        col("team.area.flag").alias("area_flag"),
        col("team.coach.id").alias("coach_id"),
        col("team.coach.name").alias("coach_name"),
        col("team.coach.firstName").alias("coach_first_name"),
        col("team.coach.lastName").alias("coach_last_name"),
        col("team.coach.dateOfBirth").alias("coach_dob"),
        col("team.coach.nationality").alias("coach_nationality"),
        col("team.coach.contract.start").alias("coach_contract_start"),
        col("team.coach.contract.until").alias("coach_contract_end")
    ).drop("team")

    logging.info(
        f"Flattened teams: {df_flat.count()} rows | {len(df_flat.columns)} columns")
    return df_flat


def flatten_venues_data(spark):
    section("Flattening venues data")

    if not os.path.isdir(VENUES_PATH):
        logging.error(f"Venues folder not found: {VENUES_PATH}")
        return None

    venue_files = [
        os.path.join(VENUES_PATH, f)
        for f in os.listdir(VENUES_PATH)
        if f.endswith('.ndjson')
    ]

    if not venue_files:
        logging.warning(f"No .ndjson files found in: {VENUES_PATH}")
        return None

    logging.info(f"Found {len(venue_files)} venue file(s)")
    venues_df = spark.read.json(venue_files)

    df_flat = venues_df.select(
        col("id").alias("venue_id"),
        col("name").alias("venue_name"),
        col("address").alias("venue_address"),
        col("city").alias("venue_city"),
        col("capacity").alias("venue_capacity"),
        col("surface").alias("venue_surface"),
        col("image").alias("venue_image")
    )

    logging.info(
        f"Flattened venues: {df_flat.count()} rows | {len(df_flat.columns)} columns")
    return df_flat


def write_to_silver(df, output_path):
    section("Writing data to Silver layer")
    if df is None:
        logging.warning(f"No data to write to {output_path}")
        return
    try:
        df = df.withColumn("ingested_at", current_timestamp())
        df.write.mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(output_path)
        logging.info(f"Data successfully written to Silver: {output_path}")
    except Exception as e:
        logging.error(f"Failed to write to Silver: {e}", exc_info=True)
        raise


@timed
def main(**context):
    # setup_logging()
    spark = create_spark_session()

    try:
        df_competition = flatten_competition_data(spark)
        if df_competition:
            write_to_silver(df_competition, SILVER_COMPETITION_PATH)

        df_matches = flatten_matches_data(spark)
        if df_matches:
            write_to_silver(df_matches, SILVER_MATCHES_PATH)

        df_players = flatten_player_data(spark)
        if df_players:
            write_to_silver(df_players, SILVER_PLAYERS_PATH)

        df_standings = flatten_standings_data(spark)
        if df_standings:
            write_to_silver(df_standings, SILVER_STANDINGS_PATH)

        df_teams = flatten_teams_data(spark)
        if df_teams:
            write_to_silver(df_teams, SILVER_TEAMS_PATH)

        df_venues = flatten_venues_data(spark)
        if df_venues:
            write_to_silver(df_venues, SILVER_VENUES_PATH)

        logging.info("✅ Transformation completed successfully!")

    except Exception as e:
        logging.error(f"Error during transformation: {e}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logging.info("🔌 Spark session stopped.")


if __name__ == "__main__":
    main()

__all__ = ["main"]
