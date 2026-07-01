import logging
import aiohttp
import asyncio
import json
import os
import signal
from datetime import date, timedelta
from elt.logger import section, setup_logging, timed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
setup_logging()

# CONFIGURATION
COMPETITION_CODE = os.getenv("COMPETITION_CODE", "WC")
SEASON = int(os.getenv("SEASON", "2026"))
DAYS_LOOKBACK = int(os.getenv("DAYS_LOOKBACK", "14"))
DAYS_LOOKAHEAD = int(os.getenv("DAYS_LOOKAHEAD", "14"))
BASE_DELAY = float(os.getenv("BASE_DELAY", "7"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze")
BASE_URL = os.getenv("BASE_URL", "https://api.football-data.org/v4")
TOKEN = os.getenv("TOKEN")

if not TOKEN:
    raise ValueError("API token not found in environment variables")

HEADERS = {'X-Auth-Token': TOKEN}

PATHS = {
    "competitions": os.path.join(BRONZE_PATH, "competitions"),
    "teams": os.path.join(BRONZE_PATH, "teams"),
    "players": os.path.join(BRONZE_PATH, "players"),
    "standings": os.path.join(BRONZE_PATH, "standings"),
    "venues": os.path.join(BRONZE_PATH, "venues"),
    "matches": os.path.join(BRONZE_PATH, "matches")
}

# Graceful shutdown support
shutdown_event = asyncio.Event()


def handle_shutdown_signal(signum, frame):
    logging.warning(f"Received shutdown signal - finishing current tasks...")
    shutdown_event.set()


signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)


def ensure_directories():
    section("Ensuring Bronze Directories Exist")
    for path in PATHS.values():
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            logging.info(f"Created directory: {path}")
        else:
            logging.info(f"Directory already exists: {path}")


# Helper to write NDJSON
def write_as_ndjson(data, file_path):
    """Convert JSON data to NDJSON format and write atomically."""
    temp_path = f"{file_path}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            # If it's a list → write each object on its own line
            if isinstance(data, list):
                for item in data:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
            # If it's a dict/object → wrap as single line
            elif isinstance(data, dict):
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
            else:
                raise ValueError("Unsupported data type for NDJSON")
        os.replace(temp_path, file_path)
        return True
    except Exception as write_err:
        logging.error(f"Failed to write NDJSON to {file_path}: {write_err}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False


semaphore = asyncio.Semaphore(1)


async def safe_fetch(uri, file_path, description):
    if shutdown_event.is_set():
        logging.info(f"Shutdown requested - skipping: {description}")
        return None

    if os.path.exists(file_path):
        logging.info(f"{description} already exists - skipping")
        return None

    async with semaphore:
        for attempt in range(MAX_RETRIES):
            if shutdown_event.is_set():
                break

            try:
                async with aiohttp.ClientSession() as temp_session:
                    async with temp_session.get(uri, headers=HEADERS, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()

                            # Only save if actual data exists
                            save_data = False
                            if "matches" in data:
                                if data.get("resultSet", {}).get("count", 0) > 0:
                                    save_data = True
                            else:
                                if data and (not isinstance(data, list) or len(data) > 0):
                                    save_data = True

                            if save_data:
                                # Use NDJSON writer instead of json.dump
                                success = write_as_ndjson(data, file_path)
                                if success:
                                    logging.info(f"Saved {description}")
                            else:
                                logging.info(
                                    f"No data found for {description}")
                            return data

                        elif response.status == 401:
                            logging.error(
                                f"Invalid or expired token for {description} - stopping")
                            shutdown_event.set()
                            return None
                        elif response.status == 403:
                            logging.warning(
                                f"Access forbidden for {description} (restricted endpoint/tier)")
                            return None
                        elif response.status == 429:
                            wait = BASE_DELAY * (2 ** (attempt + 1))
                            logging.warning(
                                f"Rate limited for {description} | Attempt {attempt+1}/{MAX_RETRIES} - waiting {wait}s")
                            await asyncio.sleep(wait)
                        else:
                            logging.error(
                                f"HTTP {response.status} for {description}")
                            await asyncio.sleep(BASE_DELAY)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                wait = BASE_DELAY * (2 ** (attempt + 1))
                logging.error(
                    f"Network error for {description} | Attempt {attempt+1}: {e} - waiting {wait}s")
                await asyncio.sleep(wait)

        logging.error(
            f"FAILED COMPLETELY: {description} after {MAX_RETRIES} attempts")
        return None


async def fetch_competitions():
    section("Fetching Competition Details")
    uri = f"{BASE_URL}/competitions/{COMPETITION_CODE}"
    file_path = os.path.join(
        PATHS["competitions"], f"{COMPETITION_CODE}.ndjson")
    await safe_fetch(uri, file_path, f"Competition {COMPETITION_CODE}")


async def fetch_teams():
    section("Fetching Teams")
    uri = f"{BASE_URL}/competitions/{COMPETITION_CODE}/teams"
    file_path = os.path.join(
        PATHS["teams"], f"{COMPETITION_CODE}_teams.ndjson")
    await safe_fetch(uri, file_path, f"Teams for {COMPETITION_CODE}")


async def fetch_players():
    section("Fetching Players")
    if shutdown_event.is_set():
        return

    teams_file = os.path.join(
        PATHS["teams"], f"{COMPETITION_CODE}_teams.ndjson")
    if not os.path.exists(teams_file):
        logging.warning("Teams file not found - run fetch_teams first")
        return

    # Read NDJSON back as list for processing
    teams_data = []
    try:
        with open(teams_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    teams_data.append(json.loads(line))
    except Exception as e:
        logging.error(f"Could not read teams NDJSON: {e}")
        return

    if not teams_data or "teams" not in teams_data[0]:
        logging.warning("No teams found in teams file")
        return

    for team in teams_data[0]["teams"]:
        if shutdown_event.is_set():
            break
        team_id = team["id"]
        team_name = team["name"].replace(" ", "_")
        uri = f"{BASE_URL}/teams/{team_id}"
        file_path = os.path.join(
            PATHS["players"], f"{team_id}_{team_name}.ndjson")
        await safe_fetch(uri, file_path, f"Players for {team_name}")
        await asyncio.sleep(BASE_DELAY)


async def fetch_standings():
    section("Fetching Standings")
    uri = f"{BASE_URL}/competitions/{COMPETITION_CODE}/standings?season={SEASON}"
    file_path = os.path.join(
        PATHS["standings"], f"{COMPETITION_CODE}_{SEASON}_standings.ndjson")
    await safe_fetch(uri, file_path, f"Standings {COMPETITION_CODE} {SEASON}")


async def fetch_matches():
    section("Fetching Matches")
    if shutdown_event.is_set():
        return

    start_date = date.today() - timedelta(days=DAYS_LOOKBACK)
    end_date = date.today() + timedelta(days=DAYS_LOOKAHEAD)
    current = start_date

    connector = aiohttp.TCPConnector(limit=1, force_close=True)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        while current <= end_date and not shutdown_event.is_set():
            date_str = current.isoformat()
            uri = f"{BASE_URL}/matches?dateFrom={date_str}&dateTo={date_str}"
            file_path = os.path.join(PATHS["matches"], f"{date_str}.ndjson")
            await safe_fetch(uri, file_path, f"Matches {date_str}")
            await asyncio.sleep(BASE_DELAY)
            current += timedelta(days=1)


async def fetch_venues():
    section("Fetching Venues")
    if shutdown_event.is_set():
        return

    start_date = date.today() - timedelta(days=DAYS_LOOKBACK)
    end_date = date.today() + timedelta(days=DAYS_LOOKAHEAD)
    current = start_date
    all_venues = {}

    while current <= end_date and not shutdown_event.is_set():
        date_str = current.isoformat()
        uri = f"{BASE_URL}/matches?dateFrom={date_str}&dateTo={date_str}"
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            try:
                async with session.get(uri, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        if "matches" in data and data["matches"]:
                            for match in data["matches"]:
                                if "venue" in match and match["venue"]:
                                    vid = match["venue"].get("id")
                                    if vid and vid not in all_venues:
                                        all_venues[vid] = match["venue"]
            except Exception as e:
                logging.debug(f"Could not fetch venues for {date_str}: {e}")
        await asyncio.sleep(BASE_DELAY)
        current += timedelta(days=1)

    if all_venues:
        file_path = os.path.join(
            PATHS["venues"], f"{COMPETITION_CODE}_venues.ndjson")
        write_as_ndjson(list(all_venues.values()), file_path)
        logging.info(f"Saved {len(all_venues)} unique venues")
    else:
        logging.info("No venue data found")


async def run_all_extractions():
    await fetch_competitions()
    if shutdown_event.is_set():
        return
    await asyncio.sleep(BASE_DELAY)

    await fetch_teams()
    if shutdown_event.is_set():
        return
    await asyncio.sleep(BASE_DELAY)

    await fetch_standings()
    if shutdown_event.is_set():
        return
    await asyncio.sleep(BASE_DELAY)

    await fetch_matches()
    if shutdown_event.is_set():
        return
    await asyncio.sleep(BASE_DELAY)

    await fetch_venues()
    if shutdown_event.is_set():
        return
    await asyncio.sleep(BASE_DELAY)

    await fetch_players()


def count_all_files():
    section("Counting Extracted Files")
    total = 0
    for name, path in PATHS.items():
        if os.path.exists(path):
            count = len([f for f in os.listdir(path) if f.endswith(".ndjson")])
            logging.info(f"{name.capitalize()}: {count} files")
            total += count
    logging.info(f"Total files in bronze: {total}")


@timed
def main():
    section("Starting Full Extraction Process")
    ensure_directories()
    asyncio.run(run_all_extractions())
    count_all_files()
    if shutdown_event.is_set():
        logging.info("Process stopped by user request")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(
            f"An error occurred during extraction: {e}", exc_info=True)
    finally:
        logging.info(
            "===================== END OF FULL EXTRACTION =====================")
