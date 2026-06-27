import logging
import aiohttp
import asyncio
import json
import os
from datetime import date, timedelta
from elt.logger import section, setup_logging, timed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize logging
setup_logging()

# folder path for storing the extracted data
BRONZE_DIR = "data/bronze"


def ensure_bronze_dir():
    """Ensure the bronze data directory exists."""
    section("Ensuring Bronze Directory Exists")
    if not os.path.exists(BRONZE_DIR):
        os.makedirs(BRONZE_DIR, exist_ok=True)
        logging.info(f"Created bronze data directory: {BRONZE_DIR}")
    else:
        logging.info(f"Bronze data directory already exists: {BRONZE_DIR}")


semaphore = asyncio.Semaphore(1)  # Limit to 1 concurrent request
BASE_DELAY = 7
MAX_RETRIES = 3

# Asynchronous function to fetch Match data from the API


async def fetch_match_data(session, target_date):
    date_str = target_date.isoformat()
    uri = f"https://api.football-data.org/v4/matches?dateFrom={date_str}&dateTo={date_str}"
    headers = {'X-Auth-Token': os.getenv('TOKEN')}
    file_path = os.path.join(BRONZE_DIR, f"{date_str}.json")

    if os.path.exists(file_path):
        logging.info(f"Data for {date_str} already exists. Skipping API call.")
        return None

    try:
        async with semaphore:
            for attempt in range(MAX_RETRIES):
                try:
                    async with session.get(uri, headers=headers, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'matches' in data and data['matches']:
                                with open(file_path, "w", encoding="utf-8") as f:
                                    json.dump(data['matches'], f,
                                              ensure_ascii=False, indent=4)
                                logging.info(f"Successfully saved {date_str}")
                            else:
                                logging.info(
                                    f"No matches found for {date_str}")
                            return data

                        elif response.status == 429:
                            # Exponential backoff: BASE_DELAY * 2^attempt
                            wait = BASE_DELAY * (2 ** (attempt + 1))
                            logging.warning(
                                f"Rate limited for {date_str}. Attempt {attempt+1}/{MAX_RETRIES} — waiting {wait}s")
                            await asyncio.sleep(wait)

                        else:
                            logging.error(
                                f"HTTP {response.status} for {date_str}")
                            await asyncio.sleep(BASE_DELAY)

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    # Exponential backoff for network errors too
                    wait = BASE_DELAY * (2 ** (attempt + 1))
                    logging.error(
                        f"Network error {date_str} attempt {attempt+1}: {e} — waiting {wait}s")
                    await asyncio.sleep(wait)

            logging.error(
                f"FAILED COMPLETELY: {date_str} after {MAX_RETRIES} attempts")
            return None

    except Exception as e:
        logging.error(f"Exception fetching {date_str}: {e}")
        return None

# Controller for Coroutine execution


async def run_extraction():
    section("Starting Data Extraction from API")
    start_date = date.today() - timedelta(days=14)
    end_date = date.today() + timedelta(days=14)
    current = start_date

    connector = aiohttp.TCPConnector(limit=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        while current <= end_date:
            await fetch_match_data(session, current)
            await asyncio.sleep(BASE_DELAY)
            current += timedelta(days=1)

# Count the number of JSON files in the bronze directory


def count_json_files():
    section("Counting Extracted JSON Files")
    json_count = len([f for f in os.listdir(BRONZE_DIR)
                      if os.path.isfile(os.path.join(BRONZE_DIR, f))
                      and f.endswith('.json')])
    logging.info(f"Total JSON files: {json_count}")


@timed
def main():
    section("Starting Extraction Process")
    ensure_bronze_dir()
    asyncio.run(run_extraction())
    count_json_files()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred during extraction: {e}")
    finally:
        logging.info(
            f"""\033[92m===================== END OF EXTRACTION =====================\033[0m""")
