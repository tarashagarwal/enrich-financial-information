import os
import asyncio
import aiohttp
import aiofiles
import pandas as pd
import csv
import logging
from dotenv import load_dotenv
from datetime import datetime

# ===========================
# CONFIGURATION
# ===========================
MAX_CALLS_PER_MINUTE = 50   # total API calls per minute (global)
NUM_WORKERS = 5             # number of async workers
SLEEP_TIME = 60             # seconds each worker sleeps after finishing batch
# ===========================

load_dotenv()

os.makedirs("output", exist_ok=True)
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("output/error.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("FINNHUB_API_KEY")
INPUT_FILE = os.getenv("INPUT_FILE") or "output/data.csv"
TEMP_FILE = os.getenv("TEMP_FILE") or "output/data_tmp.csv"

if not API_KEY:
    raise ValueError("FINNHUB_API_KEY not found in .env file")

if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

expected_columns = ["Name", "Symbol", "Price", "# of Shares", "Market Value"]
data = pd.read_csv(INPUT_FILE)

if not all(col in data.columns for col in expected_columns):
    raise ValueError(f"Input file must contain columns: {expected_columns}")

print(f"Using input file: {INPUT_FILE}")
print("\n--- Preview of Input Data ---")
print(data.head(), "\n")

# Create temp file with headers
with open(TEMP_FILE, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=expected_columns)
    writer.writeheader()

#We will use API here bcoz Library is blocking.
BASE_URL = "https://finnhub.io/api/v1"


# ===========================
# ASYNC FINNHUB HELPERS
# ===========================
async def fetch_json(session, url, params):
    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise Exception(f"API error {resp.status}: {text}")
        return await resp.json()

async def get_symbol_lookup(session, name):
    url = f"{BASE_URL}/search"
    params = {"q": name, "token": API_KEY}
    return await fetch_json(session, url, params)

async def get_company_profile(session, symbol):
    url = f"{BASE_URL}/stock/profile2"
    params = {"symbol": symbol, "token": API_KEY}
    return await fetch_json(session, url, params)

async def get_quote(session, symbol):
    url = f"{BASE_URL}/quote"
    params = {"symbol": symbol, "token": API_KEY}
    return await fetch_json(session, url, params)

# ===========================
# ASYNC WORKER
# ===========================
async def worker(worker_name, queue, session, semaphore):
    while True:
        try:
            idx, row = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        async with semaphore:  # rate-limited block
            try:
                name_val = str(row["Name"]).strip() if not pd.isna(row["Name"]) else ""
                symbol = str(row["Symbol"]).strip() if not pd.isna(row["Symbol"]) else ""

                if not name_val and not symbol:
                    logger.error(f"[{worker_name}] Row {idx+1}: Missing key fields.")
                    continue

                print(f"[{worker_name}] Processing row {idx+1}...")

                # Lookup missing symbol or name
                if not symbol and name_val:
                    lookup = await get_symbol_lookup(session, name_val)
                    if lookup.get("count", 0) > 0:
                        symbol = lookup["result"][0]["symbol"]
                        print(f"[{worker_name}] Found symbol '{symbol}' for company '{name_val}'.")
                    else:
                        print(f"[{worker_name}] No symbol found for '{name_val}'.")
                        continue
                elif not name_val and symbol:
                    profile = await get_company_profile(session, symbol)
                    name_val = profile.get("name", "")
                    if not name_val:
                        print(f"[{worker_name}] No name found for symbol '{symbol}'.")
                        continue

                # Fetch main data
                profile = await get_company_profile(session, symbol)
                quote = await get_quote(session, symbol)

                price = row["Price"] if not pd.isna(row["Price"]) else quote.get("c", "")
                shares = row["# of Shares"] if not pd.isna(row["# of Shares"]) else profile.get("shareOutstanding", "")
                market_value = row["Market Value"] if not pd.isna(row["Market Value"]) else profile.get("marketCapitalization", "")

                updated_row = {
                    "Name": name_val,
                    "Symbol": symbol,
                    "Price": price,
                    "# of Shares": shares,
                    "Market Value": market_value,
                }

                async with aiofiles.open(TEMP_FILE, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=expected_columns)
                    await asyncio.get_event_loop().run_in_executor(None, writer.writerow, updated_row)

                print(f"[{worker_name}] ✅ Row {idx+1} done.")
            except Exception as e:
                logger.error(f"[{worker_name}] Row {idx+1} error: {e}")
            finally:
                # ensure pacing between calls
                await asyncio.sleep(60 / MAX_CALLS_PER_MINUTE)

    print(f"[{worker_name}] Sleeping {SLEEP_TIME}s after batch...")
    await asyncio.sleep(SLEEP_TIME)