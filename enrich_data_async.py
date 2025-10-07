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

BASE_URL = "https://finnhub.io/api/v1"