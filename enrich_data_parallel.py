import os
import logging
from dotenv import load_dotenv
import finnhub
# ---------------------------
# Config (env-overridable)
# ---------------------------
load_dotenv()

MAX_CALLS_PER_MIN        = int(os.getenv("MAX_CALLS_PER_MIN", "50"))     # global cap
NUM_WORKERS              = int(os.getenv("NUM_WORKERS", "5"))            # threads
WORKER_COOLDOWN_SECS     = int(os.getenv("WORKER_COOLDOWN_SECS", "60"))  # sleep after quota
PER_WORKER_CALLS_PER_MIN = max(1, MAX_CALLS_PER_MIN // max(1, NUM_WORKERS))

INPUT_FILE  = os.getenv("INPUT_FILE") or "output/data.csv"
TEMP_FILE   = os.getenv("TEMP_FILE")  or "output/data_tmp.csv"
ERROR_LOG   = os.getenv("ERROR_LOG")  or "output/error.log"
EXPECTED_COLUMNS = ["Name", "Symbol", "Price", "# of Shares", "Market Value"]

os.makedirs("output", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(ERROR_LOG), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("FINNHUB_API_KEY")
if not API_KEY:
    raise ValueError("FINNHUB_API_KEY not found in environment/.env")
finnhub_client = finnhub.Client(api_key=API_KEY)