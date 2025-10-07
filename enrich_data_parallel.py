import os
import logging
from dotenv import load_dotenv
import finnhub
import threading
import time
from queue import Queue, Empty
import pandas as pd
import csv
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
DEV_LOG     = os.getenv("DEV_LOG")     or "output/dev.log"
EXPECTED_COLUMNS = ["Name", "Symbol", "Price", "# of Shares", "Market Value"]

os.makedirs("output", exist_ok=True)

# Configure logging with separate handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Clear any existing handlers
logger.handlers.clear()

# Create formatters
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# File handler for INFO and above (dev.log)
dev_handler = logging.FileHandler(DEV_LOG)
dev_handler.setLevel(logging.INFO)
dev_handler.setFormatter(formatter)

# File handler for ERROR and above (error.log)
error_handler = logging.FileHandler(ERROR_LOG)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)

# Console handler for INFO and above
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(dev_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)

API_KEY = os.getenv("FINNHUB_API_KEY")
if not API_KEY:
    raise ValueError("FINNHUB_API_KEY not found in environment/.env")
finnhub_client = finnhub.Client(api_key=API_KEY)

class MinuteRateLimiter:
    """Simple token bucket refilled once per minute."""
    def __init__(self, max_per_minute: int):
        self.max = max_per_minute
        self.tokens = max_per_minute
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self._stop = False
        self.refill_thread = threading.Thread(target=self._refill_loop, daemon=True)
        self.refill_thread.start()

    def _refill_loop(self):
        while not self._stop:
            time.sleep(60)
            with self.lock:
                self.tokens = self.max
                self.cv.notify_all()

    def acquire(self):
        with self.lock:
            while self.tokens <= 0 and not self._stop:
                self.cv.wait(timeout=0.1)
            if self._stop:
                return False
            self.tokens -= 1
            return True

    def stop(self):
        with self.lock:
            self._stop = True
            self.cv.notify_all()
        self.refill_thread.join(timeout=1)

rate_limiter = MinuteRateLimiter(MAX_CALLS_PER_MIN)

#this is helper method
def sdk_call(fn, *args, **kwargs):
    """Wrap a finnhub SDK call with the global minute limiter."""
    if not rate_limiter.acquire():
        raise RuntimeError("Rate limiter stopped")
    return fn(*args, **kwargs)

    # ---------------------------
# Worker + writer setup
# ---------------------------
todo_q = Queue()#rows in csv to be processed
write_q = Queue()#processed rowsa waitng to be written


def normalize_cell(v):
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s

def enrich_row(row_idx_1based: int, row_dict: dict):
    """Return (updated_row_dict, calls_used)."""
    calls_used = 0
    name   = normalize_cell(row_dict.get("Name"))
    symbol = normalize_cell(row_dict.get("Symbol"))

    price  = row_dict.get("Price")
    shares = row_dict.get("# of Shares")
    mval   = row_dict.get("Market Value")

    price  = "" if (price  is None or (isinstance(price, float)  and pd.isna(price)))  else price
    shares = "" if (shares is None or (isinstance(shares, float) and pd.isna(shares))) else shares
    mval   = "" if (mval   is None or (isinstance(mval, float)   and pd.isna(mval)))   else mval

    if not name and not symbol:
        logger.error(f"Row {row_idx_1based} is missing key feature, cannot fetch data")
        return {
            "Name": row_dict.get("Name", ""), "Symbol": row_dict.get("Symbol", ""),
            "Price": row_dict.get("Price", ""), "# of Shares": row_dict.get("# of Shares", ""),
            "Market Value": row_dict.get("Market Value", "")
        }, calls_used

    # If Symbol missing but Name present: lookup symbol
    if not symbol and name:
        try:
            lookup = sdk_call(finnhub_client.symbol_lookup, name)
            calls_used += 1
            if lookup and lookup.get("count", 0) > 0:
                symbol = normalize_cell(lookup["result"][0].get("symbol"))
                logger.info(f"Row {row_idx_1based}: Found symbol '{symbol}' for '{name}'.")
            else:
                logger.warning(f"Row {row_idx_1based}: No symbol found for '{name}'. Leaving row as-is.")
                return {
                    "Name": name, "Symbol": "", "Price": price,
                    "# of Shares": shares, "Market Value": mval
                }, calls_used
        except Exception as e:
            logger.error(f"Row {row_idx_1based}: symbol_lookup error for name='{name}': {e}")
            return {
                "Name": name, "Symbol": "", "Price": price,
                "# of Shares": shares, "Market Value": mval
            }, calls_used

    # If Name missing but Symbol present: get profile to fill name
    profile = None
    if not name and symbol:
        try:
            profile = sdk_call(finnhub_client.company_profile2, symbol=symbol)
            calls_used += 1
            name = normalize_cell(profile.get("name", "")) or name
            if name:
                logger.info(f"Row {row_idx_1based}: Found name '{name}' for symbol '{symbol}'.")
            else:
                logger.warning(f"Row {row_idx_1based}: No name found for symbol '{symbol}'. Leaving row as-is.")
                return {
                    "Name": "", "Symbol": symbol, "Price": price,
                    "# of Shares": shares, "Market Value": mval
                }, calls_used
        except Exception as e:
            logger.error(f"Row {row_idx_1based}: company_profile2 error for '{symbol}': {e}")
            return {
                "Name": "", "Symbol": symbol, "Price": price,
                "# of Shares": shares, "Market Value": mval
            }, calls_used

    # Fetch profile + quote to fill missing values
    try:
        if profile is None:
            profile = sdk_call(finnhub_client.company_profile2, symbol=symbol)
            calls_used += 1
        quote = sdk_call(finnhub_client.quote, symbol)
        calls_used += 1

        if price == "":
            price = quote.get("c", "")
        if shares == "":
            shares = profile.get("shareOutstanding", "")
        if mval == "":
            mval = profile.get("marketCapitalization", "")
    except Exception as e:
        logger.error(f"Row {row_idx_1based}: fetch profile/quote error for '{symbol}': {e}")

    return {
        "Name": name, "Symbol": symbol, "Price": price,
        "# of Shares": shares, "Market Value": mval
    }, calls_used

def worker_fn(worker_id: int):
    used_this_minute = 0
    while True:
        try:
            row_idx, row_dict = todo_q.get(timeout=0.2)
        except Empty:
            return
        updated, calls_used = enrich_row(row_idx, row_dict)
        write_q.put(updated)
        used_this_minute += calls_used

        # Per-worker quota enforcement
        if used_this_minute >= PER_WORKER_CALLS_PER_MIN:
            logger.info(f"Worker {worker_id}: used {used_this_minute} calls. Cooling down {WORKER_COOLDOWN_SECS}s.")
            time.sleep(WORKER_COOLDOWN_SECS)
            used_this_minute = 0

        todo_q.task_done()

def writer_fn(temp_file: str, header: list):
    with open(temp_file, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        while True:
            item = write_q.get()
            if item is None:  # sentinel
                write_q.task_done()
                break
            w.writerow(item)
            write_q.task_done()

def main():
    # Validate input CSV
    if not INPUT_FILE or not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Input file not found at: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    logger.info(f"Using input file: {INPUT_FILE}")
    logger.info(f"Global cap: {MAX_CALLS_PER_MIN}/min; Workers: {NUM_WORKERS}; Per-worker cap: {PER_WORKER_CALLS_PER_MIN}/min")

    # Fill work queue
    for i, row in df.iterrows():
        todo_q.put((i + 1, row.to_dict()))

    # Start writer
    writer_th = threading.Thread(target=writer_fn, args=(TEMP_FILE, EXPECTED_COLUMNS), daemon=True)
    writer_th.start()

    # Start workers
    workers = [threading.Thread(target=worker_fn, args=(wid,), daemon=True)
               for wid in range(1, NUM_WORKERS + 1)]
    for t in workers:
        t.start()

    # Wait for completion
    todo_q.join()
    write_q.put(None)  # stop writer
    write_q.join()
    writer_th.join(timeout=1)

    # Stop limiter and replace file
    rate_limiter.stop()
    os.replace(TEMP_FILE, INPUT_FILE)
    logger.info(f"Incremental update complete. Updated file saved at: {INPUT_FILE}")

if __name__ == "__main__":
    main()

