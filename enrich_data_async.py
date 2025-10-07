import os
import csv
import time
import math
import signal
import logging
import threading
from queue import Queue, Empty
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
import finnhub

# ==========================
# CONFIG (env-overridable)
# ==========================
load_dotenv()

MAX_CALLS_PER_MIN        = int(os.getenv("MAX_CALLS_PER_MIN", "60"))  # Finnhub free tier ~60/min
NUM_WORKERS              = int(os.getenv("NUM_WORKERS", "5"))
SHORT_SLEEP_SECS         = float(os.getenv("SHORT_SLEEP_SECS", "0.25"))  # worker retry when tokens unavailable
LOOKUP_NAME_MAXLEN       = int(os.getenv("LOOKUP_NAME_MAXLEN", "64"))    # avoid "q too long" (422)
INPUT_FILE               = os.getenv("INPUT_FILE") or "input/data.csv"
TEMP_FILE                = os.getenv("TEMP_FILE")  or "output/data_tmp.csv"
OUTPUT_DIR               = os.getenv("OUTPUT_DIR") or "output"
ERROR_LOG                = os.getenv("ERROR_LOG")  or os.path.join(OUTPUT_DIR, "error.log")
DEV_LOG                  = os.getenv("DEV_LOG")    or os.path.join(OUTPUT_DIR, "dev.log")

EXPECTED_COLUMNS         = ["Name", "Symbol", "Price", "# of Shares", "Market Value"]

API_KEY                  = os.getenv("FINNHUB_API_KEY")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================
# LOGGING
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
    handlers=[
        logging.FileHandler(ERROR_LOG),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Separate lightweight dev logger if you want extra trace
dev_handler = logging.FileHandler(DEV_LOG)
dev_handler.setLevel(logging.DEBUG)
dev_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s - %(message)s"))
dev_log = logging.getLogger("dev")
dev_log.setLevel(logging.DEBUG)
dev_log.addHandler(dev_handler)

# ==========================
# VALIDATE ENV / INPUTS
# ==========================
if not API_KEY:
    raise ValueError("FINNHUB_API_KEY not found in .env file")

if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"Input file not found at: {INPUT_FILE}")

# ==========================
# FINNHUB CLIENT
# ==========================
finnhub_client = finnhub.Client(api_key=API_KEY)

# ==========================
# RATE LIMITER (token bucket)
# ==========================
class MinuteRateLimiter:
    """
    Global token bucket that refills once per minute.
    try_acquire() is non-blocking: returns False if no tokens, so workers can sleep
    without blocking each other.
    """
    def __init__(self, max_per_minute: int):
        self.max = max_per_minute
        self.tokens = max_per_minute
        self.lock = threading.Lock()
        self._stop = False
        self.refill_thread = threading.Thread(target=self._refill_loop, name="RefillThread", daemon=True)
        self.refill_thread.start()

    def _refill_loop(self):
        while not self._stop:
            time.sleep(60.0)
            with self.lock:
                self.tokens = self.max

    def try_acquire(self, n: int = 1) -> bool:
        with self.lock:
            if self.tokens >= n:
                self.tokens -= n
                return True
            return False

    def stop(self):
        self._stop = True

rate_limiter = MinuteRateLimiter(MAX_CALLS_PER_MIN)

# ==========================
# IO QUEUES
# ==========================
task_q = Queue()   # rows to process
write_q = Queue()  # enriched rows to write

# ==========================
# HELPERS
# ==========================
def safe_get(d: dict, key: str, default=""):
    try:
        return d.get(key, default)
    except Exception:
        return default

def truncate_query(q: str, maxlen: int) -> str:
    q = q.strip()
    if len(q) <= maxlen:
        return q
    return q[:maxlen]

def acquire_or_wait(calls_needed: int = 1):
    """Non-blocking limiter: if not enough tokens, sleep a bit and retry."""
    while True:
        if rate_limiter.try_acquire(calls_needed):
            return
        time.sleep(SHORT_SLEEP_SECS)

def lookup_symbol_by_name(name: str) -> str:
    """
    Use symbol_lookup with a truncated query to avoid 422 'q too long'.
    Prefer exact match by displaySymbol or description containing the name.
    """
    if not name:
        return ""
    query = truncate_query(name, LOOKUP_NAME_MAXLEN)

    # 1 API call
    acquire_or_wait(1)
    resp = finnhub_client.symbol_lookup(query)
    count = int(resp.get("count", 0))
    if count == 0:
        return ""

    # Simple heuristic: exact match by description or displaySymbol first; else first result.
    results = resp.get("result", []) or []
    lower_name = name.lower()

    # Try exact-ish description match
    for r in results:
        desc = safe_get(r, "description", "")
        if desc and desc.lower() == lower_name:
            return safe_get(r, "symbol", "")

    # Try contains match on description
    for r in results:
        desc = safe_get(r, "description", "")
        if desc and lower_name in desc.lower():
            return safe_get(r, "symbol", "")

    # Fallback to the first result
    return safe_get(results[0], "symbol", "")

def fetch_profile(symbol: str) -> dict:
    # 1 API call
    acquire_or_wait(1)
    return finnhub_client.company_profile2(symbol=symbol) or {}

def fetch_quote(symbol: str) -> dict:
    # 1 API call
    acquire_or_wait(1)
    return finnhub_client.quote(symbol) or {}

def normalize_cell(x):
    if pd.isna(x):
        return ""
    return x

def build_updated_row(name, symbol, price, shares_outstanding, market_value, profile, quote):
    # Fill missing pieces from API responses
    if not price:
        price = safe_get(quote, "c", "")

    if not shares_outstanding:
        shares_outstanding = safe_get(profile, "shareOutstanding", "")

    if not market_value:
        market_value = safe_get(profile, "marketCapitalization", "")

    # If name is still missing, try to pick from profile
    if not name:
        name = safe_get(profile, "name", "")

    return {
        "Name": name,
        "Symbol": symbol,
        "Price": price,
        "# of Shares": shares_outstanding,
        "Market Value": market_value
    }

# ==========================
# WORKER
# ==========================
def worker_loop():
    while True:
        try:
            idx, row_dict = task_q.get(timeout=1.0)
        except Empty:
            # No more tasks right now; loop again
            continue

        try:
            name   = str(normalize_cell(row_dict.get("Name"))).strip()
            symbol = str(normalize_cell(row_dict.get("Symbol"))).strip()
            price  = normalize_cell(row_dict.get("Price"))
            shares = normalize_cell(row_dict.get("# of Shares"))
            mktval = normalize_cell(row_dict.get("Market Value"))

            if not name and not symbol:
                msg = f"Row {idx+1}: both Name and Symbol missing. Skipping."
                log.error(msg)
                task_q.task_done()
                continue

            # Resolve name/symbol if needed
            if not symbol and name:
                try:
                    symbol = lookup_symbol_by_name(name)
                    if not symbol:
                        log.error(f"Row {idx+1}: No symbol found for '{name}'. Skipping.")
                        task_q.task_done()
                        continue
                    dev_log.debug(f"Row {idx+1}: Resolved symbol '{symbol}' from name '{name}'.")
                except finnhub.FinnhubAPIException as e:
                    log.error(f"Row {idx+1}: Lookup failed for '{name}': {e}")
                    task_q.task_done()
                    continue

            # If we have symbol, try to enrich
            if symbol:
                try:
                    profile = fetch_profile(symbol)
                    # If name missing, populate from profile later
                except finnhub.FinnhubAPIException as e:
                    log.error(f"Row {idx+1}: Profile fetch failed for '{symbol}': {e}")
                    task_q.task_done()
                    continue

                try:
                    quote = fetch_quote(symbol)
                except finnhub.FinnhubAPIException as e:
                    log.error(f"Row {idx+1}: Quote fetch failed for '{symbol}': {e}")
                    task_q.task_done()
                    continue

                updated = build_updated_row(name, symbol, price, shares, mktval, profile, quote)
                write_q.put(updated)
                dev_log.debug(f"Row {idx+1}: Enriched and queued for write.")

            else:
                # Shouldn't reach here given checks above
                log.error(f"Row {idx+1}: Missing symbol after resolution. Skipping.")

        except Exception as e:
            log.error(f"Row {idx+1}: Unexpected error: {e}")
        finally:
            task_q.task_done()

# ==========================
# WRITER
# ==========================
def writer_loop(stop_event: threading.Event):
    # Create/overwrite temp with header first
    with open(TEMP_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXPECTED_COLUMNS)
        writer.writeheader()

    # Append rows as workers produce them
    while not stop_event.is_set() or not write_q.empty():
        try:
            item = write_q.get(timeout=0.5)
        except Empty:
            continue
        try:
            with open(TEMP_FILE, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=EXPECTED_COLUMNS)
                writer.writerow(item)
        except Exception as e:
            log.error(f"Writer error: {e}")
        finally:
            write_q.task_done()

# ==========================
# MAIN
# ==========================
def main():
    # Read input CSV
    df = pd.read_csv(INPUT_FILE)
    # Validate columns
    if not all(col in df.columns for col in EXPECTED_COLUMNS):
        raise ValueError(f"Input file must contain columns: {EXPECTED_COLUMNS}")

    print(f"Using input file: {INPUT_FILE}")
    print("\n--- Preview of Input Data ---")
    print(df.head(), "\n")

    # Enqueue tasks
    for idx, row in df.iterrows():
        row_dict = {col: row[col] for col in EXPECTED_COLUMNS}
        task_q.put((idx, row_dict))

    # Start writer thread
    writer_stop = threading.Event()
    writer_thread = threading.Thread(target=writer_loop, args=(writer_stop,), name="Writer", daemon=True)
    writer_thread.start()

    # Start workers
    workers = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=worker_loop, name=f"Worker-{i+1}", daemon=True)
        t.start()
        workers.append(t)

    # Graceful shutdown handler
    def handle_sigint(sig, frame):
        print("\nSIGINT received. Finishing in-flight tasks...")
        # Let workers finish queue; writer drains any remaining rows
        # We won't abruptly stop threads to avoid data loss.
    signal.signal(signal.SIGINT, handle_sigint)

    # Wait until all tasks are processed
    task_q.join()
    # Wait until all writes are flushed
    write_q.join()

    # Stop writer and limiter
    writer_stop.set()
    rate_limiter.stop()

    # Give writer a moment to exit
    writer_thread.join(timeout=2.0)

    # Atomic replace
    os.replace(TEMP_FILE, INPUT_FILE)
    print(f"\nIncremental update complete. Updated file saved at: {INPUT_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Fatal error: {e}")
        raise
