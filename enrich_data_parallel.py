import os
import logging
from dotenv import load_dotenv
import finnhub
import threading
import time
from queue import Queue, Empty
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