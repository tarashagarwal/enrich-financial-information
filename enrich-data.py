# Step 1: Read data from the CSV file.
# Load all records into memory (e.g., using pandas or csv module).

# Step 2: Validate missing values.
# If both 'company name' and 'symbol' are missing, log an error and skip that row.

# Step 3: Enrich data using Finnhub API.
# Use either the company name or symbol to fetch additional company details from Finnhub.
# Update the record with the new data (e.g., industry, market cap, etc.).

# Step 4: Save updated records.
# Write enriched data to a temporary CSV file for review or further processing.
# Log any failed rows separately for debugging.


#############################################################################

import os
from dotenv import load_dotenv
import pandas as pd
import logging
from datetime import datetime

load_dotenv()


os.makedirs("output", exist_ok=True)
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/error.log'),
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger(__name__)

INPUT_FILE = os.getenv("INPUT_FILE") or "input/data.csv"

if not INPUT_FILE or not os.path.exists(INPUT_FILE):
    error_msg = f"Input file not found at: {INPUT_FILE}"
    logger.error(error_msg)
    raise FileNotFoundError(error_msg)

print(f"Using input file: {INPUT_FILE}")


expected_columns = ["Name", "Symbol", "Price", "# of Shares", "Market Value"]
# Load the CSV
data = pd.read_csv(INPUT_FILE)
# Validate column names
if not all(col in data.columns for col in expected_columns):
    error_msg = f"Input file must contain columns: {expected_columns}"
    logger.error(error_msg)
    raise ValueError(error_msg)

print("\n--- Preview of Input Data ---")
print(data.head(), "\n")


for idx, row in data.iterrows():
    name   = str(row["Name"]).strip() if not pd.isna(row["Name"]) else ""
    symbol = str(row["Symbol"]).strip() if not pd.isna(row["Symbol"]) else ""
    # If both missing, log error and skip

    if not name and not symbol:
        error_msg = f"Row {idx+1} is missing key feature, cannot fetch data"
        logger.error(error_msg)
        print(error_msg)
        continue

    # Otherwise, continue with processing (you can add your Finnhub logic here)
    print(f"Processing row {idx+1}: Name='{name}', Symbol='{symbol}'")
        
    try:
        print(f"Processing row {idx+1}: Name='{name}', Symbol='{symbol}'")
    # Add your API calls and data processing logic here
    # Example: finnhub_client.company_profile2(symbol=symbol)
    except Exception as e:
        error_msg = f"Error processing row {idx+1} (Name='{name}', Symbol='{symbol}'): {str(e)}"
        logger.error(error_msg)
        print(f"Error: {error_msg}")
    continue