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
import finnhub
import csv
import time
import pdb

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


TEMP_FILE = os.getenv("TEMP_FILE") or "output/data_tmp.csv"

# Load Finnhub API key, please paste API key in .env file at root

API_KEY = os.getenv("FINNHUB_API_KEY")
if not API_KEY:
    raise ValueError("FINNHUB_API_KEY not found in .env file")
finnhub_client = finnhub.Client(api_key=API_KEY)


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


# Create temp file with headers
with open(TEMP_FILE, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=expected_columns)
    writer.writeheader()

for idx, row in data.iterrows():
    name   = str(row["Name"]).strip() if not pd.isna(row["Name"]) else ""
    symbol = str(row["Symbol"]).strip() if not pd.isna(row["Symbol"]) else ""
    price = row["Price"] if not pd.isna(row["Price"]) else ""
    shares_outstanding = row["# of Shares"] if not pd.isna(row["# of Shares"]) else ""
    market_value = row["Market Value"] if not pd.isna(row["Market Value"]) else ""
    # If both missing, log error and skip
    try:
        print(f"Processing row {idx+1}: Name='{name}', Symbol='{symbol}'")
        # Add your API calls and data processing logic here
        # Example: finnhub_client.company_profile2(symbol=symbol)
        #if only name is present
        if not symbol and name:
            lookup = finnhub_client.symbol_lookup(name)
            if lookup["count"] > 0:
                symbol = lookup["result"][0]["symbol"]
                print(f"Found symbol '{symbol}' for company '{name}'.")
            else:
                print(f"No symbol found for '{name}'.")
                continue
        elif not name and symbol:
            profile = finnhub_client.company_profile2(symbol=symbol)
            name = profile.get("name", "")
            if not name:
                print(f"No name found for symbol '{symbol}'.")
                continue
            print(f"Found name '{name}' for symbol '{symbol}'.")
        else:
            # Both name and symbol are present, verify the data
            print(f"Both name and symbol present for '{name}' with symbol '{symbol}'.")

        profile = finnhub_client.company_profile2(symbol=symbol)
        quote = finnhub_client.quote(symbol)

        if not price or price == "":
            price = quote.get("c", "")
            print(f"Row {idx}: Updated price = {price}")

        if not shares_outstanding or shares_outstanding == "":
            shares_outstanding = profile.get("shareOutstanding", "")
            print(f"Row {idx}: Updated shares outstanding = {shares_outstanding}")

        if not market_value or market_value == "":
            market_value = profile.get("marketCapitalization", "")
            print(f"Row {idx}: Updated market value = {market_value}")

        print(f"Row {idx}: ## Updated and written to file.\n")

        time.sleep(1.0) #Free API is throttled , so need to wait a second, we can make 60 calls in a minute

        updated_row = {
            "Name": name,
            "Symbol": symbol,
            "Price": price,
            "# of Shares": shares_outstanding,
            "Market Value": market_value
        }

        # pdb.set_trace();
        with open(TEMP_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=expected_columns)
            writer.writerow(updated_row)

    except Exception as e:
        error_msg = f"Error processing row {idx+1} (Name='{name}', Symbol='{symbol}'): {str(e)}"
        logger.error(error_msg)
        print(f"Error: {error_msg}")
    continue