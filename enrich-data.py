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

load_dotenv()

INPUT_FILE = os.getenv("INPUT_FILE") or "input/data.csv"

if not INPUT_FILE or not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"Input file not found at: {INPUT_FILE}")
print(f"Using input file: {INPUT_FILE}")


expected_columns = ["Name", "Symbol", "Price", "# of Shares", "Market Value"]
# Load the CSV
data = pd.read_csv(INPUT_FILE)
# Validate column names
if not all(col in data.columns for col in expected_columns):
    raise ValueError(f"Input file must contain columns: {expected_columns}")

print("\n--- Preview of Input Data ---")
print(data.head(), "\n")

