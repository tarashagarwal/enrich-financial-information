import pandas as pd
import requests
import os
from dotenv import load_dotenv
import finnhub
import csv
import random
import time

# ----------------------------------------
# Step 1: Fetch S&P 500 company list
# ----------------------------------------

url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/123.0 Safari/537.36"}

# Fetch HTML and parse tables
html = requests.get(url, headers=headers).text
tables = pd.read_html(html)
sp500 = tables[0]

# Clean column names
sp500.columns = [c.strip() for c in sp500.columns]

# Identify the correct columns for Symbol and Name
symbol_col = "Symbol" if "Symbol" in sp500.columns else "Ticker symbol"
name_col = "Security" if "Security" in sp500.columns else sp500.columns[1]

# Use the top 100 companies
companies_df = sp500[[name_col, symbol_col]].head(100)
companies_df.columns = ["Name", "Symbol"]

print("Fetched", len(companies_df), "companies from Wikipedia")
print(companies_df.head())

# ----------------------------------------
# Step 2: Load Finnhub API key
# ----------------------------------------
load_dotenv()
API_KEY = os.getenv("FINNHUB_API_KEY")

if not API_KEY:
    raise ValueError("FINNHUB_API_KEY not found in .env file")

finnhub_client = finnhub.Client(api_key=API_KEY)

# ----------------------------------------
# Step 3: Prepare output file
# ----------------------------------------
os.makedirs("output", exist_ok=True)
output_path = "output/top100_company_market_value.csv"

with open(output_path, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["Name", "Symbol", "Price", "# of Shares", "Market Value"]
    )
    writer.writeheader()

# ----------------------------------------
# Step 4: Fetch company data and build dataset
# ----------------------------------------

MAX_CALLS_PER_MIN = 60 #free veriosn of the API limit 60 calls per minute
COOLDOWN = 70
count = 0

for _, row in companies_df.iterrows():
    name = row["Name"]
    symbol = row["Symbol"]
    count += 1

    try:
        profile = finnhub_client.company_profile2(symbol=symbol)
        quote = finnhub_client.quote(symbol)

        price = quote.get("c", None)
        shares_outstanding = profile.get("shareOutstanding", "")
        market_cap = profile.get("marketCapitalization", "")

        data_row = {
            "Name": name,
            "Symbol": symbol,
            "Price": price,
            "# of Shares": shares_outstanding,
            "Market Value": market_cap
        }

        # Randomly drop some data, but keep at least one of Name or Symbol
        drop_name = random.random() < 0.2
        drop_symbol = random.random() < 0.2
        if drop_name and drop_symbol:
            if random.choice([True, False]):
                drop_name = False
            else:
                drop_symbol = False
        if drop_name:
            data_row["Name"] = ""
        if drop_symbol:
            data_row["Symbol"] = ""

        for key in ["Price", "# of Shares", "Market Value"]:
            if random.random() < 0.3:
                data_row[key] = ""

        with open(output_path, "a", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["Name", "Symbol", "Price", "# of Shares", "Market Value"]
            )
            writer.writerow(data_row)

        print("Processed:", symbol)
        time.sleep(1.2)

    except finnhub.FinnhubAPIException as e:
        if "429" in str(e):
            print("API limit reached. Cooling down for", COOLDOWN, "seconds.")
            time.sleep(COOLDOWN)
            continue
        else:
            print("Finnhub error for", symbol, ":", e)
    except Exception as e:
        print("General error for", symbol, ":", e)

    if count % MAX_CALLS_PER_MIN == 0:
        print("Processed", count, "companies. Cooling down for", COOLDOWN, "seconds.")
        time.sleep(COOLDOWN)

print("All data written to:", output_path)
