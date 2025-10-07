import pandas as pd
import requests
import os
from dotenv import load_dotenv
import finnhub
import csv

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