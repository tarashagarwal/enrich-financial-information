# Enrich Financial Information

This project enriches financial data using the [Finnhub API](https://finnhub.io/) by fetching company details such as market cap, industry, and related information.  
It provides both synchronous and parallel (threaded / async) scripts to handle API rate limits efficiently.

---

## 📋 Prerequisites

- Python 3.x  
- pip3  
- A valid **Finnhub API Key**

---

## ⚙️ Setup Instructions

### 1. Create a Virtual Environment

```bash
python3 -m venv .venv
```

### 2. Activate the Environment

```bash
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip3 install -r requirements.txt
```

### 4. Add Environment Variables

Create a `.env` file in the root directory:

```
FINNHUB_API_KEY=<your_api_key_here>
```

---

## 🧾 How to Run

### Step 1: Fetch Initial Data

This will download the top 100 S&P 500 companies and store them with some missing values.

```bash
python3 fetch_data.py
```

**Result:**  
`output/top100_company_market_value.csv`

---

### Step 2: Prepare Input

Copy the generated file into the `input` folder and rename it to:

```
input/data.csv
```

---

### Step 3: Enrich the Data

Run the enrichment script (waits ~1 second between each API call to respect the rate limit):

```bash
python3 enrich_data.py
```

You can also run the threaded version:

```bash
python3 enrich_data_parallel.py
```

> Note: `enrich_data_async.py` exists for asynchronous processing, but currently requires fixes and is subject to rate-limit issues with Finnhub’s blocking SDK.

---

## 📂 Output

The enriched data will overwrite the file at:

```
input/data.csv
```

Error logs (e.g., failed API calls or invalid rows) will appear in:

```
output/error.log
```

---

## 🧩 File Overview

| File | Description |
|------|--------------|
| **fetch_data.py** | Fetches top 100 S&P companies from Wikipedia and saves them to the output folder. |
| **enrich_data.py** | Reads data from `input/data.csv`, enriches missing values using the Finnhub API, and logs errors. |
| **enrich_data_parallel.py** | Multi-threaded version of `enrich_data.py` for faster processing while staying within API rate limits. |
| **enrich_data_async.py** | Asynchronous version (under development); currently limited by blocking Finnhub SDK calls. |

---

## ⚠️ Notes

- Finnhub allows **~60 requests per minute** → approx **1 request per second**.  
- Queries (like company names or descriptions) must be **≤ 20 characters**, or Finnhub will return an error.  
- All logs and intermediate data are stored in the `output` folder.

---

## 🧠 Example Flow

```text
fetch_data.py ─▶ output/top100_company_market_value.csv
        ↓
Copy to input/data.csv
        ↓
enrich_data.py or enrich_data_parallel.py ─▶ Updates data.csv with missing fields
```

---

## 🪶 Author

Developed by **Tarash Agarwal**  
For educational and research purposes only.