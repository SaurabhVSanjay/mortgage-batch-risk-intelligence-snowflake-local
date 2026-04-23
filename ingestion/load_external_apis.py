import os
from datetime import date
import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="RAW",
    )

def load_fx(cur):
    url = os.getenv("FX_API_URL")
    data = requests.get(url, timeout=30).json()
    rates = data.get("rates", {})
    rows = [("EUR", ccy, date.today().
    (), float(v)) for ccy, v in rates.items() if ccy in ["USD", "GBP", "JPY"]]
    cur.executemany(
        "INSERT INTO raw_interest_rates(rate_date, rate_name, rate_value) VALUES (%s,%s,%s)",
        [(r[2], f"FX_EUR_{r[1]}", r[3]) for r in rows]
    )
    return len(rows)

def load_macro(cur):
    fred_key = os.getenv("FRED_API_KEY")
    if not fred_key:
        return 0
    series = "UNRATE"
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={fred_key}&file_type=json&sort_order=desc&limit=1"
    data = requests.get(url, timeout=30).json()
    obs = data.get("observations", [])
    if not obs:
        return 0
    latest = obs[0]
    cur.execute(
        "INSERT INTO raw_macro_indicators(indicator_date, indicator_name, indicator_value) VALUES (%s,%s,%s)",
        (latest["date"], "UNEMPLOYMENT_RATE", float(latest["value"]))
    )
    return 1

if __name__ == "__main__":
    conn = get_conn()
    cur = conn.cursor()
    try:
        fx_n = load_fx(cur)
        macro_n = load_macro(cur)
        conn.commit()
        print(f"Loaded FX rows: {fx_n}, Macro rows: {macro_n}")
    finally:
        cur.close()
        conn.close()
