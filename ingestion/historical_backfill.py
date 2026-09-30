import os
from datetime import datetime, timezone

import pandas as pd
import snowflake.connector
import yfinance as yf
from dotenv import load_dotenv


load_dotenv("../.env")


STOCK_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "TSLA",
    "GOOGL",
    "META",
    "NFLX",
    "AMD",
    "INTC",
    "JPM",
    "V",
    "MA",
    "WMT",
    "COST",
]

HISTORY_PERIOD = "1y"


def load_historical_data():
    print("Downloading historical stock data...")

    data = yf.download(
        tickers=STOCK_SYMBOLS,
        period=HISTORY_PERIOD,
        interval="1d",
        auto_adjust=False,
        group_by="ticker",
        progress=False,
        threads=True,
    )

    rows = []

    for symbol in STOCK_SYMBOLS:
        if symbol not in data.columns.get_level_values(0):
            print(f"Warning: no data returned for {symbol}")
            continue

        stock_df = data[symbol].copy()
        stock_df = stock_df.reset_index()

        for _, row in stock_df.iterrows():
            if pd.isna(row["Close"]):
                continue

            trade_date = pd.Timestamp(row["Date"]).date()

            rows.append(
                (
                    symbol,
                    trade_date,
                    float(row["Open"]) if pd.notna(row["Open"]) else None,
                    float(row["High"]) if pd.notna(row["High"]) else None,
                    float(row["Low"]) if pd.notna(row["Low"]) else None,
                    float(row["Close"]) if pd.notna(row["Close"]) else None,
                    int(row["Volume"]) if pd.notna(row["Volume"]) else None,
                    float(row["Dividends"]) if "Dividends" in row and pd.notna(row["Dividends"]) else 0.0,
                    float(row["Stock Splits"]) if "Stock Splits" in row and pd.notna(row["Stock Splits"]) else 0.0,
                    datetime.now(timezone.utc),
                )
            )

    return rows


def create_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STOCK_PRICES_HISTORY (
            SYMBOL VARCHAR(20),
            TRADE_DATE DATE,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME NUMBER,
            DIVIDENDS FLOAT,
            STOCK_SPLITS FLOAT,
            LOADED_AT TIMESTAMP_NTZ
        )
        """
    )


def insert_data(cursor, rows):
    if not rows:
        return

    cursor.executemany(
        """
        INSERT INTO STOCK_PRICES_HISTORY (
            SYMBOL,
            TRADE_DATE,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            DIVIDENDS,
            STOCK_SPLITS,
            LOADED_AT
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def main():
    rows = load_historical_data()

    print(f"Rows prepared: {len(rows)}")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )

    cursor = conn.cursor()

    try:
        create_table(cursor)
        insert_data(cursor, rows)
        conn.commit()

        print("Historical data loaded successfully.")
        print(f"Rows inserted: {len(rows)}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()