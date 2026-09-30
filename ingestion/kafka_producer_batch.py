import json
import os
import time

from dotenv import load_dotenv
from kafka import KafkaProducer

from ingestion.stock_api import get_stock_quote


load_dotenv()

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

KAFKA_TOPIC = "stock-prices"

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ROUNDS = 1
WAIT_SECONDS = 10


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


for round_number in range(1, ROUNDS + 1):

    print(f"\n--- Round {round_number}/{ROUNDS} ---")

    for symbol in STOCK_SYMBOLS:

        quote = get_stock_quote(symbol)

        message = {
            "symbol": symbol,
            "price": quote["c"],
            "change": quote["d"],
            "change_percent": quote["dp"],
            "high": quote["h"],
            "low": quote["l"],
            "open": quote["o"],
            "previous_close": quote["pc"],
            "timestamp": quote["t"],
        }

        producer.send(
            KAFKA_TOPIC,
            value=message
        )

        print(
            f"Sent: {symbol} | "
            f"Price: {quote['c']} | "
            f"Change: {quote['d']}"
        )

    producer.flush()

    if round_number < ROUNDS:
        print(f"Waiting {WAIT_SECONDS} seconds...")
        time.sleep(WAIT_SECONDS)


producer.close()

print("\nBatch producer finished successfully.")