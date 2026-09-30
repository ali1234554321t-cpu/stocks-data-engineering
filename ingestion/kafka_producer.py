import json
import time

from kafka import KafkaProducer

from config import STOCK_SYMBOLS
from ingestion.stock_api import get_stock_quote


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


while True:
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

        producer.send("stock-prices", value=message)

        print(f"Sent: {symbol} | Price: {quote['c']}")

    producer.flush()

    print("Waiting 10 seconds...\n")
    time.sleep(10)
