import json
from datetime import datetime, timezone
from pathlib import Path

from config import STOCK_SYMBOLS
from ingestion.stock_api import get_stock_quote


def save_stock_quote(symbol):
    quote = get_stock_quote(symbol)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{symbol}_{timestamp}.json"

    with open(output_file, "w") as file:
        json.dump(quote, file, indent=4)

    print(f"Saved: {output_file}")


if __name__ == "__main__":
    for symbol in STOCK_SYMBOLS:
        save_stock_quote(symbol)