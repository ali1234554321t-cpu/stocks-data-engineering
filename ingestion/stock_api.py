import os

import requests
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")

BASE_URL = "https://finnhub.io/api/v1"


def get_stock_quote(symbol):
    url = f"{BASE_URL}/quote"

    params = {
        "symbol": symbol,
        "token": API_KEY,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    return response.json()