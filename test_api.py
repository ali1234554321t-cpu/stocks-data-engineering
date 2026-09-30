import os
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("FINNHUB_API_KEY")

url = "https://finnhub.io/api/v1/quote"
params = {
    "symbol": "AAPL",
    "token": api_key
}

response = requests.get(url, params=params)

print("Status Code:", response.status_code)
print("Response:", response.json())