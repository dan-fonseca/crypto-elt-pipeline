import requests
import pandas as pd
from datetime import datetime
import os
import sys

# Start importing data from the coingecko API endpoint
URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 50,
    'page': 1,
    'sparkline': 'false'
}

def fetch_crypto_data():
    try:
        print(f"Fetching data from {URL}...")
        response = requests.get(URL, params=PARAMS, timeout=10)
        response.raise_for_status() # Checking for any potential errors 
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"API Request failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # 1. Fetch
    raw_data = fetch_crypto_data()
    
    # 2. Adding Timestamp
    load_timestamp = datetime.now().isoformat()
    
    # 3. Parse simple fields
    clean_data = []
    for coin in raw_data:
        clean_data.append({
            'symbol': coin['symbol'],
            'name': coin['name'],
            'current_price': coin['current_price'],
            'market_cap': coin['market_cap'],
            'volume_24h': coin['total_volume'],
            'ingested_at': load_timestamp
        })
    
    # 4. Save to CSV (Staging)
    df = pd.DataFrame(clean_data)
    
    # define output path (mapped to Airflow DAGs folder)
    output_dir = "/opt/airflow/dags/data"
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"{output_dir}/crypto_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    
    print(f"Success, saved {len(df)} rows to {filename}")