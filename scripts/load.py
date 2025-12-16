import psycopg2
import os
import glob # Very useful to deal with files properties
import pandas as pd
import sys

# db config - setup of Docker
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"
DB_HOST = "postgres" 
DB_PORT = "5432"

def get_latest_file(directory):
    """Finds the most recent CSV file in the directory."""
    list_of_files = glob.glob(os.path.join(directory, '*.csv'))
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)

def load_data():
    # Locate the file
    data_dir = '/opt/airflow/dags/data'
    latest_file = get_latest_file(data_dir)
    
    if not latest_file:
        print("No CSV files found to load.")
        sys.exit(0)
        
    print(f"Loading data from: {latest_file}")
    
    # Read CSV into pd DataFrame
    df = pd.read_csv(latest_file)
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()
        
        #Create Table (if not exists)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_crypto_data (
            symbol VARCHAR(50),
            name VARCHAR(100),
            current_price NUMERIC,
            market_cap NUMERIC,
            volume_24h NUMERIC,
            ingested_at TIMESTAMP
        );
        """
        cur.execute(create_table_query)
        
        # Insert Data
        insert_query = """
        INSERT INTO raw_crypto_data (symbol, name, current_price, market_cap, volume_24h, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        for _, row in df.iterrows():
            cur.execute(insert_query, (
                row['symbol'], 
                row['name'], 
                row['current_price'], 
                row['market_cap'], 
                row['volume_24h'], 
                row['ingested_at']
            ))
            
        conn.commit()
        print(f"Successfully loaded {len(df)} rows into Postgres.")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    load_data()