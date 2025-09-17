import os
from datetime import datetime, timedelta
from dateutil import parser
from dotenv import load_dotenv
import psycopg2
from data import fetch_data, parse_data, insert_entries

START_DATE = datetime(2025, 8, 1).date()

# Load environment variables
load_dotenv()

# Database connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def main():
    connection = None
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME
        )
        cursor = connection.cursor()
        
        today = datetime.today().date()
        
        # All required dates
        all_dates = {START_DATE + timedelta(days=i) for i in range((today - START_DATE).days + 2)} # +2 to include today and tomorrow
        
        # Dates already in DB
        cursor.execute("SELECT DISTINCT delivery_date FROM electricity_prices")
        existing_dates = {row[0] for row in cursor.fetchall()}
        
        # Missing dates
        missing_dates = sorted(all_dates - existing_dates)
        
        print(f"Found {len(missing_dates)} missing dates to process")
        
        for d in missing_dates:
            print(f"Processing date: {d}")
            raw = fetch_data(d)
            entries = parse_data(raw)
            insert_entries(entries, connection)
            print(f"Inserted data for {d}")
            
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()