import os
import requests
from datetime import datetime
from dateutil import parser
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

def fetch_data(date):
    """Fetch data from the API."""
    # API URL
    datestr = datetime.strftime(date, "%Y-%m-%d")
    url = f"https://dataportal-api.nordpoolgroup.com/api/DayAheadPriceIndices?date={datestr}&market=DayAhead&indexNames=EE,LT,LV,AT,BE,FR,GER,NL,PL,DK1,DK2,FI,NO1,NO2,NO3,NO4,NO5,SE1,SE2,SE3,SE4&currency=EUR&resolutionInMinutes=15"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {date}: {e}")
        return None

def parse_data(data):
    """Parse API data into a list of dictionaries."""
    if not data:
        return []
    
    entries = []
    
    # Handle missing keys gracefully
    delivery_date = parser.isoparse(data.get("deliveryDateCET"))
    updated_at = parser.isoparse(data.get("updatedAt"))
    currency = data.get("currency")
    resolution = int(data.get("resolutionInMinutes", 0))
    
    if not delivery_date:
        print("Warning: No delivery_date found in API response")
        return []
    
    regions = [
        "EE", "LT", "LV", "AT", "BE", "FR", "GER", "NL", "PL",
        "DK1", "DK2", "FI", "NO1", "NO2", "NO3", "NO4", "NO5",
        "SE1", "SE2", "SE3", "SE4"
    ]
    
    for entry in data.get("multiIndexEntries"):
        try:
            start_time = parser.isoparse(entry["deliveryStart"])
            end_time = parser.isoparse(entry["deliveryEnd"])
            
            entry_data = {
                "delivery_date": delivery_date,
                "updated_at": updated_at,
                "currency": currency,
                "resolution": resolution,
                "start_time": start_time,
                "end_time": end_time,
            }
            
            # Add price data for each region
            for region in regions:
                price = entry.get("entryPerArea", {}).get(region)
                entry_data[f"{region}_price"] = price
            
            entries.append(entry_data)
            
        except (KeyError, ValueError) as e:
            print(f"Error parsing entry: {e}")
            continue
    
    print(f"Fetched {len(entries)} entries for date {delivery_date.date()}")
    return entries

def insert_entries(entries, connection):
    """Insert entries into the database."""
    if not entries:
        print("No entries to insert")
        return
    
    cursor = connection.cursor()
    
    insert_query = sql.SQL("""
        INSERT INTO electricity_prices (
            delivery_date, updated_at, currency, resolution,
            start_time, end_time,
            EE_price, LT_price, LV_price, AT_price, BE_price,
            FR_price, GER_price, NL_price, PL_price,
            DK1_price, DK2_price, FI_price,
            NO1_price, NO2_price, NO3_price, NO4_price, NO5_price,
            SE1_price, SE2_price, SE3_price, SE4_price
        ) VALUES (
            %(delivery_date)s, %(updated_at)s, %(currency)s, %(resolution)s,
            %(start_time)s, %(end_time)s,
            %(EE_price)s, %(LT_price)s, %(LV_price)s, %(AT_price)s, %(BE_price)s,
            %(FR_price)s, %(GER_price)s, %(NL_price)s, %(PL_price)s,
            %(DK1_price)s, %(DK2_price)s, %(FI_price)s,
            %(NO1_price)s, %(NO2_price)s, %(NO3_price)s, %(NO4_price)s, %(NO5_price)s,
            %(SE1_price)s, %(SE2_price)s, %(SE3_price)s, %(SE4_price)s
        )
    """)
    
    try:
        cursor.executemany(insert_query, entries)
        connection.commit()
        print(f"Successfully inserted {len(entries)} entries")
    except Exception as e:
        connection.rollback()
        print(f"Error inserting data: {e}")
        raise