import requests
import json
from datetime import datetime, timedelta
import pytz

# Function to fetch price data for specified dates from Energy-Charts API
def fetch_price_data(start_date, end_date, bzn='DE-LU'):
    url = "https://api.energy-charts.info/price"
    
    params = {
        'bzn': bzn,
        'start': start_date,
        'end': end_date
    }
    
    response = requests.get(url, params=params)

    # Check if request was successful and return data, else raise error
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

# Function to append fetched price data to a JSON file
def save_price_data_to_file(data, filename):
    with open(filename, 'a') as json_file:
        json.dump(data, json_file, indent=4)

# Main ingestion function, supports backfill and default (today) modes
def ingest_price_data(frequency='daily', start_date=None, end_date=None, filename='price_data.json'):
    # Set timezone to Europe/Berlin for automatic handling of CET and CEST
    tz = pytz.timezone('Europe/Berlin')

    # Start the JSON array in the file (if it's the first time writing)
    with open(filename, 'w') as json_file:
        json_file.write("[\n")

    entries = []  # List to store all entries for validation

    if start_date and end_date:
        # Backfill ingestion with specified start and end dates
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').astimezone(tz)
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').astimezone(tz)

        current_date = start_date_obj
        
        while current_date <= end_date_obj:
            try:
                data = fetch_price_data(start_date=current_date.strftime('%Y-%m-%d'), end_date=current_date.strftime('%Y-%m-%d'))
                entries.append(data)  # Collect data for later validation
            except Exception as e:
                print(e)
            
            current_date += timedelta(days=1)  # Increment the date by one day

    else:
        # Default to today's date for backfill
        today = datetime.now(tz)
        start_date_obj = today.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_obj = today.replace(hour=23, minute=59, second=59, microsecond=999999)

        current_date = start_date_obj
        
        while current_date <= end_date_obj:
            try:
                data = fetch_price_data(start_date=current_date.strftime('%Y-%m-%d'), end_date=current_date.strftime('%Y-%m-%d'))
                entries.append(data)  # Collect data for later validation
            except Exception as e:
                print(e)
            
            current_date += timedelta(days=1)  # Increment the date by one day

    # Write entries to file and handle potential trailing commas
    if entries:
        for i, entry in enumerate(entries):
            save_price_data_to_file(entry, filename)
            if i < len(entries) - 1:  # Check if this is not the last entry
                with open(filename, 'a') as json_file:
                    json_file.write(",\n")

    # Close the JSON array at the end
    with open(filename, 'a') as json_file:
        json_file.write("\n]")  # Write the closing bracket

    # Validate JSON format
    validate_json_file(filename)

def validate_json_file(filename):
    """ Check if the JSON file is properly formatted. """
    with open(filename, 'r') as json_file:
        try:
            json_file.seek(0)  # Move to the start of the file
            json.load(json_file)  # Try loading the JSON to validate
            print("JSON format is valid.")
        except json.JSONDecodeError as e:
            print(f"JSON format is invalid: {e}")

# Example usage:
mode = input("Enter mode (backfill) or press Enter for backfill: ").strip().lower()

if mode == "backfill" or mode == "":
    start_date = input("Enter start date (YYYY-MM-DD) or press Enter for today: ").strip() or None
    end_date = input("Enter end date (YYYY-MM-DD) or press Enter for today: ").strip() or None
    
    # Run in backfill ingestion mode
    ingest_price_data(start_date=start_date, end_date=end_date, filename='price_data.json')

else:
    print("Invalid mode. Defaulting to backfill.")
    start_date = input("Enter start date (YYYY-MM-DD) or press Enter for today: ").strip() or None
    end_date = input("Enter end date (YYYY-MM-DD) or press Enter for today: ").strip() or None
    
    # Run in backfill ingestion mode
    ingest_price_data(start_date=start_date, end_date=end_date, filename='price_data.json')
