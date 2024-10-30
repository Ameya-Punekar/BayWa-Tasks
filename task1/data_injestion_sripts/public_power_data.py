import requests
import json
from datetime import datetime, timedelta
import time
import pytz


# Fetch data from Energy Charts API for a specific time range
def fetch_public_power_data(start_time, end_time, country='de'):
    url = "https://api.energy-charts.info/public_power"
    
    # Define API parameters
    params = {
        'country': country,
        'start': start_time,
        'end': end_time
    }
    
    response = requests.get(url, params=params)

    # Make GET request to fetch data
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")


# Save data to JSON file and add a comma if it’s not the last entry
def save_data_to_file(data, filename, is_last_entry=False):
    with open(filename, 'a') as json_file:
        json.dump(data, json_file, indent=4)
        
        if not is_last_entry:
            json_file.write(",\n")  # Add a comma only if it's not the last entry


# Main function for data ingestion in either backfill or real-time mode
def ingest_data(frequency='15min', backfill_start=None, backfill_end=None, filename='public_power_data.json'):
    now = datetime.now()  # Use default UTC time

    # Set timezone to Europe/Berlin
    tz = pytz.timezone('Europe/Berlin')

    # Start the JSON array in the file
    with open(filename, 'w') as json_file:
        json_file.write("[\n")
    
    if backfill_start and backfill_end:
        # Set start time for backfill
        current_time = backfill_start.astimezone(tz)
        
        # Loop through each 15-minute interval in the backfill period
        while current_time < backfill_end.astimezone(tz):
            next_time = current_time + timedelta(minutes=15)
            is_last_entry = next_time >= backfill_end.astimezone(tz)  # Check if it's the last entry

            try:
                # Fetch data for the 15-minute interval
                data = fetch_public_power_data(
                    start_time=current_time.isoformat(),
                    end_time=next_time.isoformat()
                )
                save_data_to_file(data, filename, is_last_entry)
            except Exception as e:
                print(e)
            
            # Move to next 15-minute interval
            current_time = next_time
        
        # End JSON array after backfill loop
        with open(filename, 'a') as json_file:
            json_file.write("\n]")

    else:
        # Real-time ingestion: start from midnight
        today = now.strftime('%Y-%m-%d')  # Get today's date
        start_time = today  # Start from midnight today
        
         # Set current_time to midnight today
        current_time = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(tz)  # Set current_time to midnight today
        end_time = now.astimezone(tz)  # Current time

        # Fetch data from midnight until the current time in 15-minute intervals
        while current_time < end_time:
            next_time = current_time + timedelta(minutes=15)
            try:
                data = fetch_public_power_data(
                    start_time=current_time.isoformat(),
                    end_time=next_time.isoformat()
                )
                save_data_to_file(data, filename, is_last_entry=(next_time >= end_time))
            except Exception as e:
                print(e)
            
            current_time = next_time  # Move to the next interval

        # After reaching the current time, continue fetching data every 15 minutes
        while True:
            next_time = current_time + timedelta(minutes=15)

            try:
                data = fetch_public_power_data(
                    start_time=current_time.isoformat(),
                    end_time=next_time.isoformat()
                )
                save_data_to_file(data, filename, is_last_entry=False)
            except Exception as e:
                print(e)
            
            time.sleep(15 * 60)  # Wait for 15 minutes
            current_time = next_time  # Update current time to the next interval

        # Close the JSON array
        with open(filename, 'a') as json_file:
            json_file.write("\n]")

# Example usage:
mode = input("Enter mode (realtime/backfill): ").strip().lower()

if mode == "backfill":
    # Set the backfill dates
    backfill_start = datetime(2024, 10, 15, 0, 0)  # Start date
    backfill_end = datetime(2024, 10, 15, 23, 59)  # End date

    # Backfill data and save to a single file
    ingest_data(backfill_start=backfill_start, backfill_end=backfill_end, filename='public_power_data.json')

elif mode == "realtime":
    # Run in real-time ingestion mode
    ingest_data(filename='real_time_power_data.json')

else:
    print("Invalid mode. Please enter 'realtime' or 'backfill'.")
