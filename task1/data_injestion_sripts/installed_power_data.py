import requests
import json
from datetime import datetime, timedelta

# Function to fetch installed power data
def fetch_installed_power_data(country='de', time_step='monthly', installation_decommission=False):
    url = "https://api.energy-charts.info/installed_power"
    
    # Parameters for the API request
    params = {
        'country': country,
        'time_step': time_step,
        'installation_decommission': str(installation_decommission).lower()  # Convert boolean to lowercase string
    }
    
    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

# Function to append fetched price data to a JSON file
def save_data_to_file(data, filename, is_last_entry=False):
    # Open the file in append mode
    with open(filename, 'a') as json_file:
        # Convert data to JSON and write it
        json.dump(data, json_file, indent=4)
        
        if not is_last_entry:
            json_file.write(",\n")  # Add a comma only if it's not the last entry

# Main ingestion function, supports backfill and default (today) modes
def ingest_installed_power_data(time_step='monthly', installation_decommission=False, filename='installed_power_data.json'):
    # Start the JSON array in the file
    with open(filename, 'w') as json_file:
        json_file.write("[\n")
    
    try:
        # Fetch the installed power data
        data = fetch_installed_power_data(
            country='de',
            time_step=time_step,
            installation_decommission=installation_decommission
        )
        # Save the data to the file
        save_data_to_file(data, filename, is_last_entry=True)
        
    except Exception as e:
        print(e)
    
    # Close the JSON array after the data is written
    with open(filename, 'a') as json_file:
        json_file.write("\n]")

# Example usage:
# For monthly ingestion
ingest_installed_power_data(time_step='monthly', installation_decommission=False, filename='installed_power_data_monthly.json')

