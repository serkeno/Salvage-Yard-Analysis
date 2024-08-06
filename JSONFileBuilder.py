import StoredQueries

import json
from datetime import datetime, timezone
import os


def build_file(dataframe, name="Default"):

    # Get the current date to be used in the file naming
    current_utc_time = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Define the path and file name
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls')
    os.makedirs(desktop_path, exist_ok=True)  # Create the directory if it doesn't exist
    file_name = os.path.join(desktop_path, f'{name}_{current_utc_time}.json')

    # Get the JSON result (assuming StoredQueries.model_and_sales_by_date() returns a JSON string)
    json_result = dataframe.to_json()

    # Write the JSON result to the file
    with open(file_name, 'w') as json_file:
        json.dump(json.loads(json_result), json_file, indent=4)
def build_binary_vehicle_presence():

    # Get the current date to be used in the file naming
    current_utc_time = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Define the path and file name
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls')
    os.makedirs(desktop_path, exist_ok=True)  # Create the directory if it doesn't exist
    file_name = os.path.join(desktop_path, f'binary_vehicle_presence{current_utc_time}.json')

    # Get the model_and_sales_by_date query df
    stored_query = StoredQueries.binary_vehicle_presence()

    # Get the JSON result (assuming StoredQueries.model_and_sales_by_date() returns a JSON string)
    json_result = stored_query.to_json()

    # Write the JSON result to the file
    with open(file_name, 'w') as json_file:
        json.dump(json.loads(json_result), json_file, indent=4)


def build_continuous_vehicle_presence():
    import json
    from datetime import datetime, timezone
    import os

    # Get the current date to be used in the file naming
    current_utc_time = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Define the path and file name
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls')
    os.makedirs(desktop_path, exist_ok=True)  # Create the directory if it doesn't exist
    file_name = os.path.join(desktop_path, f'continuous_vehicle_presence{current_utc_time}.json')

    # Get the model_and_sales_by_date query df
    stored_query = StoredQueries.continuous_vehicle_presence()

    # Get the JSON result (assuming StoredQueries.model_and_sales_by_date() returns a JSON string)
    json_result = stored_query.to_json()

    # Write the JSON result to the file
    with open(file_name, 'w') as json_file:
        json.dump(json.loads(json_result), json_file, indent=4)

