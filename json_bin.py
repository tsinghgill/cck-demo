import os
import requests

BIN_ID = os.environ['BIN_ID']
JSON_BIN_API_KEY = os.environ['JSON_BIN_API_KEY']

def get_schema_id_from_bin():
    url = f"https://api.jsonbin.io/v3/b/{BIN_ID}"
    headers = {
        "Content-Type": "application/json",
        "X-Master-Key": JSON_BIN_API_KEY,
    }

    # Fetch the current schema ID from the bin
    response = requests.get(url, headers=headers)
    response_data = response.json()
    return response_data.get("record", {}).get("schema_id")

def update_schema_id_in_bin(new_schema_id):
    url = f"https://api.jsonbin.io/v3/b/{BIN_ID}"
    headers = {
        "Content-Type": "application/json",
        "X-Master-Key": JSON_BIN_API_KEY,
    }

    current_schema_id = get_schema_id_from_bin()

    # If the schema ID is not the latest, update it
    if current_schema_id != new_schema_id:
        updated_data = {"schema_id": new_schema_id}
        response = requests.put(url, json=updated_data, headers=headers)
        print("Schema ID updated")
        return new_schema_id

    return current_schema_id