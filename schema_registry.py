import json
import os
import requests

SCHEMA_REGISTRY_API_KEY = os.environ['SCHEMA_REGISTRY_API_KEY']
SCHEMA_REGISTRY_API_SECRET = os.environ['SCHEMA_REGISTRY_API_SECRET']
SCHEMA_REGISTRY_URL = os.environ['SCHEMA_REGISTRY_URL']

def list_subjects():
    print("list_subjects")
    response = requests.get(
        f'{SCHEMA_REGISTRY_URL}/subjects',
        auth=(SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET)
    )
    print("list_subjects response", response)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error listing subjects: {response.text}")

def get_schema(subject, schema_id=None):
    print("get_schema")
    if schema_id is None:
        version = "latest"
    else:
        version = str(schema_id)
    
    response = requests.get(
        f'{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/{version}',
        auth=(SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET)
    )
    print("get_schema response", response)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error getting schema: {response.text}")


def get_schema_id(subject):
    schema = get_schema(subject)
    return schema['id']

def create_or_update_schema(subject, schema_file_path):
    print("create_or_update_schema")
    try:
        with open(schema_file_path, 'r') as file:
            schema_json = json.load(file)
    except FileNotFoundError:
        raise Exception("Schema file not found")
    except json.JSONDecodeError:
        raise Exception("Invalid JSON in schema file")

    payload = {
        'schema': json.dumps(schema_json),
        'schemaType': 'AVRO'
    }

    response = requests.post(
        f'{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions',
        auth=(SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET),
        headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'},
        json=payload
    )

    print("create_or_update_schema response", response)

    if response.status_code == 200:
        print("create_or_update_schema response.json()", response.json())
        return response.json()['id']
    else:
        raise Exception(f"Error creating or updating schema: {response.text}")

def delete_subject(subject):
    print("delete_subject")
    response = requests.delete(
        f'{SCHEMA_REGISTRY_URL}/subjects/{subject}',
        auth=(SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET)
    )
    print("delete_subject response", response)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error deleting subject: {response.text}")