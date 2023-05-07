import json

def get_updated_data(payload, retrieved_schema):
    schema_fields = json.loads(retrieved_schema['schema'])['fields']
    field_names = [field['name'] for field in schema_fields]

    try:
        record_data = payload['after']
        transformed_data = {field: record_data[field] for field in field_names if field in record_data}

    except (KeyError, TypeError) as e:
        print(f"Error transforming data: {e}")
        raise

    return transformed_data