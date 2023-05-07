from json_bin import get_schema_id_from_bin, update_schema_id_in_bin
from schema_registry import create_or_update_schema, get_schema, list_subjects, get_schema_id
from datadog import send_datadog_event
from helpers import get_updated_data

import logging
import sys

from turbine.runtime import RecordList
from turbine.runtime import Runtime

logging.basicConfig(level=logging.INFO)

def transform(records: RecordList) -> RecordList:
    logging.info(f"processing {len(records)} record(s)")
    for record in records:
        logging.info(f"input: {record}")
        try:
            # Get the schema ID from schema registry for a subject
            schema_id = get_schema_id("purchase-value")
            print("schema_id:", schema_id)

            # Get our stored schema ID from bin
            schema_id_from_bin = get_schema_id_from_bin()
            print(f"schema_id_from_bin: {schema_id_from_bin}")

            # Check if initial deployment
            if schema_id_from_bin is None:
                print(f"Initial Deployment. update_schema_id_in_bin: {schema_id}")
                schema_id_from_bin = update_schema_id_in_bin(schema_id)

            # Check if schema changed
            if schema_id_from_bin != schema_id:
                # Alert a 3rd party about schema change
                send_datadog_event("Schema Change", "A schema was detected")
                # Get the old schema
                schema = get_schema("purchase-value")
                print("Schema:", schema)

                # Transform the incoming record based on the retrieved schema
                updated_data = get_updated_data(record.value["payload"], schema)
                record.value["payload"]["after"] = updated_data
                print("Transformed record:", record)

            # Other Examples:

            # List all subjects in the Schema Registry
            # subjects = list_subjects()
            # print("Subjects:", subjects)

            # Get the latest schema for a subject
            # schema = get_schema("purchase-value")
            # print("Schema:", schema)

            # # Create or update a schema for a subject using an Avro (.avsc) file
            # schema_file = "./schemas/purchase.avsc"
            # schema_id = create_or_update_schema("purchase-value", schema_file)
            # print("New schema ID:", schema_id)

            logging.info(f"output: {record}")
        except Exception as e:
            print("Error occurred while parsing records: " + str(e))
            logging.info(f"output: {record}")
    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            # To configure your data stores as resources on the Meroxa Platform use the Meroxa Dashboard, CLI, or Meroxa Terraform Provider.
            # For more details refer to: https://docs.meroxa.com/

            # Identify an upstream data store for your data app with the `resources` function.
            # Replace `source_name` with the resource name the data store was configured with on the Meroxa platform.
            source = await turbine.resources("source_name")

            # Specify which upstream records to pull with the `records` function.
            # Replace `collection_name` with a table, collection, or bucket name in your data store.
            # If you need additional connector configurations, replace '{}' with the key and value, i.e. {"incrementing.field.name": "id"}
            records = await source.records("collection_name", {})

            # Specify which secrets in environment variables should be passed into the Process.
            turbine.register_secrets("BIN_ID")
            turbine.register_secrets("JSON_BIN_API_KEY")
            turbine.register_secrets("DATADOG_API_KEY")

            
            turbine.register_secrets("SCHEMA_REGISTRY_API_KEY")
            turbine.register_secrets("SCHEMA_REGISTRY_API_SECRET")
            turbine.register_secrets("SCHEMA_REGISTRY_URL")

            # Specify what code to execute against upstream records with the `process` function.
            # Replace `transform` with the name of your function code.
            transformed = await turbine.process(records, transform)

            # Identify a downstream data store for your data app with the `resources` function.
            # Replace `destination_name` with the resource name the data store was configured with on the Meroxa platform.
            destination_db = await turbine.resources("destination_name")

            # Specify where to write records downstream using the `write` function.
            # Replace `collection_archive` with a table, collection, or bucket name in your data store.
            # If you need additional connector configurations, replace '{}' with the key and value, i.e. {"behavior.on.null.values": "ignore"}
            await destination_db.write(transformed, "collection_archive", {})
        except Exception as e:
            print(e, file=sys.stderr)
