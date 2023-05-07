import requests
import json
import os

DATADOG_API_KEY = os.environ['DATADOG_API_KEY']

def send_datadog_event(title, text):
    url = f'https://api.datadoghq.com/api/v1/events?api_key={DATADOG_API_KEY}'
    headers = {'Content-Type': 'application/json'}
    payload = {
        'title': title,
        'text': text,
        'priority': 'normal',
        'alert_type': 'info',
        'source_type_name': 'custom'
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 202:
        print ("datadog response.text", response.text)
        print("Event sent to Datadog")
    else:
        print("Error sending event to Datadog:", response.text)
