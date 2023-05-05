import requests

# URL for creating a new JSON bin
url = 'https://api.jsonbin.io/v3/b'

# Headers required for the request, including your API key
headers = {
  'Content-Type': 'application/json',
  'X-Master-Key': '<JSON_BIN_MASTER_KEY>'
}

# Initial data to be stored in the new JSON bin
data = {"sample": "Hello World"}

# Send a POST request to create the new JSON bin
req = requests.post(url, json=data, headers=headers)

# Print the server response
print(req.text)
