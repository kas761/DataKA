import requests
import os


url = "http://127.0.0.1:8000/process_data"


headers = {
    "X-API-Key": os.getenv('API_KEY')
}

# For high quality data
params_high = {'qualityquery': 'high'}
response_high = requests.get(url, headers=headers, params=params_high)

# For low quality data
params_low = {'qualityquery': 'low'}
response_low = requests.get(url, headers=headers, params=params_low)

# Print the responses from both queries
if response_high.status_code == 200:
    print("High Quality Data:")
    print(response_high.json())
else:
    print("Error fetching high-quality data:", response_high.status_code)

if response_low.status_code == 200:
    print("\nLow Quality Data:")
    print(response_low.json())
else:
    print("Error fetching low-quality data:", response_low.status_code)
