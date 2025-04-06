import requests
import os


url = "http://127.0.0.1:8000/process_data"


headers = {
    "X-API-Key": os.getenv('API_KEY')
}

# For high quality data
params_high = {'qualityquery': 'high'}
params_low = {'qualityquery': 'low'}


if __name__ == "__main__":
    response = requests.get(url, headers=headers, params=params_high)
    print(response.json())
    response = requests.get(url, headers=headers, params=params_low)
    print(response.json())