import requests
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables from .env file
load_dotenv()

def get_api_session():
    """
    TASK 1: Create a session that automatically retries 3 times 
    with an exponential backoff (e.g., wait 1s, 2s, 4s).
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=2, #because it goes like 2,4,8,16 etc enough time for the server to get back
        allowed_methods=["HEAD", "GET", "OPTIONS"] #allow_methods instead of methods_whitelist (more futureproof)
    )
    adapter=HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter) # beware because the link coul be unsecure 'http' so the retry strategy would not work
    #The adapters job here is to establish a connection and stabilize the connection
    return session

def extract():
    api_url = os.getenv("API_URL")
    output_path = os.getenv("RAW_STORAGE_PATH")
    session=get_api_session()
    try:
        print(f"Timestamp [{datetime.now().strftime('%Y%m%d_%H%M%S')}] Attempting to fetch data...")
        r=session.get(url=api_url)
        r.raise_for_status()
        data=r.json()
        now=datetime.now()

        print("Success! Saving file...",end='\n\n')

        enriched_data = {
         "metadata": {
        "source": api_url,
        "extracted_at (Time of the extracting location)": now.isoformat(),
        "schema_version": "1.0"},
        "raw_payload": data
        }

        os.makedirs(output_path,exist_ok=True)
        filename = f"weather_data_{now.strftime('%Y%m%d_%H%M%S')}.json"
        full_path = os.path.join(output_path, filename)
        with open(full_path, 'w') as f:
            json.dump(enriched_data, f)
            
            
    except requests.exceptions.HTTPError as err:
        print(f'Timestamp [{now.strftime("%Y%m%d_%H%M%S")}] HTTP Error occurred: {err}')
    
    except requests.exceptions.ConnectionError as err:
        print(f'Timestamp [{now.strftime("%Y%m%d_%H%M%S")}] Failing infrastructure error: {err}')
    
    except requests.exceptions.JSONDecodeError as err:
        print(f'Timestamp [{now.strftime("%Y%m%d_%H%M%S")}] JSON error: {err}')
    
    except OSError as err:
        print(f'Timestamp [{now.strftime("%Y%m%d_%H%M%S")}] Operating system error: {err}')
    
    except Exception as err:

        print(f'Timestamp [{now.strftime("%Y%m%d_%H%M%S")}] PIPELINE FAILURE: {err}')

if __name__ == "__main__":
    extract()
