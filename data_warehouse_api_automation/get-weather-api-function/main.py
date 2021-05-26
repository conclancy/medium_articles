# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests
import json
import time
import logging

from google.cloud import storage
from google.cloud import secretmanager

def get_weather(event, context):
    """Retreive weather data from API and store in Cloud Storage bucket.
    Triggered from a message on a Cloud Pub/Sub topic.
    
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    try:
        # client creation
        storage_client = storage.Client()
        secrets_client = secretmanager.SecretManagerServiceClient()
        
        # variable set up
        condition_list = []
        zip_code_list = ['02101', '94105', '98101']
        project_id = '**Project ID from Console Home Tab**'
        secret_id = '**your secret id**'
        bucket_id = '**your bucket id**'

        # API Key Retreival from secret manager
        name = f'projects/{project_id}/secrets/{secret_id}/versions/1'
        response = secrets_client.access_secret_version(request={"name": name})
        api_key = response.payload.data.decode("UTF-8")
        
        # Retrieve and model data 
        for zip_code in zip_code_list:

            # API call
            r = requests.get(f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={zip_code}')
            r_json = json.loads(r.text)
            
            # Model data
            city_dict = {
                'condition_timestamp': r_json['current']['last_updated'], 
                'zipcode': zip_code,
                'city': r_json['location']['name'],
                'temperature': r_json['current']['temp_f'],
                'condition': r_json['current']['condition']['text']
            }
            
            # Stage data
            condition_list.append(city_dict)
        
        # Store data in bucket
        bucket = storage_client.get_bucket(bucket_id)

        # Create a unique name for file using epoch unix timestamp
        blob = bucket.blob(f'{int(time.time())}.json')

        # Add data to cloud storage as json file
        blob.upload_from_string(
            data=json.dumps(condition_list),
            content_type='application/json'
        )

    except Exception:
        logging.error()
