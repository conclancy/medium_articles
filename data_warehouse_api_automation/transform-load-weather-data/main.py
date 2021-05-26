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

import json
from google.api_core import retry
from google.cloud import bigquery
from google.cloud import storage

# create clients 
storage_client = storage.Client()
bq_client = bigquery.Client()

# BigQuery variables
dataset_name = '**your dataset name**'
table_name = '**your table name**'


def to_gbq(data, context):
    '''Transform and load data from Cloud Storage into BigQuery.
    This function is executed whenever a file is added to Cloud Storage.
    
    Args:
         data (dict): File data. 
         context (google.cloud.functions.Context): Metadata for the event.
    '''

    # file metadata 
    bucket_name = data['bucket']
    file_name = data['name']

    # load file from bucket 
    blob = storage_client.get_bucket(bucket_name).blob(file_name)

    # transform and load
    raw_json = json.loads(blob.download_as_string())

    for j in raw_json: 

        # transform data by adding celsius temperature 
        j['temperature_c'] = round((j['temperature'] - 32) * (5/9), 1)

        # rename 'temperature' column (create new key while deleting old key)
        j['temperature_f'] = j.pop('temperature')

        # load to BigQuery data warehouse
        table = bq_client.dataset(dataset_name).table(table_name)
        errors = bq_client.insert_rows_json(table,
                                    json_rows=[j],
                                    row_ids=[file_name + j['zipcode']],
                                    retry=retry.Retry(deadline=30))
                                    
        # error handinling 
        if errors != []:
            raise BigQueryError(errors)
    
    # delete the file to free up storage
    blob.delete()