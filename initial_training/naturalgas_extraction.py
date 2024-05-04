# Import modules
import json
import os
import requests
import boto3
from dotenv import load_dotenv
from typing import Generator

# Import environment variables
load_dotenv()

# S3 Bucket Class for storage + retrieval of extracted content
class S3:
    # Define class variables
    access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    bucket = os.environ.get('S3_BUCKET')

    # Connect to instance of S3 bucket
    @classmethod
    def connect(cls) -> None:
        cls.s3_client=boto3.client('s3', aws_access_key_id=cls.access_key_id, aws_secret_access_key=cls.secret_access_key)

    # Disconnect from instance of S3 bucket
    @classmethod
    def disconnect(cls) -> None:
        del cls.s3_client
    
    # Extract data from specified object
    @classmethod
    def retrieve(cls, folder: str, object_key: str) -> Generator(dict):
        cls.connect()
        object = cls.s3_client.get_object(Bucket=cls.bucket, Key=folder + object_key)
        contents = object['Body'].read().decode('utf-8')
        yield json.loads(contents)
        cls.disconnect()

    # Store data into S3 bucket
    @classmethod
    def store(cls, data: list, folder: str, object_key: str) -> None:
        data_json = json.dumps(data)
        cls.connect()
        cls.s3_client.put_object(Bucket=cls.bucket, Key=folder + object_key, Body=data_json, ContentType='application/json')
        cls.disconnect()

# Class for extracting data from Energy Information Administration API
class EIA:
    # Define class variables
    api_key = os.environ.get('API_KEY')
    base_url = 'https://api.eia.gov/v2/'

    # Define API request
    @classmethod
    def api_request(cls, endpoint: str, parameters: dict, offset=0) -> requests.Response:
        url = cls.base_url + endpoint
        parameters['start'] = offset
        response = requests.get(url, params=parameters)
        return response
    
    # Make API calls until all data has been extracted (API by default only returns 5000 rows)
    @classmethod
    def extract(cls, endpoint: str, parameters: dict, folder, object_key, offset=0) -> None:
        data = []
        while True:
            response = cls.api_request(endpoint=endpoint, parameters=parameters, offset=offset)
            if response.status_code == 200:
                results = response.json()
                if not results:
                    break
                data.extend(results)
                offset += 5000
            else:
                raise Exception(f'{response.status_code} : {response.text}')
        S3.store(data=data, folder=folder, object_key=object_key)

# Class for extracting weather data from selected locations from Open-meteo.com
class OpenMeteo:




    

        
            



    

    


    










    
