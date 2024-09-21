''' Import modules '''
from datetime import datetime
import json
import os
import requests
from utils.aws import S3, S3Metadata
from dotenv import load_dotenv

# Import environment variables
load_dotenv()

class EIA:
    '''
    Class for extracting data from Energy Information Administration API
    
    Class Variables
    ---------------
    api_key (str): API key for making requests
    base_url (str): Base url to be used by all requests

    Methods
    -------
    api_request(endpoint, headers, offset):
        Makes an API request to a specific endpoint of the Energy Information Administration API
    extract(endpoint, parameters, folder, object_key, offset):
        Extracts data from a request to a specific endpoint and puts data in a S3 endpoint.
        Maybe multiple requests to a specific endpoint as the API can only return 5000 results
        at once hence adjustment of offset maybe necessary
    
    '''
    api_key = os.environ.get('API_KEY')
    base_url = 'https://api.eia.gov/v2/'

    @classmethod
    def api_request(cls, endpoint: str, headers: dict, metadata_folder: str, metadata_object_key: str, metadata_dataset_key: str, start_date_if_none: str, offset=0) -> requests.Response:
        '''
        Makes an API request to a specific endpoint of the Energy Information Administration API
        
        Args:
            endpoint (str): Endpoint API request is being made to
            headers (dict): Header values to be passed to the API request
            metadata_folder (str): Metadata folder where latest end date for data extraction of given dataset is retrieved from
            metadata_object_key (str): Metadata object where latest end date is being retrieved from
            metadata_dataset_key (str): Dataset key from metadata where latest end date is being retrieved for
            offset (int): Offset in the results. Incremented for results that contain over 5000
            units of data
        
        Returns:
            requests.Response: Response object from API request
        '''
        url = cls.base_url + endpoint
        params = {'api_key': os.environ.get('API_KEY')}
        start_date = S3Metadata.get_latest_end_date(folder=metadata_folder, object_key=metadata_object_key, dataset_key=metadata_dataset_key) # Varies depending on metadata for given dataset
        if start_date is None:
            start_date = start_date_if_none
        headers['start_date'] = start_date
        headers['offset'] = offset
        headers = {
            'X-Params': json.dumps(headers),
            'Content-Type': 'application/json'
        }
        try: 
            response = requests.get(url, headers=headers,  params=params, timeout=30)
            return response
        except requests.RequestException as e:
            return 'Error occurred', e
    
    @classmethod
    def get_max_date(cls, data: list) -> str:
        ''' Retrieves latest end date from data extracted to be logged in metadata '''
        if not data:
            return None
        else: 
            dates = [datetime.strptime(item['period'], '%Y-%m-%d') for item in data]
            max_date = max(dates)
            max_date_str = max_date.strftime('%Y-%m-%d')
            return max_date_str
    
    @classmethod
    def extract(cls, endpoint: str, headers: dict, folder: str, object_key: str, metadata_folder: str, 
                metadata_object_key: str, metadata_dataset_key: str, start_date_if_none: str, offset=0) -> None:
        '''
        Extracts data from a request to a specific endpoint and puts data in a S3 endpoint.
        Maybe multiple requests to a specific endpoint as the API can only return 5000 results
        at once hence adjustment of offset maybe necessary
        
        Args:
            endpoint (str): Endpoint API request is being made to
            headers (dict): Parameters being passed to the API request
            folder (str): S3 folder data is going to be ingressed into
            object_key (str): Name of object being ingressed into S3 bucket
            metadata_folder (str): Metadata folder where latest end date for data extraction of given dataset is retrieved from
            metadata_object_key (str): Metadata object where latest end date is being retrieved from
            metadata_dataset_key (str): Dataset key from metadata where latest end date is being retrieved for
            offset (int): Offset in the results. Incremented for results that contain over 5000
            units of data

        '''
        data = []
        while True:
            response = cls.api_request(endpoint=endpoint, headers=headers, metadata_folder=metadata_folder, metadata_object_key=metadata_object_key,
                                       metadata_dataset_key = metadata_dataset_key, start_date_if_none=start_date_if_none, offset=offset)
            if response.status_code == 200 and len(response.json()['response']['data']) > 0:
                results = response.json()['response']['data']
                data.extend(results)
                offset += 5000
            else:
                break
        
        max_date = cls.get_max_date(data)
        if max_date is None:
            return
        else:
            S3.put_data(data=data, folder=folder, object_key=object_key)
            S3Metadata.update_metadata(folder=metadata_folder, object_key=metadata_object_key, dataset_key=metadata_dataset_key, new_date=max_date)