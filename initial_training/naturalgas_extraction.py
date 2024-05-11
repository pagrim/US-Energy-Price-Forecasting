''' Import modules '''
import json
import os
import requests
import boto3
import requests_cache
import openmeteo_requests
from dotenv import load_dotenv
from retry_requests import retry

# Import environment variables
load_dotenv()

class S3:
    ''' 
    S3 Bucket Class for storage + retrieval of extracted content 
    
    Class Variables
    ---------------
    access_key_id (str): AWS Access Key
    secret_access_key (str): AWS Secret Access Key
    bucket (str): AWS S3 bucket for storage + retrieval
    s3_client (boto3.client): Boto3 S3 connection client

    Methods
    -------
    connect(cls):
        Creates a connection to S3 client using access_key_id and secret_access_key
    disconnect(cls):
        Disconnects from S3 client
    get_data(cls, folder, object_key):
        Gets data from S3 bucket for a given folder and object key
    put_data(cls, data, folder, object_key):
        Puts data in S3 bucket as json file under a given folder with a specific object_key
        
    '''
    access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    bucket = os.environ.get('S3_BUCKET')

    @classmethod
    def connect(cls) -> None:
        ''' 
        Creates a connection to S3 client using access_key_id and secret_access_key
        '''
        cls.s3_client=boto3.client('s3', aws_access_key_id=cls.access_key_id, 
                                   aws_secret_access_key=cls.secret_access_key)

    @classmethod
    def disconnect(cls) -> None:
        '''
        Disconnects from S3 client
        '''
        del cls.s3_client
    
    @classmethod
    def get_data(cls, folder: str, object_key: str) -> dict:
        '''
        Gets data from S3 bucket for a given folder and object key

        Args:
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
        
        Returns:
            dict: Returns json data in the form of a dictionary object

        '''
        cls.connect()
        object = cls.s3_client.get_object(Bucket=cls.bucket, Key=folder + object_key)
        contents = object['Body'].read().decode('utf-8')
        yield json.loads(contents)
        cls.disconnect()

    @classmethod
    def put_data(cls, data: list, folder: str, object_key: str) -> None:
        '''
        Puts data in S3 bucket as json file under a given folder with a specific object_key

        Args:
            data (list): Data to be put into S3 bucket
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved

        '''
        data_json = json.dumps(data)
        cls.connect()
        cls.s3_client.put_object(Bucket=cls.bucket, Key=folder + object_key, 
                                 Body=data_json, ContentType='application/json')
        cls.disconnect()

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
    def api_request(cls, endpoint: str, headers: dict, offset=0) -> requests.Response:
        '''
        Makes an API request to a specific endpoint of the Energy Information Administration API
        
        Args:
            endpoint (str): Endpoint API request is being made to
            headers (dict): Header values to be passed to the API request
            offset (int): Offset in the results. Incremented for results that contain over 5000
            units of data

        '''
        url = cls.base_url + endpoint
        params = {'api_key': os.environ.get('API_KEY')}
        headers['offset'] = offset
        headers = {
            'X-Params': json.dumps(headers),
            'Content-Type': 'application/json'
        }
        print(headers)
        try: 
            response = requests.get(url, headers=headers,  params=params, timeout=30)
            return response
        except requests.RequestException as e:
            return 'Error occurred', e
    
    @classmethod
    def extract(cls, endpoint: str, headers: dict, folder: str, object_key:str, offset=0) -> None:
        '''
        Extracts data from a request to a specific endpoint and puts data in a S3 endpoint.
        Maybe multiple requests to a specific endpoint as the API can only return 5000 results
        at once hence adjustment of offset maybe necessary
        
        Args:
            endpoint (str): Endpoint API request is being made to
            parameters (dict): Parameters being passed to the API request
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
            offset (int): Offset in the results. Incremented for results that contain over 5000
            units of data

        '''
        data = []
        while True:
            response = cls.api_request(endpoint=endpoint, headers=headers, offset=offset)
            if response.status_code == 200 and len(response.json()['response']['data']) > 0:
                results = response.json()['response']['data']
                data.extend(results)
                offset += 5000
            else:
                break
        S3.put_data(data=data, folder=folder, object_key=object_key)

class OpenMeteo:
    ''' 
    Class for extracing historical hourly weather data from selected locations from open-meteo.com

    Locations weather data was extracted from:
    Los Angeles, San Diego, San Jose, San Francisco, Sacramento, Miami,
    Tampa, Orlando, Jacksonville, Chicago, St Louis, New Orleans, Baton Rouge,
    Shreveport, Detroit, Ann Arbor, Grand Rapids, New York, Buffalo, Cleveland,
    Columbus, Cincinnati, Philadelphia, Pittsburgh, Houston, Dallas, San Antonio,
    Austin

    The above locations were located in the states with the highest quantities of
    natural gas consumption across the US. Weather in these locations is going
    to have the greatest impact on price.

    Class Variables
    ---------------
    cache_session (CachedSession): Caches session
    retry_session (CachedSession): Retries cache session is unsuccessful
    openmeteo (Client): Initialises a client session for open-meteo
    url (str): Url to be used to extract historical weather data
    latitude (list): Latitude coordinate for specific location
    longitude (list): Longitude coordinate for specific location
    locations (list): List of lists consisting of city name and state name corresponding to order of latitude and longitude values

    Methods
    -------
    api_request(parameters):
        Makes an API request to retrieve historical weather data from open-meteo API
    extract(parameters, folder, object_key):
        Extract data from request and puts results in an S3 endpoint

    '''
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    url = 'https://archive-api.open-meteo.com/v1/archive'
    latitude = [34.0522, 32.7157, 37.3394, 37.7749, 38.5816, 25.7743, 27.9475, 28.5383, 
                30.3322, 41.85, 38.6273, 29.9547, 30.4433, 32.5251, 42.3314, 42.2776,
                42.9634, 40.7143, 42.8865, 41.4995, 39.9612, 39.1271, 39.9523,
                40.4406, 29.7633, 32.7831, 29.4241, 30.2672
                ]
    longitude = [-118.2437, -117.1647, -121.895, -122.4194, -121.4944, -80.1937, -82.4584, -81.3792, 
                 -81.6556, -87.65, -90.1979, -90.0751, -91.1875, -93.7502, -83.0457, 83.7409,
                 -85.6681, -74.006, -78.8784, -81.6954, -82.9988, -84.5144, -75.1638,
                 -79.9959, -95.3633, -96.8067, -98.4936, -97.7431
                 ]
    locations = [['Los Angeles', 'California'], ['San Diego', 'California'], ['San Jose', 'California'], ['San Francisco', 'California'], 
                 ['Sacramento', 'California'], ['Miami', 'Florida'], ['Tampa', 'Florida'], ['Orlando', 'Florida'], ['Jacksonville', 'Florida'],
                 ['Chicago', 'Illinois'], ['St Louis', 'Illinois'], ['New Orleans', 'Louisiana'], ['Baton Rouge', 'Louisiana'], 
                 ['Shreveport', 'Louisiana'], ['Detroit', 'Michigan'], ['Grand Rapids', 'Michigan'], ['New York', 'New York'],
                 ['Buffalo', 'New York'], ['Cleveland', 'Ohio'], ['Columbus', 'Ohio'], ['Cincinnati', 'Ohio'],
                 ['Philadelphia', 'Pennsylvania'], ['Pittsburgh', 'Pennsylvania'], ['Houston', 'Texas'], ['Dallas', 'Texas'], 
                 ['San Antonio', 'Texas'], ['Austin', 'Texas']]
    
    @classmethod
    def api_request(cls, parameters: dict):
        ''' 
        Makes an API request to retrieve historical weather data from open-meteo API

        Args:
            parameters (dict): Parameters to be passed to the API request

        '''
        try:
            responses = cls.openmeteo.weather_api(cls.url, params = parameters)
            return responses
        except Exception as e:
            return 'Error Occurred', e
    
    @classmethod
    def extract(cls, parameters: dict, folder: str, object_key: str):
        ''' 
        Extract data from request and puts results in an S3 endpoint.
        
        Args:
            parameters (dict): Parameters to be passed to the API request
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved

        '''
        data = []
        count = 0
        responses = cls.api_request(parameters = parameters)
        for response in responses:
            print(response)
            dic = {}
            dic['location']: cls.locations[count]
            dic['temperature']: response.Hourly().Variables(0).ValuesAsNumpy()
            dic['relative humidity']: response.Hourly().Variables(1).ValuesAsNumpy()
            dic['precipitation']: response.Hourly().Variables(2).ValuesAsNumpy()
            dic['rainfall']: response.Hourly().Variables(3).ValuesAsNumpy()
            dic['snowfall']: response.Hourly().Variables(4).ValuesAsNumpy()
            dic['wind_speed']: response.Hourly().Variables(5).ValuesAsNumpy()
            data.append(dic)
            count += 1
            
        S3.put_data(data=data, folder=folder, object_key=object_key)

if __name__ == '__main__':
    EIA.extract(endpoint='natural-gas/pri/fut/data/', headers = {
        'api_key': os.environ.get('API_KEY'),
        'frequency': 'daily',
        'data': ['value'],
        'facets': {
            'series': ['RNGWHHD',]
        },
        'start': '1999-01-04',
        'end': '2024-04-26',
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
    }, folder='initial_training/', object_key='natural_gas_spot_prices')
    EIA.extract(endpoint='petroleum/pri/spt/data/', headers = {
        'api_key': os.environ.get('API_KEY'),
        'frequency': 'daily',
        'data': [
            'value'
        ],
        'facets': {
            'series': [
                'EER_EPD2F_PF4_Y35NY_DPG',
                'RWTC'
            ]
        },
        'start': '1999-01-04',
        'end': '2024-04-26',
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
    }, folder='initial_training/', object_key='oil_spot_prices')
    OpenMeteo.extract(parameters = {
        "latitude": OpenMeteo.latitude,
	    "longitude": OpenMeteo.longitude,
	    "start_date": "1999-01-04",
	    "end_date": "2024-04-26",
	    "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain", "snowfall", "wind_speed_10m"],
	    "wind_speed_unit": "ms"
    }, folder='initial_training/', object_key='daily_weather')


    


    

    


    

        
            



    

    


    










    
