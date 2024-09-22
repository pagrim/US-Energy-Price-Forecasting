''' Import modules '''
from datetime import datetime, timedelta
import os
import requests 
from utils.aws import S3, S3Metadata
from dotenv import load_dotenv

# Import environment variables
load_dotenv()

class NOAA:
    ''' 
    Class for extracing historical hourly weather data from selected locations from 
    National Oceanic and Atmospheric Administration (NOAA)

    Locations weather data was extracted from:
    Los Angeles, San Diego, San Francisco, Sacramento, Miami,
    Tampa, Orlando, Jacksonville, Chicago, St Louis, New Orleans, Baton Rouge,
    Shreveport, Detroit, Grand Rapids, New York, Buffalo, Cleveland,
    Columbus, Cincinnati, Philadelphia, Pittsburgh, Houston, Dallas, San Antonio,
    Austin

    The above locations were located in the states with the highest quantities of
    natural gas consumption across the US. Weather in these locations is going
    to have the greatest impact on price.

    Class Variables
    ---------------
    token (str): Token to be used for API requests
    url (str): Url to be used to extract historical weather data
    locations (dict): Dictionary consisting of stationid : [city, state] key + values

    Methods
    -------
    api_request(parameters):
        Makes an API request to retrieve historical weather data from NOAA API
    get_max_date(data):
        Retrieves latest end date from data extracted to be logged in metadata
    extract(parameters, folder, object_key):
        Extract data from request and puts results in an S3 endpoint
    '''
    token = os.environ.get('TOKEN')
    url = 'https://www.ncei.noaa.gov/cdo-web/api/v2/data'
    locations = {'GHCND:USW00023174': ['Los Angeles', 'California'], 'GHCND:USW00023188': ['San Diego', 'California'], 
                 'GHCND:USW00023234': ['San Francisco', 'California'], 
                 'GHCND:USW00023232': ['Sacramento', 'California'], 'GHCND:USW00012839':['Miami', 'Florida'], 
                 'GHCND:USW00012842': ['Tampa', 'Florida'], 'GHCND:USW00012815':['Orlando', 'Florida'], 
                 'GHCND:USW00013889': ['Jacksonville', 'Florida'], 'GHCND:USW00094846': ['Chicago', 'Illinois'], 
                 'GHCND:USW00013994': ['St Louis', 'Illinois'], 'GHCND:USW00012916': ['New Orleans', 'Louisiana'], 
                 'GHCND:USW00013970': ['Baton Rouge', 'Louisiana'], 'GHCND:USW00013957': ['Shreveport', 'Louisiana'], 
                 'GHCND:USW00094847': ['Detroit', 'Michigan'], 'GHCND:USW00094860': ['Grand Rapids', 'Michigan'], 
                 'GHCND:USW00014734': ['New York', 'New York'], 'GHCND:USW00014733': ['Buffalo', 'New York'], 
                 'GHCND:USW00014820': ['Cleveland', 'Ohio'], 'GHCND:USW00014821': ['Columbus', 'Ohio'], 
                 'GHCND:USW00093814': ['Cincinnati', 'Ohio'], 'GHCND:USW00013739': ['Philadelphia', 'Pennsylvania'], 
                 'GHCND:USW00094823': ['Pittsburgh', 'Pennsylvania'], 'GHCND:USW00012960': ['Houston', 'Texas'], 
                 'GHCND:USW00013960': ['Dallas', 'Texas'], 'GHCND:USW00012921': ['San Antonio', 'Texas'], 
                 'GHCND:USW00013904': ['Austin', 'Texas']}
    
    @classmethod
    def api_request(cls, parameters: dict) -> requests.Response:
        ''' 
        Makes an API request to retrieve historical weather data from NOAA API

        Args:
            parameters (dict): Parameters to be passed to the API request
        
        Returns:
            requests.Response: Response object from API request
        '''
        while True:
            try:
                response = requests.get(url = cls.url, headers = {'token': cls.token},
                                        params = parameters, timeout=7)
                if response.status_code == 200:
                    break
            
            except requests.exceptions.Timeout:
                response = requests.get(url = cls.url, headers = {'token': cls.token},
                                        params = parameters, timeout=7)
                    
            except requests.RequestException as e:
                return 'Error occurred', e
            
        return response
    
    @classmethod
    def get_max_date(cls, data: list) -> str:
        ''' 
        Retrieves latest end date from data extracted to be logged in metadata
        Args:
            data (list): Records in extracted data
        
        Returns:
            str: Latest end date in string format 
        '''
        if not data:
            return None
        else: 
            dates = [datetime.strptime(item['date'], '%Y-%m-%d') for item in data]
            max_date = max(dates)
            max_date_str = max_date.strftime('%Y-%m-%d')
            return max_date_str
    
    @classmethod
    def extract(cls, parameters: dict, folder: str, object_key: str, metadata_folder: str, 
        metadata_object_key: str, metadata_dataset_key: str, start_date_if_none: str) -> None:
        ''' 
        Extract data from requests and puts results in an S3 endpoint.
        
        Args:
            parameters (dict): Parameters to be passed to the API request
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
            metadata_folder (str): Metadata folder where latest end date for data extraction of given dataset is retrieved from
            metadata_object_key (str): Metadata object where latest end date is being retrieved from
            metadata_dataset_key (str): Dataset key from metadata where latest end date is being retrieved for
            start_date_if_none (str): Date to be used for start_date key in headers if there are no dates for a given metadata dataset key
        '''
        data = []
        
        start_date = S3Metadata.get_latest_end_date(folder=metadata_folder, object_key=metadata_object_key, dataset_key=metadata_dataset_key)
        if start_date is None:
            start_date = start_date_if_none
        parameters['startdate'] = start_date

        while True:
            response = cls.api_request(parameters=parameters)
            results = response.json()['results']

            if results is None:
                break
            
            for record in results:
                record['city'] = cls.locations.get(record['station'])[0]
                record['state'] = cls.locations.get(record['station'])[1]
            
            data.extend(results)
            
            startdate = datetime.strptime(parameters['startdate'], '%Y-%m-%d')
            enddate = datetime.strptime(parameters['enddate'], '%Y-%m-%d')
            increment = timedelta(days=6)
                
            parameters['startdate'] = (startdate + increment).strftime('%Y-%m-%d')
            parameters['enddate'] = (enddate + increment).strftime('%Y-%m-%d')
        
        max_date = cls.get_max_date(data)
        if max_date is None:
            return
        else:
            S3.put_data(data=data, folder=folder, object_key=object_key)
            S3Metadata.update_metadata(folder=metadata_folder, object_key=metadata_object_key, dataset_key=metadata_dataset_key, new_date=max_date)

