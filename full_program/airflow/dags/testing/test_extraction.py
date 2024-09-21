''' Import modules '''
import json
import pytest
import requests
from unittest.mock import patch, MagicMock
from extraction.eia_api import EIA
from extraction.noaa_api import NOAA
from fixtures.fixtures import mock_environment_variables, mock_boto3_s3, mock_requests_get, mock_natural_gas_spot_prices_response

class TestEIA:
    def test_eia_api_request_success_with_latest_end_date_default_offset(self, mock_environment_variables, mock_requests_get, mock_get_latest_end_date, mock_natural_gas_spot_prices_response):
        ''' Test api_request method of EIA class where latest end date is specified for a given dataset key '''
        mock_get_latest_end_date.return_value = '1999-01-04'
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = mock_natural_gas_spot_prices_response
        mock_requests_get.return_value = mock_response

        endpoint = 'natural-gas/pri/fut/data/'
        headers = {
        'api_key': 'api_key',
        'frequency': 'daily',
        'data': ['value'],
        'facets': {
            'series': ['RNGWHHD',]
        },
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
        }
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = ''
        
        response = EIA.api_request(endpoint=endpoint,
        headers = headers,
        metadata_folder = metadata_folder,
        metadata_object_key = metadata_object_key,
        metadata_dataset_key = metadata_dataset_key,
        start_date_if_none = start_date_if_none)
        
        mock_requests_get.assert_called_once_with('https://api.eia.gov/v2/' + endpoint,
            headers={'X-Params': json.dumps({
                'api_key': 'api_key',
                'frequency': 'daily',
                'data': ['value'],
                'facets': {
                    'series': ['RNGWHHD',]
                },
                'sort': [{
                'column': 'period',
                'direction': 'asc'
                }],
                'length': 5000,
                'start_date': '1999-01-04',
                'offset': 0}),
                'Content-Type': 'application/json'},
            params = {'api_key': 'api_key'},
            timeout=30
        )
        assert response.status_code == 200
        assert response.json() == json.loads(mock_natural_gas_spot_prices_response)
    
    def test_eia_api_request_success_with_non_default_offset(self, mock_environment_variables, mock_requests_get, mock_get_latest_end_date, mock_natural_gas_spot_prices_response):
        ''' Test api_request method of EIA class where latest end date is specified for a given dataset key and non-default offset '''
        mock_get_latest_end_date.return_value = '1999-01-04'
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = mock_natural_gas_spot_prices_response
        mock_requests_get.return_value = mock_response

        endpoint = 'natural-gas/pri/fut/data/'
        headers = {
        'api_key': 'api_key',
        'frequency': 'daily',
        'data': ['value'],
        'facets': {
            'series': ['RNGWHHD',]
        },
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
        }
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = ''
        offset = 5000
        
        response = EIA.api_request(endpoint=endpoint,
        headers = headers,
        metadata_folder = metadata_folder,
        metadata_object_key = metadata_object_key,
        metadata_dataset_key = metadata_dataset_key,
        start_date_if_none = start_date_if_none,
        offset=offset)
        
        mock_requests_get.assert_called_once_with('https://api.eia.gov/v2/' + endpoint,
            headers={'X-Params': json.dumps({
                'api_key': 'api_key',
                'frequency': 'daily',
                'data': ['value'],
                'facets': {
                    'series': ['RNGWHHD',]
                },
                'sort': [{
                'column': 'period',
                'direction': 'asc'
                }],
                'length': 5000,
                'start_date': '1999-01-04',
                'offset': offset}),
                'Content-Type': 'application/json'},
            params = {'api_key': 'api_key'},
            timeout=30
        )
        assert response.status_code == 200
        assert response.json() == json.loads(mock_natural_gas_spot_prices_response)
    
    def test_eia_api_request_with_latest_end_date_none(self, mock_environment_variables, mock_requests_get, mock_get_latest_end_date, mock_natural_gas_spot_prices_response):
        ''' Test api_request method of EIA class where latest end date is specified for a given dataset key and non-default offset '''
        mock_get_latest_end_date.return_value = None
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = mock_natural_gas_spot_prices_response
        mock_requests_get.return_value = mock_response

        endpoint = 'natural-gas/pri/fut/data/'
        headers = {
        'api_key': 'api_key',
        'frequency': 'daily',
        'data': ['value'],
        'facets': {
            'series': ['RNGWHHD',]
        },
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
        }
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = '1999-01-04'
        offset = 0
        
        response = EIA.api_request(endpoint=endpoint,
        headers = headers,
        metadata_folder = metadata_folder,
        metadata_object_key = metadata_object_key,
        metadata_dataset_key = metadata_dataset_key,
        start_date_if_none = start_date_if_none,
        offset=offset)
        
        mock_requests_get.assert_called_once_with('https://api.eia.gov/v2/' + endpoint,
            headers={'X-Params': json.dumps({
                'api_key': 'api_key',
                'frequency': 'daily',
                'data': ['value'],
                'facets': {
                    'series': ['RNGWHHD',]
                },
                'sort': [{
                'column': 'period',
                'direction': 'asc'
                }],
                'length': 5000,
                'start_date': '1999-01-04',
                'offset': offset}),
                'Content-Type': 'application/json'},
            params = {'api_key': 'api_key'},
            timeout=30
        )
        assert response.status_code == 200
        assert response.json() == json.loads(mock_natural_gas_spot_prices_response)

    def test_eia_api_request_error(self, mock_environment_variables, mock_requests_get):
        ''' Test api_request method of EIA class where an error is produced for a given request '''
        mock_requests_get.side_effect = requests.RequestException("API Error")
        
        endpoint = 'natural-gas/pri/fut/data/'
        headers = {
        'api_key': 'api_key',
        'frequency': 'daily',
        'data': ['value'],
        'facets': {
            'series': ['RNGWHHD',]
        },
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
        }
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = '1999-01-04'
        offset = 0
        
        response = EIA.api_request(endpoint=endpoint, 
        headers=headers,
        metadata_folder = metadata_folder,
        metadata_object_key = metadata_object_key,
        metadata_dataset_key = metadata_dataset_key,
        start_date_if_none = start_date_if_none,
        offset=offset)
        
        assert response == ('Error occurred', requests.RequestException("API Error"))

    def test_eia_api_get_max_date_with_data(self, mock_natural_gas_spot_prices_response):
        ''' Test get_max_period method of EIA class where data is not None '''
        data = mock_natural_gas_spot_prices_response 
        result = EIA.get_max_date(data=data)
        assert result == '1999-01-05'
    
    def test_eia_api_get_max_date_with_no_data(self):
        ''' Test get_max_period method of EIA class where data is None '''
        data = []
        result = EIA.get_max_date(data=data)
        assert result is None

    def test_eia_extract_success(self, mock_environment_variables, mock_requests_get, mock_boto3_s3, mock_natural_gas_spot_prices_response,
        mock_update_metadata):
        ''' Test eia_extract method of EIA class where max_period is not None '''
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = json.loads(mock_natural_gas_spot_prices_response)
        mock_requests_get.side_effect = [mock_response, mock_response]

        endpoint = 'natural-gas/pri/fut/data/'
        folder = 'full_program/extraction'
        object_key = 'natural_gas_spot_prices'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = '1999-01-04'
        offset = 0

        EIA.extract(endpoint=endpoint, headers = {
        'api_key': 'api_key',
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
        }, folder=folder, object_key=object_key,
        metadata_folder=metadata_folder, metadata_object_key=metadata_object_key,
        metadata_dataset_key=metadata_dataset_key, start_date_if_none=start_date_if_none,
        offset=offset)
        
        assert mock_requests_get.call_count == 2
        mock_boto3_s3().put_data.assert_called_once_with(
        data=mock_natural_gas_spot_prices_response,
        folder='natural-gas/pri/fut/data/',
        object_key='natural_gas_spot_prices'
        )
        mock_update_metadata.assert_called_once_with(
        folder=metadata_folder,
        object_key=metadata_object_key,
        dataset_key=object_key,
        new_date='2024-04-26'
        )
    
    def test_eia_extract_no_data(self, mock_requests_get, mock_boto3_s3, mock_environment_variables, mock_update_metadata):
        ''' Test eia_extract method of EIA class where max_period is None '''
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"response": {"data": []}}
        mock_requests_get.return_value = mock_response
        mock_requests_get.side_effect = [mock_response, mock_response]
        
        endpoint = 'natural-gas/pri/fut/data/'
        headers = {'Authorization': 'Bearer token'}
        folder = 'full_program/extraction'
        object_key = 'natural_gas_spot_prices'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = '1999-01-04'
        offset = 0
        
        EIA.extract(endpoint=endpoint, headers=headers, folder=folder, object_key=object_key, 
                    metadata_folder=metadata_folder, metadata_object_key=metadata_object_key,
                    metadata_dataset_key=metadata_dataset_key, start_date_if_none=start_date_if_none,
                    offset=offset)
        
        assert mock_requests_get.call_count == 2
        mock_boto3_s3().put_data.assert_not_called()
        mock_update_metadata.assert_not_called()

class TestNOAA:
    def test_noaa_api_request_success(self, mock_environment_variables, mock_requests_get, mock_noaa_daily_weather_data_response):
        ''' Test api_request method of NOAA class where api request is successful '''
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = mock_noaa_daily_weather_data_response
        mock_requests_get.return_value = mock_response

        parameters = {'datasetid': 'GHCND',
        'datatypeid': ['AWND'],
        'stationid': 'GHCND:USW00094847',
        'startdate': '1999-01-04',
        'enddate': '2024-05-24',
        'units': 'metric',
        'limit': 1000}

        response = NOAA.api_request(parameters=parameters)
        
        mock_requests_get.assert_called_once_with('https://www.ncei.noaa.gov/cdo-web/api/v2/data',
        headers={'token': 'token'},
        params=parameters,
        timeout=7)
        assert response.status_code == 200
        assert response.json() == json.loads(mock_noaa_daily_weather_data_response)
    
    def test_noaa_api_request_timeout_failure(self, mock_environment_variables, mock_requests_get):
        ''' Test api_request method of NOAA class where a timeout error is produced for a given request '''
        mock_requests_get.side_effect = requests.exceptions.Timeout
        
        parameters = {'datasetid': 'GHCND',
        'datatypeid': ['AWND'],
        'stationid': 'GHCND:USW00094847',
        'startdate': '1999-01-04',
        'enddate': '2024-05-24',
        'units': 'metric',
        'limit': 1000}
        
        response = NOAA.api_request(parameters=parameters)
        
        assert mock_requests_get.call_count == 2
        assert response is None
    
    def test_noaa_api_request_no_timeout_failure(self, mock_environment_variables, mock_requests_get):
        ''' Test api_request method of NOAA class where a timeout error is produced for a given request '''
        mock_requests_get.side_effect = requests.RequestException("API Error")
        
        parameters = {'datasetid': 'GHCND',
        'datatypeid': ['AWND'],
        'stationid': 'GHCND:USW00094847',
        'startdate': '1999-01-04',
        'enddate': '2024-05-24',
        'units': 'metric',
        'limit': 1000}

        response = NOAA.api_request(parameters=parameters)
        assert response == ('Error occurred', requests.RequestException("API Error"))
    
    def test_noaa_api_get_max_period_with_data(self, mock_noaa_daily_weather_data_response):
        ''' Test get_max_period method of NOAA class where data is not None '''
        data = mock_noaa_daily_weather_data_response 
        result = NOAA.get_max_date(data=data)
        assert result == '2024-05-24'
    
    def test_noaa_api_get_max_period_with_no_data(self):
        ''' Test get_max_period method of NOAA class where data is None '''
        data = []
        result = NOAA.get_max_date(data=data)
        assert result is None

    def test_noaa_extract_success_with_latest_end_date(self, mock_environment_variables,
                                                       ):
        ''' Test extract method of NOAA class where latest end date is not None '''







        
    










