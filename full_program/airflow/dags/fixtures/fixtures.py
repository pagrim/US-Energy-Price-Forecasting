''' Import modules'''
import os
import json
import pytest
import requests_mock
import boto3
from unittest.mock import patch, MagicMock
from utils.aws import S3, S3Metadata

@pytest.fixture
def mock_environment_variables(mocker):
    ''' Mocks environment variables for testing '''
    mocker.patch.dict(os.environ, {
        'AWS_ACCESS_KEY_ID': 'access-key',
        'AWS_SECRET_ACCESS_KEY': 'secret-key',
        'S3_BUCKET': 'bucket',
        'API_KEY': 'api_key',
        'TOKEN': 'token'
    })

@pytest.fixture
def mock_boto3_s3(mocker):
    ''' Mocks S3 bucket for testing '''
    mock_s3 = mocker.patch('boto3.client', return_value=MagicMock())
    yield mock_s3

@pytest.fixture
def mock_get_data(mocker):
    ''' Mocks get_data method of S3 object '''
    mock_s3_get_data = mocker.patch.object(S3, 'get_data')
    return mock_s3_get_data

@pytest.fixture
def mock_requests_get(mocker):
    ''' Mocks requests.get for testing '''
    return mocker.patch('requests.get')

@pytest.fixture
def mock_get_latest_end_date(mocker):
    ''' Mocks get_latest_end_date method of S3Metadata class '''
    mock_s3metadata_get_latest_end_date = mocker.patch.object(S3Metadata, 'get_latest_end_date')
    return mock_s3metadata_get_latest_end_date

@pytest.fixture
def mock_update_metadata(mocker):
    ''' Mocks update_metadata method of S3Metadata class '''
    mock_s3metadata_get_latest_end_date = mocker.patch.object(S3Metadata, 'update_metadata')
    return mock_s3metadata_get_latest_end_date

@pytest.fixture
def mock_eia_headers():
    ''' Mocks headers used in EIA api_request and extract methods '''
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
    return headers

@pytest.fixture
def mock_noaa_parameters():
    ''' Mocks parameters used in NOAA api_request and extract methods '''
    parameters = {'datasetid': 'GHCND',
        'datatypeid': ['AWND'],
        'stationid': 'GHCND:USW00094847',
        'enddate': '2024-05-24',
        'units': 'metric',
        'limit': 1000}
    return parameters

@pytest.fixture
def mock_natural_gas_spot_prices_response():
    ''' Mocks data for natural gas spot prices response in EIA API '''
    data = [{"period": "1999-01-04", "duoarea": "RGC", "area-name": "NA", "product": "EPG0", 
            "product-name": "Natural Gas", "process": "PS0", "process-name": "Spot Price", 
            "series": "RNGWHHD", 
            "series-description": "Henry Hub Natural Gas Spot Price (Dollars per Million Btu)",
            "value": "2.1", "units": "$/MMBTU"},
            {"period": "1999-01-05", "duoarea": "RGC", "area-name": "NA", "product": "EPG0",
            "product-name": "Natural Gas", "process": "PS0", "process-name": "Spot Price",
            "series": "RNGWHHD", 
            "series-description": "Henry Hub Natural Gas Spot Price (Dollars per Million Btu)",
            "value": "2.06", "units": "$/MMBTU"}]
    response_dict = {"response": {"data": data}}
    response_json = json.dumps(response_dict)
    return response_json

@pytest.fixture
def mock_noaa_daily_weather_data_response():
    ''' Mocks data for noaa daily weather data response in NOAA API '''
    data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city':'Detroit', 'state': 'Michigan'},
            {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city':'Detroit', 'state': 'Michigan'},
            {'date': '2024-05-24', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city':'Detroit', 'state': 'Michigan'}]
    response_dict = {"results": data}
    response_json = json.dumps(response_dict)
    return response_json

@pytest.fixture
def mock_metadata_response():
    ''' Mocks metadata that is retrieved from S3 bucket to get latest load date for a given dataset '''
    data = {
        'natural_gas_spot_prices': ['1999-01-04'],
        'natural_gas_rigs_in_operation': [],
        'natural_gas_monthly_variables': ['2024-03'],
        'daily_weather': ['2024-05-21', '2024-05-23']
    }
    response_json = json.dumps(data)
    return response_json




