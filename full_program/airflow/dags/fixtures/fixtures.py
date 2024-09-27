''' Import modules'''
import os
import json
import pytest
import requests_mock
import boto3
import pandas as pd
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

@pytest.fixture
def df_etl_utils_testing():
    ''' Dataframe to be used for testing of EtlUtils class '''
    data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-06', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': None, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2024-05-24', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'}]
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def df_noaa_transformation_testing():
    ''' Dataframe to be used for testing of NoaaTransformation class '''
    data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2000-01-02', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -2, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.1, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-04', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.4, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '1999-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.2, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2000-01-02', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.1, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -10, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': -3, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2001-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2001-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 1, 'city': 'New York', 'state': 'New York'},
    {'date': '2001-04-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -5, 'city': 'New York', 'state': 'New York'},
    {'date': '2001-07-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': -1, 'city': 'New York', 'state': 'New York'},
    {'date': '2001-10-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': -3, 'city': 'New York', 'state': 'New York'},
    {'date': '2001-10-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 3.1, 'city': 'New York', 'state': 'New York'}]
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def df_noaa_transformation_testing_impute_missing_weather_variables():
    ''' 
    Dataframe to be used for testing of impute_missing_weather_variables method
    of NoaaTransformation class
    '''
    data = [{'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -2, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': None, 'city': 'Detroit', 'state': 'Michigan'}]
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def df_eia_transformation_testing():
    '''
    Dataframe to be used for testing of EiaTransformation class
    '''
    data = [{'period': '2014-05-01', 'LNG Imports (Price)': 5.99},
    {'period': '2014-06-01', 'LNG Imports (Price)': 10.48},
    {'period': '2014-07-01', 'LNG Imports (Price)': 11.80},
    {'period': '2014-08-01', 'LNG Imports (Price)': 6.56},
    {'period': '2014-09-01', 'LNG Imports (Price)': 8.73},
    {'period': '2014-10-01', 'LNG Imports (Price)': 4.63},
    {'period': '2014-11-01', 'LNG Imports (Price)': None},
    {'period': '2014-12-01', 'LNG Imports (Price)': 7.45},
    {'period': '2015-01-01', 'LNG Imports (Price)': 10.90},
    {'period': '2015-02-01', 'LNG Imports (Price)': 9.13},
    {'period': '2015-03-01', 'LNG Imports (Price)': 8.10},
    {'period': '2015-04-01', 'LNG Imports (Price)': 8.31},
    {'period': '2015-01-01', 'LNG Imports (Price)': 6.04}]
    df = pd.DataFrame(data)
    return df








