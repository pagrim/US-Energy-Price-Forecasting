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

@pytest.fixture
def df_noaa_feature_engineering_testing():
    '''
    Dataframe to be used for testing functions used for feature engineering in NoaaTransformation class
    Will be used to test the following functions:
        maximum_hdd(cls, df)
        maximum_cdd(cls, df)
        wci_sum(cls, df)
        snow_sum(cls, df)
        min_and_max_average_temperature(cls, df)
        max_abs_tavg_diff(cls, df)
        max_abs_tavg_diff_relative_daily_median(cls, df)
    '''
    data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
    '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
    '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
    'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
    'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
    'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
    'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
    'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
    'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
    'wci': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
    'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
    'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10]}
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def df_eia_feature_engineering_testing():
    '''
    Dataframe to be used for testing functions used for feature engineering in NoaaTransformation class
    Will be used to test the following functions:
        natural_gas_prices_lag(cls, df)
        heating_oil_natural_gas_price_ratio(cls, df)
        expotential_weighted_natural_gas_price_volatility(cls, df)
        rolling_average_natural_gas_price(cls, df)
        rolling_median_natural_gas_price(cls, df)
        total_consumption_to_total_underground_storage_ratio(cls, df)
    '''
    data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
    '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
    '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
    '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
    '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
    '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
    '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
    '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
    'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
    'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
    'residential_consumption': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
    'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
    'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0]}
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df
    









