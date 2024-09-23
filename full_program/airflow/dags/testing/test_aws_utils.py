''' Import modules '''
import json
import pytest
from unittest.mock import MagicMock
from utils.aws import *
from fixtures.fixtures import mock_environment_variables, mock_boto3_s3, mock_get_data, mock_natural_gas_spot_prices_response, mock_metadata_response

class TestS3:
    ''' Test class for testing S3 class '''
    def test_s3_connect(self, mock_environment_variables, mock_boto3_s3):
        ''' Test for connection method of S3 class '''
        S3.connect()
        assert S3.s3_client is not None
        S3.s3_client.assert_called_once_with(
            's3', aws_access_key_id='access-key', aws_secret_access_key='secret-key'
        )

    def test_disconnect(self, mock_environment_variables, mock_boto3_s3):
        ''' Test for disconnect method of S3 class '''
        S3.connect()
        assert S3.s3_client is not None
        S3.disconnect()
        assert not hasattr(S3, 's3_client')

    def test_get_data_valid(self, mock_environment_variables, mock_boto3_s3, mock_natural_gas_spot_prices_response):
        ''' Test for get_data method of S3 class for valid folder and object key '''
        mock_s3 = mock_boto3_s3.return_value
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: mock_natural_gas_spot_prices_response.encode('utf-8'))
        }

        folder = 'extraction/'
        object_key = 'natural_gas_spot_prices'
        result = list(S3.get_data(folder=folder, object_key=object_key))  # list to get data from the generator

        mock_s3.get_object.assert_called_once_with(Bucket='bucket', Key=folder + object_key)
        assert result == mock_natural_gas_spot_prices_response['response']['data']

    def test_get_data_client_error(self, mock_environment_variables, mock_boto3_s3):
        ''' Test for get_data method of S3 class for invalid folder and object key'''
        mock_boto3_s3.get_object.side_effect= Exception('Client Error')

        folder = 'invalid_folder/'
        object_key = 'invalid_key'
        result = S3.get_data(folder=folder, object_key=object_key)

        assert result == {}

    def test_put_data(self, mock_environment_variables, mock_boto3_s3):
        ''' Test for get_data method of S3 class '''
        mock_s3 = mock_boto3_s3.return_value

        data = [{'key': 'value'}]
        folder = 'extraction/'
        object_key = 'natural_gas_spot_prices'
        
        S3.put_data(data, folder, object_key)

        mock_s3.put_object.assert_called_once_with(
            Bucket='bucket', Key=folder + object_key, 
            Body=json.dumps(data), ContentType='application/json'
        )

class TestS3Metadata:
    ''' Test class for testing S3Metadata class '''
    def test_get_metadata(self, mock_environment_variables, mock_get_data, mock_metadata_response):
        ''' Test for get_metadata method of S3Metadata class with successful response '''
        response = json.loads(mock_metadata_response)
        mock_get_data.return_value = response

        folder = 'metadata/'
        object_key = 'metadata'

        result = S3Metadata.get_metadata(folder=folder, object_key=object_key)

        mock_get_data.assert_called_once_with(folder=folder, object_key=object_key)
        assert result == response
    
    def test_get_latest_end_date_single_date(self, mock_environment_variables, mock_get_data, mock_metadata_response):
        ''' Test for get_latest_end_date method of S3Metadata class where a dataset key has only a single end date '''
        response = json.loads(mock_metadata_response)
        mock_get_data.return_value = response

        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'natural_gas_spot_prices'

        result = S3Metadata.get_latest_end_date(folder=folder, object_key=object_key, dataset_key=dataset_key)
        mock_get_data.assert_called_once_with(folder=folder, object_key=object_key)
        assert result == '2024-05-24'
    
    def test_get_latest_end_date_multiple_dates(self, environment_variables, mock_get_data, mock_metadata_response):
        ''' Test for get_latest_end_date method of S3Metadata class where dataset key exists and there are multiple end dates '''
        response = json.loads(mock_metadata_response)
        mock_get_data.return_value = response

        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'daily_weather'

        result = S3Metadata.get_latest_end_date(folder=folder, object_key=object_key, dataset_key=dataset_key)
        mock_get_data.assert_called_once_with(folder=folder, object_key=object_key)
        assert result == '2024-05-23'
    
    def test_get_latest_end_date_no_dates(self, environment_variables, mock_get_data, mock_metadata_response):
        ''' Test for get_latest_end_date method of S3Metadata class where dataset key exists but there are no end dates '''
        response = json.loads(mock_metadata_response)
        mock_get_data.return_value = response

        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'natural_gas_rigs_in_operation'

        result = S3Metadata.get_latest_end_date(folder=folder, object_key=object_key, dataset_key=dataset_key)
        mock_get_data.assert_called_once_with(folder=folder, object_key=object_key)
        assert result == []

    def test_get_latest_end_date_no_dataset_key(self, environment_variables, mock_get_data, mock_metadata_response):
        ''' Test for get_latest_end_date method of S3Metadata class where no dataset key exists '''
        response = json.loads(mock_metadata_response)
        mock_get_data.return_value = response

        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'invalid_dataset_key'

        result = S3Metadata.get_latest_end_date(folder=folder, object_key=object_key, dataset_key=dataset_key)
        mock_get_data.assert_called_once_with(folder=folder, object_key=object_key)
        assert result == []

    def test_update_metadata_dataset_key_exists_with_dates(self, mock_environment_variables, mock_get_data, mock_boto3_s3, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key already exists in metadata '''
        mock_get_data.return_value = json.loads(mock_metadata_response)
        mock_put_object = mock_boto3_s3.return_value.put_object

        new_date = '2024-06-01'
        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'natural_gas_spot_prices'

        S3Metadata.update_metadata(folder=folder, object_key=object_key, dataset_key=dataset_key, new_date=new_date)
        
        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': ['1999-01-04', '2024-06-01'],
            'natural_gas_rigs_in_operation': [],
            'natural_gas_monthly_variables': ['2024-03'],
            'daily_weather': ['2024-05-21']
        }
        mock_put_object.assert_called_once_with(
            Bucket='bucket',
            Key='full_program/metadata/metadata',
            Body=json.dumps(expected_metadata)
        )
    
    def test_update_metadata_dataset_key_exists_with_no_dates(self, mock_environment_variables, mock_get_data, mock_boto3_s3, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key already exists in metadata '''
        mock_get_data.return_value = json.loads(mock_metadata_response)
        mock_put_object = mock_boto3_s3.return_value.put_object

        new_date = '2024-06'
        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'natural_gas_rigs_in_operation'

        S3Metadata.update_metadata(folder=folder, object_key=object_key, dataset_key=dataset_key, new_date=new_date)

        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': ['1999-01-04'],
            'natural_gas_rigs_in_operation': ['2024-06'],
            'natural_gas_monthly_variables': ['2024-03'],
            'daily_weather': ['2024-05-21']
        }
        mock_put_object.assert_called_once_with(
            Bucket='bucket',
            Key='full_program/metadata/metadata',
            Body=json.dumps(expected_metadata)
        )
        
        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': ['1999-01-04'],
            'natural_gas_rigs_in_operation': ['2024-06'],
            'natural_gas_monthly_variables': ['2024-03'],
            'daily_weather': ['2024-05-21']
        }
        mock_put_object.assert_called_once_with(
            Bucket='bucket',
            Key='full_program/metadata/metadata',
            Body=json.dumps(expected_metadata)
        )
    
    def test_update_metadata_dataset_key_not_exist(self, mock_environment_variables, mock_get_data, mock_boto3_s3, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key doesn't exist in metadata '''
        mock_get_data.return_value = json.loads(mock_metadata_response)
        mock_put_object = mock_boto3_s3.return_value.put_object

        new_date = '2024-06-01'
        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'new_dataset_key'

        S3Metadata.update_metadata(folder=folder, object_key=object_key, dataset_key=dataset_key, new_date=new_date)
        
        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': ['1999-01-04'],
            'natural_gas_rigs_in_operation': [],
            'natural_gas_monthly_variables': ['2024-03'],
            'daily_weather': ['2024-05-21'],
            'new_dataset_key': ['2024-06-01']
        }
        mock_put_object.assert_called_once_with(
            Bucket='bucket',
            Key='full_program/metadata/metadata',
            Body=json.dumps(expected_metadata)
        )
        

        

    





        











