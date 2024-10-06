''' Import modules '''
import json
import pytest
import pandas as pd
from utils.etl_utils import *
from fixtures.fixtures import df_etl_utils_testing

class TestEtlUtils:
    ''' Test class for testing EtlUtils class '''
    def test_json_to_df(self, df_etl_utils_testing):
        '''
        Tests json_to_df function of EtlUtils class
        '''
        json_data = df_etl_utils_testing.to_json(orient='records')
        result_df = EtlUtils.json_to_df(data=json_data)
        pd.testing.assert_frame_equal(result_df, df_etl_utils_testing)
    
    def test_df_to_json(self, df_etl_utils_testing):
        '''
        Tests df_to_json function of EtlUtils class
        '''
        expected_json = df_etl_utils_testing.to_json(orient='records')
        result_json = EtlUtils.df_to_json(df=df_etl_utils_testing)
        assert expected_json == result_json

    def test_drop_columns(self, df_etl_utils_testing):
        '''
        Tests drop_columns function of EtlUtils class
        '''
        data = [{'date': '1999-01-04', 'datatype': 'AWND', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-04', 'datatype': 'AWND', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-05', 'datatype': 'AWND', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-06', 'datatype': 'AWND', 'value': None, 'city': 'Detroit', 'state': 'Michigan'}
        {'date': '2024-05-24', 'datatype': 'AWND', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'}]
        expected_df = pd.DataFrame(data)
        result_df = EtlUtils.drop_columns(df=df_etl_utils_testing, columns=['station'])
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_rename_columns(self, df_etl_utils_testing):
        '''
        Tests rename_columns function from EtlUtils class
        '''
        renamed_columns = {'date': 'Date', 'datatype': 'DataType', 'station': 'Station', 'value': 'Value', 'city': 'City', 'state': 'State'}
        data = [{'Date': '1999-01-04', 'Datatype': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': 4.3, 'City':'Detroit', 'State': 'Michigan'},
        {'Date': '1999-01-04', 'Datatype': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': 4.3, 'City': 'Detroit', 'State': 'Michigan'},
        {'Date': '1999-01-05', 'Datatype': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': 4.2, 'City': 'Detroit', 'State': 'Michigan'},
        {'Date': '1999-01-06', 'Datatype': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': None, 'City': 'Detroit', 'State': 'Michigan'},
        {'Date': '2024-05-24', 'Datatype': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': 4.0, 'City': 'Detroit', 'State': 'Michigan'}]
        expected_df = pd.DataFrame(data)
        result_df = EtlUtils.rename_columns(df=df_etl_utils_testing, renamed_columns=renamed_columns)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_pivot_columns(self, df_etl_utils_testing):
        '''
        Tests pivot_columns function of EtlUtils class
        '''
        index = ['date']
        column = 'datatype'
        value = 'value'
        data = [{'date': '1999-01-04', 'AWND': 4.3},
        {'date': '1999-01-05', 'AWND': 4.2},
        {'date': '1999-01-06', 'AWND': None},
        {'date': '2024-05-24', 'AWND': 4.0}]
        expected_df = pd.DataFrame(data)
        expected_df = expected_df.set_index('date')
        result_df = EtlUtils.pivot_columns(df=df_etl_utils_testing, index=index, column=column, value=value)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_drop_null(self, df_etl_utils_testing):
        '''
        Tests drop_null function of EtlUtils class
        '''
        data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2024-05-24', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'}]
        expected_df = pd.DataFrame(data)
        result_df = EtlUtils.drop_null(df=df_etl_utils_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_date_index(self, df_etl_utils_testing):
        '''
        Tests set_date_index function of EtlUtils class
        '''
        expected_df = df_etl_utils_testing.set_index('date')
        expected_index = pd.to_datetime(['1991-01-04', '1999-01-04', '1999-01-05', '1999-01-06', '2024-05-24'])
        result_df = EtlUtils.set_date_index(df=df_etl_utils_testing)
        result_index = result_df.index
        pd.testing.assert_index_equal(result_index, expected_index)
        pd.testing.assert_frame_equal(result_df, expected_df)
    

        






        




    
        


