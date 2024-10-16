''' Import modules '''
import json
import pytest
import pandas as pd
from transformation.etl_transforms import *
from fixtures.fixtures import df_etl_transforms_testing, merge_dataframes_natural_gas_spot_prices_df_no_missing_date, merge_dataframes_natural_gas_spot_prices_df_missing_date, \
merge_dataframes_heating_oil_spot_prices_df_no_missing_date, merge_dataframes_heating_oil_spot_prices_df_missing_date, merge_dataframes_natural_gas_monthly_variables_df_no_missing_date, \
merge_dataframes_natural_gas_monthly_variables_df_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_missing_date, \
merge_dataframes_daily_weather_df_no_missing_date, merge_dataframes_daily_weather_df_missing_date

class TestEtlTransforms:
    ''' Test class for testing EtlUtils class '''
    def test_json_to_df(self, df_etl_transforms_testing):
        '''
        Tests json_to_df function of EtlUtils class
        '''
        json_data = df_etl_transforms_testing.to_json(orient='records')
        result_df = EtlTransforms.json_to_df(data=json_data)
        pd.testing.assert_frame_equal(result_df, df_etl_transforms_testing)
    
    def test_df_to_json(self, df_etl_transforms_testing):
        '''
        Tests df_to_json function of EtlUtils class
        '''
        expected_json = df_etl_transforms_testing.to_json(orient='records')
        result_json = EtlTransforms.df_to_json(df=df_etl_transforms_testing)
        assert expected_json == result_json

    def test_drop_columns(self, df_etl_transforms_testing):
        '''
        Tests drop_columns function of EtlUtils class
        '''
        data = [{'date': '1999-01-04', 'datatype': 'AWND', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-04', 'datatype': 'AWND', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-05', 'datatype': 'AWND', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-06', 'datatype': 'AWND', 'value': None, 'city': 'Detroit', 'state': 'Michigan'}
        {'date': '2024-05-24', 'datatype': 'AWND', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'}]
        expected_df = pd.DataFrame(data)
        result_df = EtlTransforms.drop_columns(df=df_etl_transforms_testing, columns=['station'])
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_rename_columns(self, df_etl_transforms_testing):
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
        result_df = EtlTransforms.rename_columns(df=df_etl_transforms_testing, renamed_columns=renamed_columns)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_pivot_columns(self, df_etl_transforms_testing):
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
        result_df = EtlTransforms.pivot_columns(df=df_etl_transforms_testing, index=index, column=column, value=value)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_drop_null(self, df_etl_transforms_testing):
        '''
        Tests drop_null function of EtlUtils class
        '''
        data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2024-05-24', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'}]
        expected_df = pd.DataFrame(data)
        result_df = EtlTransforms.drop_null(df=df_etl_transforms_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_date_index(self, df_etl_transforms_testing):
        '''
        Tests set_date_index function of EtlUtils class
        '''
        expected_df = df_etl_transforms_testing.set_index('date')
        expected_index = pd.to_datetime(['1991-01-04', '1999-01-04', '1999-01-05', '1999-01-06', '2024-05-24'])
        result_df = EtlTransforms.set_date_index(df=df_etl_transforms_testing)
        result_index = result_df.index
        pd.testing.assert_index_equal(result_index, expected_index)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_merge_dataframes_no_missing_dates_all_datasets(self, merge_dataframes_daily_weather_df_no_missing_date, merge_dataframes_natural_gas_monthly_variables_df_no_missing_date, 
    merge_dataframes_natural_gas_spot_prices_df_no_missing_date, 
    merge_dataframes_heating_oil_spot_prices_df_no_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date):
        '''
        Tests merge_dataframes function where all datasets to be merged don't contain any missing dates
        '''
        data = {
        'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13', '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', 
        '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26', '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05', '1999-02-08', 
        '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18', '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', 
        '1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12', '1999-03-15', '1999-03-16', '1999-03-17', 
        '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 
        1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 
        1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 
        0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 
        0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'imports': [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 
        2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 
        3000, 3000, 3000, 4000],
        'lng_imports': [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 60, 60, 
        60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 80],
        'residential_consumption': [2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 
        2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 
        2.04, 2.04, 2.04, 2.04, 2.04, 1.91],
        'commerical_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 
        475945.0, 475945.0, 475945.0, 475945.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 
        475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 
        475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475980.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 
        6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 
        6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 
        6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404500.0],
        'natural_gas_rigs_in_operation': [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 2000, 2000, 2000, 2000, 2000, 
        2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 
        3000, 3000, 3000, 3000, 3000, 3000, 3000, 4000],
        'state': ['Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 
        'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 
        'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 
        'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida'],
        'city': ['Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 
        'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 
        'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 
        'Miami', 'Miami', 'Miami', 'Miami'],
        'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 
        5, 4, 5, 4, 5, 4, 5],
        'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 
        4, 5, 4, 5],
        'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 
        4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.merge_dataframes(daily_weather_df=merge_dataframes_daily_weather_df_no_missing_date, 
        natural_gas_monthly_variables_df=merge_dataframes_natural_gas_monthly_variables_df_no_missing_date,
        natural_gas_rigs_in_operation_df=merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date,
        natural_gas_spot_prices_df=merge_dataframes_natural_gas_spot_prices_df_no_missing_date,
        heating_oil_spot_prices_df=merge_dataframes_heating_oil_spot_prices_df_no_missing_date)
        pd.testing.assert_frame_equal(result_df, expected_df)
        







        




    
        


