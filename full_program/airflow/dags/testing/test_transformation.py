''' Import modules '''
import pytest
import pandas as pd
from transformation.noaa_api_transformation import *
from transformation.eia_api_transformation import *
from fixtures.fixtures import df_noaa_transformation_testing, df_noaa_transformation_testing_impute_missing_weather_variables, df_eia_transformation_testing, df_noaa_feature_engineering_testing

class TestNoaaTransformation:
    ''' 
    Test class for testing NoaaTransformation class 
    '''
    def test_modify_date(self, df_noaa_transformation_testing):
        ''' 
        Tests modify_date function of NoaaTransformation class
        '''
        expected_quarters = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 4]
        df = NoaaTransformation.modify_date(df=df_noaa_transformation_testing)
        assert pd.api.types.is_datetime64_any_dtype(df['date'])
        assert df['quarter'].tolist() == expected_quarters
        assert df.shape == (21, 7)

    def test_imputation_df(self, df_noaa_transformation_testing):
        '''
        Tests imputation_df function of NoaaTransformation class
        '''
        data = [{'city': 'Detroit' , 'quarter': 1, 'datatype': 'TMIN', 'impute_method': 'Mean', 'impute value': -2},
        {'city': 'Detroit' , 'quarter': 1, 'datatype': 'TMAX', 'impute_method': 'Mean', 'impute value': 5},
        {'city': 'Detroit' , 'quarter': 1, 'datatype': 'AWND', 'impute_method': 'Mean', 'impute value': 4.17},
        {'city': 'Detroit' , 'quarter': 1, 'datatype': 'SNOW', 'impute_method': 'Median', 'impute value': 0.1},
        {'city': 'Chicago' , 'quarter': 1, 'datatype': 'TMIN', 'impute_method': 'Mean', 'impute value': -10},
        {'city': 'Chicago' , 'quarter': 1, 'datatype': 'TMAX', 'impute_method': 'Mean', 'impute value': -3},
        {'city': 'Chicago' , 'quarter': 1, 'datatype': 'AWND', 'impute_method': 'Mean', 'impute value': 5},
        {'city': 'Chicago' , 'quarter': 1, 'datatype': 'SNOW', 'impute_method': 'Median', 'impute value': 0.2},
        {'city': 'New York' , 'quarter': 2, 'datatype': 'TMIN', 'impute_method': 'Mean', 'impute value': -5}, 
        {'city': 'New York' , 'quarter': 3, 'datatype': 'TMAX', 'impute_method': 'Mean', 'impute value': -1},
        {'city': 'New York' , 'quarter': 4, 'datatype': 'AWND', 'impute_method': 'Mean', 'impute value': 3.1},
        {'city': 'New York' , 'quarter': 1, 'datatype': 'SNOW', 'impute_method': 'Median', 'impute value': 1}
        ]
        expected_df = pd.DataFrame(data)
        result_df = NoaaTransformation.imputation_df(df=df_noaa_transformation_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_impute_missing_weather_variables(self, df_noaa_transformation_testing_impute_missing_weather_variables):
        ''' 
        Tests impute_missing_weather_variables function of NoaaTransformation class
        '''
        data = [{'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -2, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2001-01-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': None, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2001-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.17, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2001-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.1, 'city': 'Detroit', 'state': 'Michigan'}]
        expected_df = pd.DataFrame(data)
        imputation_df = NoaaTransformation.imputation_df(df=df_noaa_transformation_testing_impute_missing_weather_variables)
        result_df = NoaaTransformation.impute_missing_weather_variables(df=df_noaa_transformation_testing_impute_missing_weather_variables, imputation_df=imputation_df)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_calculate_missing_tavg(self, df_noaa_transformation_testing):
        '''
        Tests calculate_missing_tavg function of NoaaTransformation class
        '''
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
        {'date': '2001-10-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 3.1, 'city': 'New York', 'state': 'New York'},
        {'date': '2001-10-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': 1.5, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2001-10-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': -6.5, 'city': 'Chicago', 'state': 'Illinois'}]
        expected_df = pd.DataFrame(data)
        result_df = NoaaTransformation.calculate_missing_tavg(df=df_noaa_transformation_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_maximum_hdd(self, df_noaa_feature_engineering_testing):
        '''
        Tests maximum_hdd function of NooaTransformation class
        '''
        data = [{'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
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
        'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'hdd': [18.33, 3.33, 1.33, 18.33, 5.33, 3.33, 1.33, 2.33, 2.33, 1.33, 1.33, 1.33, 2.33, 1.33, 0, 21.33, 19.33, 3.33, 12.33, 11.33, 18.33, 0, 28.33, 24.33, 0, 33.33, 28.33],
        'max_hdd': [18.33, 18.33, 18.33, 18.33, 18.33, 18.33, 2.33, 2.33, 2.33, 1.33, 1.33, 1.33, 2.33, 2.33, 2.33, 21.33, 21.33, 21.33, 18.33, 18.33, 18.33, 28.33, 28.33, 28.33, 33.33, 33.33, 33.33]}]
        expected_df = pd.DataFrame(data)
        result_df = NoaaTransformation.maximum_hdd(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_maximum_cdd(self, df_noaa_feature_engineering_testing):
        '''
        Tests maximum_cdd function of NooaTransformation class
        '''
        data = [{'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
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
        'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'cdd': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11.67, 0, 0, 0, 0, 0, 0, 6.67, 0, 0, 3.67, 0, 0],
        'max_cdd': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11.67, 11.67, 11.67, 0, 0, 0, 0, 0, 0, 6.67, 6.67, 6.67, 3.67, 3.67, 3.67]}]
        expected_df = pd.DataFrame(data)
        result_df = NoaaTransformation.maximum_cdd(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_wci_sum(self, df_noaa_feature_engineering_testing):
        '''
        Tests wci_sum function of NoaaTransformation class
        '''
        data = [{'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
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
        'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'wci_sum': [16, 16, 16, 19, 19, 19, 16, 16, 16, 3, 3, 3, 9, 9, 9, 9, 9, 9, 24, 24, 24, 32, 32, 32, 17, 17, 17]}]
        expected_df = pd.DataFrame(data)
        result_df = NoaaTransformation.wci_sum(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_snow_sum(self, df_noaa_feature_engineering_testing):
        '''
        Tests snow_sum function of NoaaTransformation class
        '''
        data = [{'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
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
        'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'snow_sum': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 3, 3, 3, 12, 12, 12]}]
        expected_df = pd.DataFrame(data)
        result_df = NoaaTransformation.snow_sum(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_min_and_max_average_temperature(self, df_noaa_feature_engineering_testing):
        '''
        Tests min_and_max_average_temperature function of NoaaTransformation class
        '''
        data = [{'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
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
        'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'min_tavg': [15, 15, 15, 13, 13, 13, 16, 16, 16, 17, 17, 17, 16, 16, 16, -3, -3, -3, 6, 6, 6, -10, -10, -10, -15, -15, -15],
        'max_tavg': [17, 17, 17, 15, 15, 15, 17, 17, 17, 17, 17, 17, 30, 30, 30, 15, 15, 15, 7, 7, 7, 25, 25, 25, 22, 22, 22]}]
        expected_df = pd.DataFrame(data)
        result_df = NoaaTransformation.min_and_max_average_temperature(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)



        

class TestEiaTransformation:
    ''' 
    Test class for testing EiaTransformation class 
    '''
    def test_impute_null_monthly_variable(self, df_eia_transformation_testing):
        '''
        Tests impute_null_monthly_variable function of EiaTransformation Class
        '''
        data = [{'period': '2014-05-01', 'LNG Imports (Price)': 5.99},
        {'period': '2014-06-01', 'LNG Imports (Price)': 10.48},
        {'period': '2014-07-01', 'LNG Imports (Price)': 11.80},
        {'period': '2014-08-01', 'LNG Imports (Price)': 6.56},
        {'period': '2014-09-01', 'LNG Imports (Price)': 8.73},
        {'period': '2014-10-01', 'LNG Imports (Price)': 4.63},
        {'period': '2014-11-01', 'LNG Imports (Price)': 8.21},
        {'period': '2014-12-01', 'LNG Imports (Price)': 7.45},
        {'period': '2015-01-01', 'LNG Imports (Price)': 10.90},
        {'period': '2015-02-01', 'LNG Imports (Price)': 9.13},
        {'period': '2015-03-01', 'LNG Imports (Price)': 8.10},
        {'period': '2015-04-01', 'LNG Imports (Price)': 8.31},
        {'period': '2015-01-01', 'LNG Imports (Price)': 6.04}]
        expected_df = pd.DataFrame(data)
        result_df = EiaTransformation.impute_null_monthly_variable(df=df_eia_transformation_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    

    

    










