''' Import modules '''
import pytest
import pandas as pd
from transformation.noaa_api_transformation import *
from transformation.eia_api_transformation import *
from fixtures.fixtures import df_noaa_transformation_testing, df_noaa_transformation_testing_impute_missing_weather_variables, df_eia_transformation_testing

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
    
    

    

    










