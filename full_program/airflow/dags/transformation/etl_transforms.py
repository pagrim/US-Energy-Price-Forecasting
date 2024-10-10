''' Import modules '''
import json
import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler

class EtlTransforms:
    '''
    Class containing general purpose functions used in ETL process that are not specific 
    to noaa and eia api's

    Methods
    -------
    json_to_df(cls, data):
        Converts json data to a dataframe
    df_to_json(cls, df):
        Converts dataframe to a json file
    drop_columns(cls, df, columns):
        Drops columns from dataframe and returns modified dataframe
    drop_duplicates(cls, df):
        Drops duplicate rows from dataframe and returns modified dataframe
    rename_columns(cls, df):
        Renames columns in dataframe and returns dataframe with new column names
    pivot_columns(cls, df):
        Pivots columns in dataframe and returns modified dataframe
    drop_null(cls, df):
        Drops all null rows in dataframe and returns modified dataframe
    '''
    @classmethod
    def json_to_df(cls, data: json) -> pd.DataFrame:
        '''
        Converts json data to a dataframe

        Args:
            data (json): Data in json format to be converted to dataframe
        
        Returns:
            pd.DataFrame: Returns json data as a dataframe
        '''
        json_data = next(data)
        df = pd.DataFrame(json_data)
        return df
    
    @classmethod
    def df_to_json(cls, df: pd.DataFrame) -> json:
        '''
        Converts dataframe to a json file

        Args:
            df (pd.DataFrame): Pandas dataframe to be converted to json format

        Returns:
            json: Data in json format
        '''
        json_data = df.to_json(orient='records')
        return json_data
    
    @classmethod
    def drop_columns(cls, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        '''
        Drops columns from dataframe and returns modified dataframe

        Args:
          df (pd.DataFrame): Pandas dataframe where columns are going to be dropped
          columns (list): List of columns to be removed

        Returns:
            pd.DataFrame: Dataframe with specific columns removed
        '''
        df = df.drop(columns=columns, axis=1)
        return df
    
    @classmethod
    def drop_duplicates(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Drops duplicate rows from dataframe and returns modified dataframe

        Args:
          df (pd.DataFrame): Pandas dataframe where duplicated rows are going to be dropped

        Returns:
            pd.DataFrame: Dataframe with specific columns removed
        '''
        df = df.drop_duplicates(axis=1)
        return df
    
    @classmethod
    def rename_columns(cls, df: pd.DataFrame, renamed_columns: dict) -> pd.DataFrame:
        '''
        Renames columns in a given dataframe

        Args:
            df (pd.DataFrame): Pandas dataframe where columns are going to be renamed
            renamed_columns (dict): Key value pair representing original column name and new column name
        
        Returns:
            pd.DataFrame: Dataframe with renamed columns
        '''
        df = df.rename(columns=renamed_columns)
        return df
    
    @classmethod
    def pivot_columns(cls, df: pd.DataFrame, index: list, column: str, value: str) -> pd.DataFrame:
        ''' 
        Pivots columns in a given dataframe 
        
        Args:
            df (pd.DataFrame): Pandas dataframe where columns are going to be pivoted
            index (list): Columns to index for pivot
            column (str): Column to pivot value for
            value (str): Value displayed for pivoted column
        
        Returns:
            pd.DataFrame: Dataframe with pivoted columns
        '''
        df = df.pivot(index=index, columns=column, values=value).reset_index()
        df.columns.name = None
        return df
    
    @classmethod
    def drop_null(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Drops null rows in a given dataframe
        
        Args:
          df (pd.DataFrame): Pandas dataframe where null rows are going to be dropped

        Returns:
            pd.DataFrame: Dataframe with null columns removed 
        '''
        df = df.dropna()
        return df
    
    @classmethod
    def set_date_index(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Sets date as index for given dataframe 
        
        Args:
          df (pd.DataFrame): Pandas dataframe where date is going to be set as index

        Returns:
            pd.DataFrame: Dataframe with date as index
        '''
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        return df
    
    @classmethod
    def merge_dataframes(cls, daily_weather_df: pd.DataFrame,  natural_gas_monthly_variables_df: pd.DataFrame, 
    natural_gas_rigs_in_operation_df: pd.DataFrame, natural_gas_spot_prices_df: pd.DataFrame, heating_oil_spot_prices_df: pd.DataFrame):
        '''
        Merges dataframes representing each of the transformed sources from transformation folder in S3 Bucket

        Args:
            daily_weather_df (pd.DataFrame): Daily weather data df
            natural_gas_monthly_variables_df (pd.DataFrame): Natural gas monthly variables dataframe
            natural_gas_rigs_in_operation_df (pd.DataFrame): Monthly natural gas rigs in operation dataframe
            natural_gas_spot_prices_df (pd.DataFrame): Natural gas spot prices dataframe
            heating_oil_spot_prices_df (pd.DataFrame): Heating oil spot prices dataframe
        
        Returns:
            pd.DataFrame: Merged dataframe
        '''
        common_dates_spot_prices = natural_gas_spot_prices_df.index.intersection(heating_oil_spot_prices_df.index)
        natural_gas_spot_prices_df = natural_gas_spot_prices_df.loc[common_dates_spot_prices]
        heating_oil_spot_prices_df = heating_oil_spot_prices_df.loc[common_dates_spot_prices]
        spot_prices_merged_df = pd.merge(natural_gas_spot_prices_df, heating_oil_spot_prices_df, left_index=True, right_index=True)
        monthly_variables_merged_df = pd.merge(natural_gas_monthly_variables_df, natural_gas_rigs_in_operation_df, left_index=True, right_index=True)
        monthly_variables_merged_df['year_month'] = monthly_variables_merged_df.index.dt.to_period('M')
        spot_prices_merged_df['year_month'] = spot_prices_merged_df.index.dt.to_period('M')
        df = pd.merge(spot_prices_merged_df, monthly_variables_merged_df, on='year_month', how='inner')
        df = EtlTransforms.drop_columns(df=df, columns=['year_month'])
        df = pd.merge(df, daily_weather_df, left_index=True, right_index=True)
        return df
    
    @classmethod
    def forwardfill_null_values_end_of_series(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Forward fills null values for monthly natural gas variables and weather variables at the
        end of time series

        Args:
            df (pd.DataFrame): Merged dataframe
        
        Returns:
            pd.DataFrame: Returns dataframe with columns will null values end of series forward filled
        '''
        columns_to_ffill = ['residential_consumption', 'commerical_consumption', 'total_underground_storage'
        'imports', 'lng_imports', 'natural_gas_rigs_in_operation', 'awnd', 'snow', 'tavg']
        
        for col in columns_to_ffill:
            last_valid_index = df[col].last_valid_index()
            df.loc[last_valid_index:, col] = df.loc[last_valid_index:, col].ffill()
        
        return df
    
    @classmethod
    def normalisation(cls, df: pd.DataFrame, fit_transform: bool) -> pd.DataFrame:
        '''
        Normalises dataframe before training machine learning model on dataframe

        Args: 
            df (pd.DataFrame): Merged dataframe
            fit_transform (bool): True or False value indicating whether or not data
            is to be transformed and have a model fit to it

        Returns:
            pd.Dataframe: Returns dataframe with normalised data
        '''
        robust_columns = ['imports', 'lng_imports', 'heating_oil_natural_gas_price_ratio',  '7day_ew_volatility price ($/MMBTU)',
       '14day_ew_volatility price ($/MMBTU)', '30day_ew_volatility price ($/MMBTU)', '60day_ew_volatility price ($/MMBTU)', 
       'price_1day_lag ($/MMBTU)', 'price_2day_lag ($/MMBTU)', 'price_3day_lag ($/MMBTU)',
       '7day_rolling_average price ($/MMBTU)', '14day_rolling_average price ($/MMBTU)',
       '30day_rolling_average price ($/MMBTU)', '7day_rolling_median price ($/MMBTU)','14day_rolling_median price ($/MMBTU)',
       '30day_rolling_median price ($/MMBTU)', 'total_consumption_total_underground_storage_ratio',
       'min_tavg', 'max_tavg', 'max_abs_tavg_diff', 'max_abs_tavg_diff_relative_to_daily_median', 
       'hdd_sum', 'cdd_sum', 'wci_sum']
        log_columns = ['snow_sum']
        robust_scaler = RobustScaler()

        if fit_transform is True:
            df[robust_columns] = robust_scaler.fit_transform(df[robust_columns])
        
        else:
            df[robust_columns] = robust_scaler.transform(df[robust_columns])
        
        df[log_columns] = np.log(df[log_columns] + 1)
        return df
        



        



        









        
        
        
        



    


    


