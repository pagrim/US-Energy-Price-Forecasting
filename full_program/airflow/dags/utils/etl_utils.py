''' Import modules '''
import json
import pandas as pd
from aws import S3

class EtlUtils:
    '''
    Class containing utility + general purpose functions used in ETL process

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
    


    


