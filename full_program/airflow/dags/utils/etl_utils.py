''' Import modules '''
import json
import pandas as pd

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

    


    


