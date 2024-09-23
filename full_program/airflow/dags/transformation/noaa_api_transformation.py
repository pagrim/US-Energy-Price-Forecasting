''' Import modules '''
import pandas as pd
from datetime import datetime

class NoaaTransformation:
    '''
    Class for performing transformations on data extracted from NOAA API
    '''
    @classmethod
    def modify_date(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Modify date column and creater quarter column for daily weather data

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df['date'] = pd.to_datetime(df['date'])
        df['quarter'] = df['date'].dt.quarter
        return df
    
    @classmethod
    def imputation_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Creates dataframe that computes mean values for TMIN, TMAX, AWND, PRCP and SNOW
        for all cities across all quarters

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        quarter_months = {
        1: [1, 2, 3],
        2: [4, 5, 6],
        3: [7, 8, 9],
        4: [10, 11, 12]
        }
        output_df = pd.DataFrame(columns=['city', 'quarter', 'datatype', 'impute method', 'impute value'])
        for city in df['city'].unique():
            for quarter, relevant_months in quarter_months.items():
                filtered_df = df[(df['city'] == city) & (df['date'].dt.month.isin(relevant_months))]
                mean_tmin = filtered_df[filtered_df['datatype'] == 'TMIN']['value'].mean()
                mean_tmax = filtered_df[filtered_df['datatype'] == 'TMAX']['value'].mean()
                mean_awnd = filtered_df[filtered_df['datatype'] == 'AWND']['value'].mean()
                median_snow = filtered_df[filtered_df['datatype'] == 'SNOW']['value'].median()
                median_snow = median_snow if not pd.isnull(median_snow) else 0 # Null values where returned for Q1 + Q4 in LA + SF
                new_rows_df = pd.DataFrame([{'city': city, 'quarter': quarter, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': mean_tmin},
                {'city': city, 'quarter': quarter, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': mean_tmax},
                {'city': city, 'quarter': quarter, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': mean_awnd},
                {'city': city, 'quarter': quarter, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': median_snow}])
                output_df = pd.concat([output_df, new_rows_df], ignore_index=True)
        return output_df
    
    @classmethod
    def impute_missing_weather_variables(cls, df: pd.DataFrame, imputation_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Imputes rows which have a missing value for TMIN, TMAX, AWND, PRCP and SNOW values

        Args:
            df (pd.DataFrame): Daily weather data df
            imputation_df (pd.DataFrame): Dataframe that contains imputed values to replace missing values in df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df_group = df.groupby(['city', 'state'])
        quarter_impute_dic = dict(zip(zip(imputation_df['city'], imputation_df['quarter']), imputation_df['impute value']))
        new_rows = []

        for (city, date), group in df_group:
            weather_variables = set(group['datatype'])
            missing_weather_variables = set(['TMIN', 'TMAX', 'AWND', 'SNOW', 'PRCP']) - weather_variables
            
            if missing_weather_variables:
                for datatype in missing_weather_variables:
                    imputed_value = quarter_impute_dic.get((city, group['quarter'].iloc[0]))
                    new_row = {'date': date, 'datatype': datatype, 'station': group['station'].iloc[0], 
                    'value': imputed_value, 'city': city, 'state': group['state'].iloc[0],
                    'quarter': group['quarter'].iloc[0]}
                    new_rows.append(new_row)
    
        new_rows_df = pd.DataFrame(new_rows)
        df = pd.concat([df, new_rows_df], ignore_index=True)
        return df
    
    @classmethod
    def calculate_missing_tavg(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Calculates TAVG (average temperature) for rows where it is missing

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        tmin_rows = df[df['datatype'] == 'TMIN']
        tmax_rows = df[df['datatype'] == 'TMAX']
        merged_df = pd.merge(tmin_rows, tmax_rows, on=['city', 'date'])
        merged_df['tavg_value'] = (merged_df['value_x'] + merged_df['value_y']) / 2
        tavg_rows = df[df['datatype'] == 'TAVG']
        merged_df['combined'] = merged_df['city'] + merged_df['date'].astype(str)
        tavg_rows_combined = tavg_rows['city'] + tavg_rows['date'].astype(str)
        missing_tavg_rows = merged_df[~merged_df['combined'].isin(tavg_rows_combined)]
        new_rows = missing_tavg_rows[['date', 'city']]
        new_rows['datatype'] = 'TAVG'
        new_rows['station'] = missing_tavg_rows['station_x']
        new_rows['value'] = missing_tavg_rows['tavg_value']
        new_rows['state'] = missing_tavg_rows['state_x']
        new_rows['quarter'] = missing_tavg_rows['quarter_x']
        df = pd.concat([df, new_rows], ignore_index=True)
        return df






    
