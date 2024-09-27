''' Import modules '''
import pandas as pd

class EiaTransformation:
    '''
    Class for performing transformations on data extracted from EIA API
    '''
    @classmethod
    def impute_null_monthly_variable(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' Imputes null values for monthly variables with median calculated from 
        values up to 6 months prior and 6 months succeeding missing value '''
        for column in df.columns:
            if df[column].isnull.any():
                for index, row in df.iterrows():
                    if pd.isnull(row[column]):
                        start_index = max(index - 6, 0)
                        end_index = min(index + 7, len(df))
                        median_value = round(df[column].iloc[start_index : end_index].median(), 2)
                        df.at[index, column] = median_value
        return df