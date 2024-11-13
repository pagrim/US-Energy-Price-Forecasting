''' Import modules '''
from datetime import datetime
from utils.config import *
from extraction.eia_api import *
from transformation.etl_transforms import EtlTransforms
from transformation.eia_api_transformation import EiaTransformation

def convert_values_to_float():
    ''' Drop null records from extracted natural gas monthly variables '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    natural_gas_monthly_variables_json = s3.get_data(folder='full_program/extraction/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')
    natural_gas_monthly_variables_df = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json, date_as_index=False)

    # Drop null values from natural_gas_spot_df
    natural_gas_monthly_variables_df = EiaTransformation.convert_column_to_float(df=natural_gas_monthly_variables_df, column='value')
    
    # Put data in S3
    s3.put_data(data=natural_gas_monthly_variables_df, folder='full_program/extraction/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')