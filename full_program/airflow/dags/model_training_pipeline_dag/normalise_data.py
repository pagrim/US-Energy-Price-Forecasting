''' Import modules '''
from datetime import datetime
from utils.aws import S3
from utils.config import Config
from transformation.etl_transforms import EtlTransforms

def normalise_data():
    ''' Function that normalises data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve curated training data from S3 folder
    curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)

    # Normalise data
    