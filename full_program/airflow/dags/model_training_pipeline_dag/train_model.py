''' Import modules '''
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from utils.aws import S3
from utils.config import Config
from transformation.etl_transforms import EtlTransforms
from modelling.mlflow_model import MlflowModel
from modelling.model import Model

def train_model():
    ''' Function that trains model '''
    # Todays date
    #today = datetime.now()
    #formatted_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Setup mlflow tracking uri and retrieve experiment_id
    mlflow_model = MlflowModel(experiment_name='Natural gas price forecasting production', tracking_uri='http://mlflow:5000')
    mlflow_model.set_tracking_uri()
    experiment_id = mlflow_model.retrieve_experiment_id()

    # Retrieve curated training and test data from folder
    curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key='curated_training_data_20241118')
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=False) # Add date to index back
    curated_test_data_json = s3.get_data(folder='full_program/curated/test_data/', object_key='curated_test_data_20241118')
    curated_test_data_df = EtlTransforms.json_to_df(data=curated_test_data_json, date_as_index=False) # Add date to index back

    # Normalise the data
    X_train = curated_training_data_df.drop(columns='price ($/MMBTU)')
    y_train = curated_training_data_df['price ($/MMBTU)']
    X_test = curated_test_data_df.drop(columns='price ($/MMBTU)')
    y_test = curated_test_data_df['price ($/MMBTU)']

    X_train, X_test = EtlTransforms.normalise(train_df=X_train, test_df=X_test)

    print('Data has been normalised')

    # Create sequences for training and test data
    train_dataset = EtlTransforms.build_dataset(x=X_train, y=y_train, sequence_length=30, batch_size=128)
    '''EtlTransforms.create_sequences(x=X_train, y=y_train, sequence_length=21, output_dir='/opt/airflow/dags/', batch_size=128, type='train_14day')
    EtlTransforms.create_sequences(x=X_train, y=y_train, sequence_length=14, output_dir='/opt/airflow/dags/', batch_size=128, type='train_30day')
    EtlTransforms.create_sequences(x=X_train, y=y_train, sequence_length=14, output_dir='/opt/airflow/dags/', batch_size=128, type='train_60day')
    EtlTransforms.create_sequences(x=X_test, y=y_test, sequence_length=30, output_dir='/opt/airflow/dags/', batch_size=128, type='test_7day')
    EtlTransforms.create_sequences(x=X_test, y=y_test, sequence_length=21, output_dir='/opt/airflow/dags/', batch_size=128, type='test_14day')
    EtlTransforms.create_sequences(x=X_test, y=y_test, sequence_length=14, output_dir='/opt/airflow/dags/', batch_size=128, type='test_30day')
    EtlTransforms.create_sequences(x=X_test, y=y_test, sequence_length=14, output_dir='/opt/airflow/dags/', batch_size=128, type='test_60day')'''
    print('sequences successfully created')

    # Train GRU model
    Model.train_model(output_dir='/opt/airflow/dags/', time_steps=30, experiment_id=experiment_id, forecast_horizon=7)
    '''Model.train_model(output_dir='/opt/airflow/dags/', time_steps=21, experiment_id=experiment_id, forecast_horizon=14)
    Model.train_model(output_dir='/opt/airflow/dags/', time_steps=14, experiment_id=experiment_id, forecast_horizon=30)
    Model.train_model(output_dir='/opt/airflow/dags/', time_steps=14, experiment_id=experiment_id, forecast_horizon=60)'''




    

