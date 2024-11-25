''' Import modules '''
from datetime import datetime
import pickle
import io
import os
import numpy as np
import pandas as pd
import mlflow
from modelling.mlflowcallback import MLflowCallback
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.callbacks import Callback, EarlyStopping
from sklearn.metrics import mean_absolute_error
from transformation.etl_transforms import EtlTransforms

class Model:
    ''' 
    Class used for modelling of data
    '''
    @classmethod
    def train_model(cls, output_dir: str, time_steps: int, experiment_id: str, forecast_horizon: int,
    ) -> None:
        ''' 
        Trains model and logs model parameters, results and creates a model artifact to be used in streamlit 
        '''
        output_file_x = os.path.join(output_dir, f'x_sequences_train_{forecast_horizon}day.npy')
        output_file_y = os.path.join(output_dir, f'y_sequences_train_{forecast_horizon}day.npy')

        x_train = np.load(output_file_x, allow_pickle=True)
        y_train = np.load(output_file_y, allow_pickle=True)

        output_file_x_test = os.path.join(output_dir, f'x_sequences_test_{forecast_horizon}day.npy')
        output_file_y_test = os.path.join(output_dir, f'y_sequences_test_{forecast_horizon}day.npy')

        x_test = np.load(output_file_x_test, allow_pickle=True)
        y_test = np.load(output_file_y_test, allow_pickle=True)

        x_train = x_train.reshape((x_train.shape[0], x_train.shape[1], x_train.shape[2]))
        x_test = x_test.reshape((x_test.shape[0], x_test.shape[1], x_test.shape[2]))

        model=keras.Sequential()
        model.add(layers.GRU(units=32, activation='tanh', return_sequences=True, input_shape=(time_steps, 26)))
        model.add(layers.GRU(units=32, activation='tanh'))
        model.add(layers.Dropout(0.2))
        model.add(layers.Dense(1))
        model.compile(optimizer='adam', loss='mae')

        current_date = datetime.now()
        current_date_formatted = current_date.strftime('%Y%m%d')

        with mlflow.start_run(experiment_id=experiment_id, run_name=f'GRU_{forecast_horizon}_day_horizon_{time_steps}_{current_date_formatted}'):
            mlflow.log_param("units", 32)
            mlflow.log_param("activation_function", 'tanh')
            mlflow.log_param("dropout", 0.2)
            mlflow.log_param("epochs", 150)
            mlflow.log_param("batch_size", 128)

            # Create the sequence generators for training and validation
            #train_gen = EtlTransforms.create_sequences(x_train, y_train, sequence_length=time_steps, batch_size=batch_size)
            #test_gen = EtlTransforms.create_sequences(x_test, y_test, sequence_length=time_steps, batch_size=batch_size)
            # The above would be passed to model fit with test_gen in validation_data parameter

            '''print(f' X_train columns : {x_train.columns}')
            print(f' Null values by column: {x_train.isnull().sum()}')
            print(f"X_train shape: {x_train.shape}")
            print(f"X_test shape: {x_test.shape}")
            x_batch, y_batch = next(train_gen)
            print(f"Batch shape: {x_batch.shape}, {y_batch.shape}")'''

            model.fit(x_train, y_train, epochs=150, batch_size=128, validation_data=(x_test, y_test), verbose=2, 
            callbacks = [MLflowCallback()])
            
            y_pred = model.predict(x_test)
            mae = mean_absolute_error(y_test['price ($/MMBTU)'].iloc[0: forecast_horizon].to_numpy(), y_pred[0: forecast_horizon])
            mlflow.log_metric("mae", mae)
        
            mlflow.keras.log_model(model, f'GRU_{forecast_horizon}_day_horizon_{time_steps}_{current_date_formatted}')
    
    

        
    

        





    
    


