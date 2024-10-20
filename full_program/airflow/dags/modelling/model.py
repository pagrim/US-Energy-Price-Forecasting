''' Import modules '''
from datetime import datetime
import pickle
import io
import numpy as np
import pandas as pd
import mlflow
from modelling.mlflowcallback import MLflowCallback
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.callbacks import Callback, EarlyStopping
from sklearn.metrics import mean_absolute_error

class Model:
    ''' 
    Class used for modelling of data
    '''
    @classmethod
    def train_model(cls, x_train: np.array, y_train: np.array, x_test: np.array, y_test: np.array, time_steps: int, experiment_id: str, forecast_horizon: int) -> None:
        ''' 
        Trains model and logs model parameters, results and creates a model artifact to be used in streamlit 
        '''
        x_train = x_train.reshape((x_train.shape[0], x_train.shape[1], x_train.shape[2]))
        x_test = x_test.reshape((x_test.shape[0], x_test.shape[1], x_test.shape[2]))

        model=keras.Sequential()
        model.add(layers.GRU(units=32, activation='tanh', return_sequences=True, input_shape=(time_steps, 25)))
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

            model.fit(x_train, y_train, epochs=150, batch_size=128, validation_data=(x_test, y_test), verbose=2, 
            callbacks = [MLflowCallback()])
            
            y_pred = model.predict(x_test)
            mae = mean_absolute_error(y_test['price ($/MMBTU)'].iloc[0: forecast_horizon].to_numpy(), y_pred[0: forecast_horizon])
            mlflow.log_metric("mae", mae)
        
            mlflow.keras.log_model(model, f'GRU_{forecast_horizon}_day_horizon_{time_steps}_{current_date_formatted}')
    
    

        
    

        





    
    


