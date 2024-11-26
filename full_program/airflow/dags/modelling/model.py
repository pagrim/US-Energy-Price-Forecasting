''' Import modules '''
from datetime import datetime
import pickle
import io
import os
import numpy as np
import pandas as pd
import mlflow
from tensorflow.python.keras.utils.version_utils import callbacks

from modelling.mlflowcallback import MLflowCallback
import tensorflow as tf
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
    def train_model(cls, dataset: tf.data.Dataset, validation_dataset: tf.data.Dataset, time_steps: int,
                    experiment_id: str, forecast_horizon: int,
                    ) -> None:
        """
        Trains model and logs model parameters, results and creates a model artifact to be used in streamlit 
        """

        model = cls.compile_model(time_steps)

        current_date = datetime.now()
        current_date_formatted = current_date.strftime('%Y%m%d')

        with mlflow.start_run(experiment_id=experiment_id,
                              run_name=f'GRU_{forecast_horizon}_day_horizon_{time_steps}_{current_date_formatted}'):
            mlflow.log_param("units", 32)
            mlflow.log_param("activation_function", 'tanh')
            mlflow.log_param("dropout", 0.2)
            mlflow.log_param("epochs", 150)
            mlflow.log_param("batch_size", 128)

            # Create the sequence generators for training and validation
            # train_gen = EtlTransforms.create_sequences(x_train, y_train, sequence_length=time_steps, batch_size=batch_size)
            # test_gen = EtlTransforms.create_sequences(x_test, y_test, sequence_length=time_steps, batch_size=batch_size)
            # The above would be passed to model fit with test_gen in validation_data parameter

            '''print(f' X_train columns : {x_train.columns}')
            print(f' Null values by column: {x_train.isnull().sum()}')
            print(f"X_train shape: {x_train.shape}")
            print(f"X_test shape: {x_test.shape}")
            x_batch, y_batch = next(train_gen)
            print(f"Batch shape: {x_batch.shape}, {y_batch.shape}")'''

            trained = cls._train(model, dataset, validation_dataset, callbacks=[MLflowCallback])
            mae = cls.compute_mae(trained, validation_dataset)

            mlflow.log_metric("mae", mae)

            mlflow.keras.log_model(model, f'GRU_{forecast_horizon}_day_horizon_{time_steps}_{current_date_formatted}')

    @classmethod
    def compile_model(cls, time_steps: int):
        model = keras.Sequential()
        model.add(layers.GRU(units=32, activation='tanh', return_sequences=True, input_shape=(time_steps, 26)))
        model.add(layers.GRU(units=32, activation='tanh'))
        model.add(layers.Dropout(0.2))
        model.add(layers.Dense(1))
        model.compile(optimizer='adam', loss='mae')
        return model

    @classmethod
    def _train(cls, model: keras.model, dataset: tf.data.Dataset, validation_dataset: tf.data.Dataset, callbacks=None):
        model.fit(dataset, epochs=150, validation_data=validation_dataset, verbose=2,
                  callbacks=[] if callbacks is None else callbacks)
        return model

    @classmethod
    def compute_mae(cls, model: keras.model, validation_dataset: tf.data.Dataset):
        y_pred = model.predict(validation_dataset)
        mae = mean_absolute_error(validation_dataset.take(-1),y_pred)
        return mae
