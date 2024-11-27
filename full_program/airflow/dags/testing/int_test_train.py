import numpy as np
from transformation.etl_transforms import EtlTransforms
from modelling.model import Model

def test_model_training():
    """
    Integration test for model training
    """
    np.random.seed(42)
    X_train = np.random.randn(200, 26)
    y_train = np.random.randn(200, 1)
    X_validation = np.random.randn(50, 26)
    y_validation = np.random.randn(50, 1)

    train_dataset = EtlTransforms.build_dataset(x=X_train, y=y_train, sequence_length=30, batch_size=128)
    validation_dataset = EtlTransforms.build_dataset(x=X_validation, y=y_validation, sequence_length=30, batch_size=128)
    model = Model()
    mdl = model.compile_model(time_steps=30)
    model._train(model=mdl, dataset=train_dataset, validation_dataset=validation_dataset, epochs=1)
