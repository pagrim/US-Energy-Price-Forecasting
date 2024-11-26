import numpy as np
from transformation.etl_transforms import EtlTransforms
from modelling.model import Model

np.random.seed(42)
X_train = np.random.randn(200, 4)
y_train = np.random.randn(200, 2)

train_dataset = EtlTransforms.build_dataset(x=X_train, y=y_train, sequence_length=30, batch_size=128)
model = Model()
mdl = model.compile_model(time_steps=30)
model._train(model=mdl, dataset=train_dataset, validation_dataset=train_dataset)
