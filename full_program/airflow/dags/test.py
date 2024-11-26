import numpy as np
from transformation.etl_transforms import EtlTransforms

np.random.seed(42)
X_train = np.random.randn(200, 4)
y_train = np.random.randn(200, 2)

train_dataset = EtlTransforms.build_dataset(x=X_train, y=y_train, sequence_length=30, batch_size=128)

# Example usage
for batch_x, batch_y in train_dataset.take(1):
    print("Batch X shape:", batch_x.shape)
    print("Batch Y shape:", batch_y.shape)
    print("Batch X:", batch_x)
    print("Batch Y:", batch_y)
