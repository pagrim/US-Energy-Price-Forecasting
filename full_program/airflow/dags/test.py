import pandas as pd
import numpy as np
from transformation.etl_transforms import EtlTransforms

X_train = pd.DataFrame(np.random.randn(100, 4), columns=list('ABCD'))
y_train = pd.DataFrame(np.random.randn(100, 2), columns=list('EF'))

EtlTransforms.create_sequences(x=X_train, y=y_train, sequence_length=30,
                               output_dir='test', batch_size=128, type='train_7day')
