import pandas as pd
from sklearn.datasets import load_iris

# Import Mage's decorators only if they aren't already loaded
if 'data_loader' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_loader
    except ImportError:
        def data_loader(f): return f
if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(f): return f


@data_loader
def load_data(*args, **kwargs):
    """
    Loads the Iris dataset from scikit-learn as a pandas DataFrame.
    """
    iris = load_iris(as_frame=True)
    df = iris.frame                      # 4 feature columns + 'target'
    print('Rows loaded:', len(df))
    return df


@test
def test_output(output, *args):
    """
    Quality check: verifies that the dataset contains exactly 150 rows.
    """
    assert len(output) == 150, 'Iris must have 150 rows'
