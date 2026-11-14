import os
import pandas as pd

# Import Mage's decorator only if it isn't already loaded
if 'data_exporter' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except ImportError:
        def data_exporter(f): return f
if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(f): return f


@data_exporter
def export_metrics(df, *args, **kwargs):
    """
    C4: Exports the evaluation metrics table to iris_metrics.csv.
    """
    df.to_csv('iris_metrics.csv', index=False)
    print('Saved', len(df), 'metrics to iris_metrics.csv')


@test
def test_metrics_file_created(output, *args):
    """
    Quality check: verifies that iris_metrics.csv was successfully created on disk.
    """
    assert os.path.exists('iris_metrics.csv'), 'iris_metrics.csv was not created'
    df = pd.read_csv('iris_metrics.csv')
    assert len(df) == 4, 'iris_metrics.csv must contain 4 metric rows'
