import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

if 'transformer' not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        def transformer(f): return f
if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(f): return f


@transformer
def evaluate(df, *args, **kwargs):
    """
    Computes confusion matrix and classification metrics on the predicted test set.
    """
    y_true = df['target']
    y_pred = df['predicted']

    print('Confusion matrix:')
    print(pd.crosstab(y_true, y_pred, rownames=['actual'], colnames=['predicted']))

    metrics = pd.DataFrame({
        'metric': ['accuracy', 'precision', 'recall', 'f1_score'],
        'value': [
            round(accuracy_score(y_true, y_pred), 3),
            round(precision_score(y_true, y_pred, average='macro'), 3),
            round(recall_score(y_true, y_pred, average='macro'), 3),
            round(f1_score(y_true, y_pred, average='macro'), 3),
        ]
    })
    print('\nMetrics table:')
    print(metrics)
    return metrics


@test
def test_evaluation(output, *args):
    """
    Quality check: verifies that the metrics DataFrame has exactly 4 metrics.
    """
    assert len(output) == 4, 'Metrics table must have 4 rows'
    assert 'value' in output.columns, 'Metrics table must contain value column'
