import joblib
from sklearn.tree import DecisionTreeClassifier

# Import Mage's decorators only if they aren't already loaded
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

FEATURES = ['sepal length (cm)', 'sepal width (cm)',
            'petal length (cm)', 'petal width (cm)']


@transformer
def train(df, *args, **kwargs):
    """
    Trains a DecisionTreeClassifier on training rows using max_depth from runtime variables.
    Predicts test rows and returns them with 'predicted' column.
    """
    max_depth = int(kwargs.get('max_depth', 3))

    train_df = df[df['split'] == 'train']
    test_df = df[df['split'] == 'test'].copy()

    model = DecisionTreeClassifier(max_depth=max_depth, random_state=42)
    model.fit(train_df[FEATURES], train_df['target'])

    test_df['predicted'] = model.predict(test_df[FEATURES])
    accuracy = (test_df['predicted'] == test_df['target']).mean()
    print('max_depth =', max_depth, '| Test accuracy =', round(accuracy, 3))

    joblib.dump(model, 'iris_model.pkl')
    return test_df


@test
def test_accuracy(output, *args):
    """
    Quality gate: asserts test accuracy is at least 80%.
    If accuracy < 0.8, the block fails and downstream blocks (save_results) will NOT run.
    """
    accuracy = (output['predicted'] == output['target']).mean()
    assert accuracy >= 0.8, f'Model accuracy {accuracy:.3f} is below 80%'
