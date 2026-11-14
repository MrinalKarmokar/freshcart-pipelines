# tests/test_prepare_data.py
from sklearn.datasets import load_iris
from iris_project.transformers.prepare_data import transform


def test_split_is_80_20():
    """
    Part A3: Verify that prepare_data splits the 150-row Iris dataset 80/20
    into 120 training rows and 30 test rows.
    """
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert (out["split"] == "train").sum() == 120
    assert (out["split"] == "test").sum() == 30
