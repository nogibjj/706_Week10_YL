import os
import pytest
from mylib.lib import extract, load, describe, query, transform, start_spark, end_spark

@pytest.fixture(scope="module")
def spark():
    spark = start_spark("testApp")
    yield spark
    end_spark(spark)

def test_extract():
    file_path = extract()
    assert os.path.exists(file_path) is True

def test_load(spark):
    df = load(spark)
    assert df is not None

def test_describe(spark):
    df = load(spark)
    res = describe(df)
    assert res is not None

def test_query(spark):
    df = load(spark)
    res = query(spark, df)
    assert res is None

def test_transform(spark):
    df = load(spark)
    res = transform(df)
    assert res is None

if __name__ == "__main__":
    test_extract()
    test_load(spark)
    test_describe(spark)
    test_query(spark)
    test_transform(spark)