import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from KPI_aggregation import KPIDataProcessor


def make_processor():
    processor = KPIDataProcessor()
    return processor


@patch('config.COLS_TO_EXPECT_IN_CSV', ['A', 'B', 'C'])
def test_validate_columns_match(mock_cols):
    processor = make_processor()
    df = pd.DataFrame(columns=['A', 'B', 'C'])
    processor.validate_columns(df)
    assert processor.python_errors_list == []

@patch('config.COLS_TO_EXPECT_IN_CSV', ['A', 'B', 'C'])
def test_validate_columns_extra(mock_cols):
    processor = make_processor()
    df = pd.DataFrame(columns=['A', 'B', 'C', 'D'])
    processor.validate_columns(df)
    # Should contain message about extra column 'D'
    assert any('Extra columns found: D' in msg for msg in processor.python_errors_list)

@patch('config.COLS_TO_EXPECT_IN_CSV', ['A', 'B', 'C'])
def test_validate_columns_missing(mock_cols):
    processor = make_processor()
    df = pd.DataFrame(columns=['A', 'B'])
    processor.validate_columns(df)
    # Should contain message about missing column 'C'
    assert any('Missing columns: C' in msg for msg in processor.python_errors_list)


@patch('config.COLS_TO_EXPECT_IN_CSV', ['A', 'B', 'C'])
def test_validate_columns_extra_and_missing(mock_cols):
    processor = make_processor()
    df = pd.DataFrame(columns=['A', 'B', 'D'])
    processor.validate_columns(df)
    assert any('Extra columns found: D' in msg for msg in processor.python_errors_list)
    assert any('Missing columns: C' in msg for msg in processor.python_errors_list)