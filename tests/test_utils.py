import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
from utils import utils
from datetime import date
import openpyxl

# --- 40-78: get_workbook_data ---

@pytest.fixture
def mock_workbook():
    wb = MagicMock()
    wb.__getitem__.side_effect = lambda x: {
        "summary": {
            "B1": MagicMock(value="123456789-12345A1234-12ABCDE1-1234-X-PS1"),
            "F1": MagicMock(value="1234_Condition"),
            "F2": MagicMock(value="Panel1"),
            "G22": MagicMock(value=None),
            "A": [MagicMock(value="Reference:", row=10), MagicMock(value=None, row=11)],
            "C38": MagicMock(value=1)
        }
    }[x]
    wb.sheetnames = ["summary", "interpret1"]
    return wb

@pytest.fixture
def mock_config():
    return {
        "Institution": "TestInst",
        "Collection method": "TestColl",
        "Allele origin": "TestAllele",
        "Affected status": "TestStatus",
        "CUH Organisation": "CUH Org",
        "CUH org ID": "CUH123",
        "NUH Organisation": "NUH Org",
        "NUH org ID": "NUH123",
        "field_cells": [("germline_classification", "B26"), ("hgvsc", "B27")],
        "acgs_criteria": [],
        "matched_strength": {},
        "strength_dropdown": [],
        "BA1_dropdown": [],
    }

@pytest.fixture
def mock_engine():
    return MagicMock()

@pytest.fixture
def parsed_wb():
    # import xlsx file from tests/test_data/NUH/nuh.xlsx
    parsed_wb = openpyxl.load_workbook(
        "tests/test_data/NUH/nuh.xlsx"
    )
    return parsed_wb


def test_check_interpret_table_valid():
    config = {
        "strength_dropdown": ["Strong", "Supporting"],
        "BA1_dropdown": ["Yes", "No"],
        "acgs_criteria": [],
    }
    df_interpret = pd.DataFrame({
        "germline_classification": ["Pathogenic"],
        "hgvsc": ["NM_1"],
        "ba1": [np.nan]
    })
    df_included = pd.DataFrame({"hgvsc": ["NM_1"]})
    assert utils.check_interpret_table(df_interpret, df_included, config) is None

def test_check_interpret_table_invalid():
    config = {
        "strength_dropdown": ["Strong", "Supporting"],
        "BA1_dropdown": ["Yes", "No"],
        "acgs_criteria": [],
    }
    df_interpret = pd.DataFrame({
        "germline_classification": [np.nan],
        "hgvsc": ["NM_1"],
        "ba1": [np.nan]
    })
    df_included = pd.DataFrame({"hgvsc": ["NM_1"]})
    err = utils.check_interpret_table(df_interpret, df_included, config)
    assert "empty ACMG classification" in err

def test_check_interpreted_col_invalid_value():
    df = pd.DataFrame({
        "interpreted": ["yes", "maybe"],
        "germline_classification": ["Pathogenic", None],
        "hgvsc": ["NM_1", "NM_2"]
    })
    err = utils.check_interpreted_col(df)
    assert "not all either 'yes' or 'no'" in err


def test_check_sample_name_error():
    err = utils.check_sample_name("bad", "bad", "bad", "bad", "bad")
    assert err is not None

@patch("utils.utils.requests.get")
def test_submission_status_check_success(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content.decode.return_value = '{"actions":[{"status":"done","responses":[{"files":[{"url":"http://file"}]}]}]}'
    mock_response.headers = {}
    mock_get.return_value = mock_response

    mock_file_response = MagicMock()
    mock_file_response.status_code = 200
    mock_file_response.content.decode.return_value = '{"file":"content"}'
    mock_get.side_effect = [mock_response, mock_file_response]

    status, status_response = utils.submission_status_check("subid", {}, "http://api")
    assert status == "done"
    assert "file" in status_response

@patch("utils.utils.requests.get")
def test_submission_status_check_fail(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.content.decode.return_value = "fail"
    mock_response.headers = {}
    mock_get.return_value = mock_response
    with pytest.raises(RuntimeError):
        utils.submission_status_check("subid", {}, "http://api")

def test_check_sample_name_valid():
    err = utils.check_sample_name("123456789", "12345A1234", "12ABCDE1", "1234", "PS1")
    assert err is None

def test_check_sample_name_invalid_probeset():
    err = utils.check_sample_name("123456789", "12345A1234", "12ABCDE1", "1234", "A"*21)
    assert "too long/short" in err
