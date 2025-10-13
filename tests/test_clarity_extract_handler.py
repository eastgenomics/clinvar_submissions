import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from utils import clarity_extract_handler as ceh
from pathlib import Path


@pytest.fixture
def example_csv(tmp_path):
    # Copy the provided test_extract.csv to a temp location
    src = (
        Path(__file__).parent
        / "test_data"
        / "clarity_extract_examples"
        / "test_extract.csv"
    )
    dest = tmp_path / "test_extract.csv"
    dest.write_text(src.read_text())
    return str(dest)


def test_open_files_with_example(example_csv):
    """Test opening a valid clarity extract CSV file."""
    df = ceh.open_files(example_csv)
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {
        "Beaker Procedure Name",
        "Received Specimen Date Time",
        "Specimen Identifier",
        "Test Directory Test Code",
        "Test Validation Status",
        "Last Final Verify Date",
    }  # Columns in the example CSV
    assert df.shape[0] == 2  # Two rows in the example CSV
    expected_df = pd.DataFrame(
        {
            "Beaker Procedure Name": ["WES NGS", "CEN NGS"],
            "Received Specimen Date Time": ["2024-01-25 03:55", "2024-02-21 11:08"],
            "Specimen Identifier": ["SP-24015R0015", "SP-24010R0031"],
            "Test Directory Test Code": ["R149.1", "R208.1"],
            "Test Validation Status": ["Verified", "Verified"],
            "Last Final Verify Date": ["2024-01-27 12:55", "2024-02-25 12:18"],
        }
    )
    pd.testing.assert_frame_equal(df, expected_df)


def test_open_files_with_empty_file(tmp_path):
    """Test opening an empty CSV file raises ValueError."""
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("")
    with pytest.raises(ValueError) as exc:
        ceh.open_files(str(empty_file))
    assert "Clarity extract file is empty" in str(exc.value)


def test_open_files_parser_error(tmp_path):
    """Test opening a malformed CSV file raises Exception."""
    bad_csv = tmp_path / "bad.csv"
    # Unclosed quote to trigger a ParserError
    bad_csv.write_text('a,b\n1,"2\n3,4\n')
    with pytest.raises(Exception) as exc:
        ceh.open_files(str(bad_csv))
    assert str(exc.value).startswith("Error reading clarity extract:")

#002_251010_A01303_0320_BACWV9DRX7_37_CEN
#002_251010_A01303_0120_AHCWV8DRX7_38_TWE


def test_create_path_cen():
    """Test path creation for CEN assay."""
    path = ceh.create_path("file.xlsx", Path("/mnt/clingen/"), "CEN", "002_251010_A01303_0320_BACWV9DRX7_37_CEN")
    expected = Path("/mnt/clingen/CEN/Run folders/251010_A01303_0320_BACWV9DRX7/file.xlsx")
    assert isinstance(path, Path)
    assert path == expected


def test_create_path_wes():
    """Test path creation for WES assay."""
    path = ceh.create_path("file.xlsx", Path("/mnt/clingen/"), "WES", "002_251010_A01303_0120_AHCWV8DRX7_38_TWE")
    expected = Path("/mnt/clingen/WES/251010_A01303_0120_AHCWV8DRX7/file.xlsx")
    assert isinstance(path, Path)
    assert path == expected


def test_create_path_nan():
    """Test path creation with NaN run folder returns None."""
    assert (
        ceh.create_path("file.xlsx", Path("/mnt/clingen/"), "CEN", float("nan")) is None
    )


def test_query_reports_for_project_handles_no_sample_ids():
    """Test querying reports with no sample IDs returns empty list."""
    # Should return empty list if sample_ids is empty
    assert ceh.query_reports_for_project("proj-1", []) == []


def test_query_reports_for_project_handles_bad_filename(monkeypatch):
    """Test querying reports with a bad filename skips the file."""
    # Patch dxpy.find_data_objects to return a file with a bad name
    monkeypatch.setattr(
        ceh.dxpy,
        "find_data_objects",
        lambda **kwargs: [{"describe": {"name": "badfilename.xlsx"}}],
    )
    records = ceh.query_reports_for_project("proj-1", ["123"])
    # Should skip the file and return an empty list
    assert records == []


def test_open_files_directory_path_raises(tmp_path):
    """Test opening a directory path raises Exception."""
    with pytest.raises(Exception) as exc:
        ceh.open_files(str(tmp_path))
    assert "Error reading clarity extract:" in str(exc.value)


@patch("utils.clarity_extract_handler.dxpy.find_projects")
def test_get_matching_projects(mock_find_projects):
    """Test getting matching projects in normal scenario."""
    mock_find_projects.return_value = [
        {"id": "proj-1", "describe": {"name": "002_foo_CEN"}},
        {"id": "proj-2", "describe": {"name": "002_bar_CEN"}},
    ]
    result = ceh.get_matching_projects(["CEN"])
    assert result == [("proj-1", "002_foo_CEN"), ("proj-2", "002_bar_CEN")]


@patch("utils.clarity_extract_handler.dxpy.find_data_objects")
def test_query_reports_for_project(mock_find_data_objects):
    """Test querying reports for a project with valid sample IDs."""
    mock_find_data_objects.return_value = [
        {"describe": {"name": "1234567-24080852.xlsx"}},
        {"describe": {"name": "8901234-24090855.xlsx"}},
    ]
    records = ceh.query_reports_for_project("proj-1", ["123", "456"])
    assert isinstance(records, list)
    assert all("sample_id" in r for r in records)


def test_find_file_name_no_files_found(monkeypatch):
    # No files returned
    monkeypatch.setattr(ceh.dxpy, "find_data_objects", lambda **kwargs: [])
    assert ceh.find_file_name("SP-24010R0031*") is None


def test_find_file_name_multiple_files(monkeypatch):
    # Multiple files returned -> should return None
    monkeypatch.setattr(
        ceh.dxpy,
        "find_data_objects",
        lambda **kwargs: [
            {"describe": {"name": "SP-24010R0031-CEN_R208.1_1.xlsx"}},
            {"describe": {"name": "SP-24010R0031-CEN_R208.1_2.xlsx"}},
        ],
    )
    assert ceh.find_file_name("SP-24010R0031*") is None


def test_find_file_name_single_file(monkeypatch):
    # Single file returned -> should return that filename
    monkeypatch.setattr(
        ceh.dxpy,
        "find_data_objects",
        lambda **kwargs: [{"describe": {"name": "SP-24010R0031-CEN_R208.1_1.xlsx"}}],
    )
    assert ceh.find_file_name("SP-24010R0031*") == "SP-24010R0031-CEN_R208.1_1.xlsx"
