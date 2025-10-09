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
    dst = tmp_path / "test_extract.csv"
    dst.write_text(src.read_text())
    return str(dst)


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
    }
    assert df.shape[0] == 2

def test_open_files_with_empty_file(tmp_path):
    """Test opening an empty CSV file raises ValueError."""
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("")
    with pytest.raises(ValueError) as exc:
        ceh.open_files(str(empty_file))
    assert "Clarity extract file is empty" in str(exc.value)

def test_open_files_with_bad_file(tmp_path):
    """Test opening a malformed CSV file raises Exception."""
    bad_file = tmp_path / "bad.csv"
    bad_file.write_text('a,b\n1,"2\n3,4\n')  # Unclosed quote to trigger a ParserError
    with pytest.raises(Exception) as exc:
        ceh.open_files(str(bad_file))
    assert str(exc.value).startswith("Error reading clarity extract:")

def test_create_path_cen():
    """Test path creation for CEN assay."""
    path = ceh.create_path("file.xlsx", Path("/mnt/clingen/"), "CEN", "002_ABC123")
    expected = Path("/mnt/clingen/CEN/Run folders/ABC123/file.xlsx")
    assert isinstance(path, Path)
    assert path == expected


def test_create_path_wes():
    """Test path creation for WES assay."""
    path = ceh.create_path("file.xlsx", Path("/mnt/clingen/"), "WES", "002_DEF456")
    expected = Path("/mnt/clingen/WES/DEF456/file.xlsx")
    assert isinstance(path, Path)
    assert path == expected


def test_create_path_nan():
    """Test path creation with NaN run folder returns None."""
    assert ceh.create_path("file.xlsx", Path("/mnt/clingen/"), "CEN", float("nan")) is None


def test_filter_duplicate_files_various_cases():
    """Test filtering duplicate files with various scenarios."""
    df = pd.DataFrame(
        {
            "sample_id": [
                "1234567-09877654",  # A: 3 files
                "1234567-09877654",
                "1234567-09877654",
                "2345678-98776543",  # B: 1 file
                "2509888-12345677",  # C: 2 files
                "2509888-12345677",
            ],
            "file_name": [
                "1234567-09877654_CNV_1.xlsx",
                "1234567-09877654_SNV_1.xlsx",
                "1234567-09877654_SNV_2.xlsx",  # A: 3 files
                "2345678-98776543_SNV_1.xlsx",  # B: 1 file
                "2509888-12345677_CNV_1.xlsx",
                "2509888-12345677_SNV_2.xlsx",  # C: 2 files
            ],
        }
    )
    filtered = ceh.filter_duplicate_files(df)
    # For sample A, only one file should be kept
    assert len(filtered[filtered["sample_id"] == "1234567-09877654"]) == 2
    assert "1234567-09877654_SNV_1.xlsx" in filtered["file_name"].values
    assert len(filtered[filtered["sample_id"] == "2345678-98776543"]) == 1
    # For sample C, 2509888-12345677_CNV_1.xlsx should be kept, 2509888-12345677_SNV_2.xlsx should be dropped
    assert len(filtered[filtered["sample_id"] == "2509888-12345677"]) == 1
    assert "2509888-12345677_CNV_1.xlsx" in filtered["file_name"].values
    assert "2509888-12345677_SNV_2.xlsx" not in filtered["file_name"].values
    # All sample_ids should still be present
    assert set(filtered["sample_id"]) == {
        "1234567-09877654",
        "2345678-98776543",
        "2509888-12345677",
    }


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


def test_open_files_success(tmp_path):
    """Test opening a valid CSV file."""
    # Create a dummy CSV file
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("a,b\n1,2\n3,4")
    df = ceh.open_files(str(csv_path))
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)


def test_open_files_empty_file(tmp_path):
    """Test opening an empty CSV file raises ValueError."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("")
    with pytest.raises(ValueError) as exc:
        ceh.open_files(str(empty_csv))
    assert "Clarity extract file is empty" in str(exc.value)


def test_open_files_parser_error(tmp_path):
    """Test opening a malformed CSV file raises Exception."""
    bad_csv = tmp_path / "bad.csv"
    # Unclosed quote to trigger a ParserError
    bad_csv.write_text('a,b\n1,"2\n3,4\n')
    with pytest.raises(Exception) as exc:
        ceh.open_files(str(bad_csv))
    assert str(exc.value).startswith("Error reading clarity extract:")


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
