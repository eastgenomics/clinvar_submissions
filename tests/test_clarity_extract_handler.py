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
    with pytest.raises(RuntimeError) as exc:
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


def test_create_path_cen():
    """Test path creation for CEN assay."""
    path = ceh.create_path(
        "file.xlsx",
        Path("/mnt/clingen/"),
        "CEN",
        "002_251010_A01303_0320_BACWV9DRX7_37_CEN",
    )
    expected = Path(
        "/mnt/clingen/CEN/Run folders/251010_A01303_0320_BACWV9DRX7_CEN/file.xlsx"
    )
    assert isinstance(path, Path)
    assert path == expected


def test_create_path_wes():
    """Test path creation for WES assay."""
    path = ceh.create_path(
        "file.xlsx",
        Path("/mnt/clingen/"),
        "WES",
        "002_251010_A01303_0120_AHCWV8DRX7_38_TWE",
    )
    expected = Path("/mnt/clingen/WES/251010_A01303_0120_AHCWV8DRX7_TWE/file.xlsx")
    assert isinstance(path, Path)
    assert path == expected


def test_create_path_nan():
    """Test path creation with NaN run folder returns None."""
    assert (
        ceh.create_path("file.xlsx", Path("/mnt/clingen/"), "CEN", float("nan")) is None
    )


def test_create_path_unknown_assay():
    """Test path creation with unknown assay returns None."""
    assert (
        ceh.create_path(
            "file.xlsx",
            Path("/mnt/clingen/"),
            "UNKNOWN",
            "002_251010_A01303_0320_BACWV9DRX7_37_UNKNOWN",
        )
        is None
    )


def test_create_path_short_run_folder():
    """Test path creation with short run folder returns None."""
    assert (
        ceh.create_path("file.xlsx", Path("/mnt/clingen/"), "CEN", "002_251010") is None
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
def test_query_reports_for_project_exact_id_match(mock_find_data_objects):
    mock_find_data_objects.return_value = [
        {
            "describe": {
                "name": "119696617-25110R0008-25NGCEN82-9527-M-97444487_R208.1_SNV_1.xlsx"
            }
        },
        {
            "describe": {
                "name": "123696617-25110R0009-25NGCEN82-9527-F-97444487_R208.1_SNV_1.xlsx"
            }
        },
    ]

    records = ceh.query_reports_for_project("proj-1", ["1234567", "8901234"])

    expected_list = [
        {
            "sample_id": "25110R0008",
            "project_id": "proj-1",
            "file_name": "119696617-25110R0008-25NGCEN82-9527-M-97444487_R208.1_SNV_1.xlsx",
        },
        {
            "sample_id": "25110R0009",
            "project_id": "proj-1",
            "file_name": "123696617-25110R0009-25NGCEN82-9527-F-97444487_R208.1_SNV_1.xlsx",
        },
    ]
    assert records == expected_list


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


class TestExtractAssayFromFilename:
    """Test extracting assay from filename."""

    def test_valid_cen(self):
        assert ceh.extract_assay_from_filename("file_CEN_123.xlsx") == "CEN"

    def test_valid_wes(self):
        assert ceh.extract_assay_from_filename("file_WES_123.xlsx") == "WES"

    def test_no_assay(self):
        assert ceh.extract_assay_from_filename("file_123.xlsx") is None

    def test_unknown_assay(self):
        assert ceh.extract_assay_from_filename("file_UNKNOWN_123.xlsx") is None


class TestFindFileName:
    """Test finding file names in a project."""

    def test_no_files(self, monkeypatch):
        monkeypatch.setattr(ceh.dxpy, "find_data_objects", lambda **kwargs: [])
        assert ceh.find_file_name("sample*") is None

    def test_multiple_files(self, monkeypatch):
        monkeypatch.setattr(
            ceh.dxpy,
            "find_data_objects",
            lambda **kwargs: [
                {"describe": {"name": "sample1.xlsx"}},
                {"describe": {"name": "sample2.xlsx"}},
            ],
        )
        assert ceh.find_file_name("sample*") is None

    def test_single_file(self, monkeypatch):
        monkeypatch.setattr(
            ceh.dxpy,
            "find_data_objects",
            lambda **kwargs: [{"describe": {"name": "sample1.xlsx"}}],
        )
        assert ceh.find_file_name("sample*") == "sample1.xlsx"


class TestRCodeMatching:
    """Test R code matching in filenames."""
    @pytest.fixture
    def example_df(self):
        csv_path = (
            Path(__file__).parent
            / "test_data"
            / "clarity_extract_examples"
            / "test_all_reports_df.csv"
        )
        return pd.read_csv(csv_path)

    @pytest.fixture
    def example_df_conflicting_r_codes(self):
        csv_path = (
            Path(__file__).parent
            / "test_data"
            / "clarity_extract_examples"
            / "test_all_reports_df_conflicting_r_codes.csv"
        )
        return pd.read_csv(csv_path)

    @pytest.fixture
    def example_df_lowercase_r_codes(self):
        csv_path = (
            Path(__file__).parent
            / "test_data"
            / "clarity_extract_examples"
            / "test_all_reports_df_lowercase.csv"
        )
        return pd.read_csv(csv_path)

    def test_exact_match(self, example_df):
        row = example_df.iloc[1]
        assert ceh.is_report_code_in_list(row) is True

    def test_no_match(self, example_df_conflicting_r_codes):
        row = example_df_conflicting_r_codes.iloc[1]
        assert ceh.is_report_code_in_list(row) is False

    def test_report_code_none(self, example_df):
        row = example_df.iloc[2]
        assert ceh.is_report_code_in_list(row) is False

    def test_empty_r_codes_list_no_report_r_code(self, example_df):
        row = example_df.iloc[3]
        assert ceh.is_report_code_in_list(row) is False

    def test_empty_r_codes_list(self, example_df):
        row = example_df.iloc[4]
        assert ceh.is_report_code_in_list(row) is False

    def test_case_sensitive(self, example_df_lowercase_r_codes):
        # function is case-sensitive; lower-case report code should not match upper-case list
        row = example_df_lowercase_r_codes.iloc[0]
        assert ceh.is_report_code_in_list(row) is False
