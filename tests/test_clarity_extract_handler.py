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
    assert result == {"proj-1": "002_foo_CEN", "proj-2": "002_bar_CEN"}


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


class TestPreProcessClarityExtract:
    """Test preprocessing of clarity extract DataFrame."""

    def test_preprocess_clarity_df_same_R_code_multiple_test_mode(self):
        example_df = pd.DataFrame(
            {
                "Beaker Procedure Name": ["RARE DISEASE NGS ANALYSIS"],
                "Specimen Identifier": [
                    "SP-250123R0037",
                ],
                "Test Directory Test Code": ["R228.1|R228.2"],
            }
        )

        processed_df = ceh.preprocess_clarity_extract(example_df)

        expected_df = pd.DataFrame(
            {
                "Beaker Procedure Name": ["RARE DISEASE NGS ANALYSIS"],
                "Specimen Identifier": [
                    "SP-250123R0037",
                ],
                "Test Directory Test Code": ["R228.1|R228.2"],
                "sample_id": [
                    "250123R0037",
                ],
                "R_codes": [
                    ["R228"],
                ],
            }
        )

        pd.testing.assert_frame_equal(processed_df, expected_df)

    def test_preprocess_clarity_df_single_R_code(self):
        example_df = pd.DataFrame(
            {
                "Beaker Procedure Name": ["CEN NGS"] * 2,
                "Specimen Identifier": ["SP-250126R0040", "SP-250127R0041"],
                "Test Directory Test Code": ["R208.1", "R149.1"],
            }
        )

        processed_df = ceh.preprocess_clarity_extract(example_df)

        expected_df = pd.DataFrame(
            {
                "Beaker Procedure Name": ["CEN NGS"] * 2,
                "Specimen Identifier": ["SP-250126R0040", "SP-250127R0041"],
                "Test Directory Test Code": ["R208.1", "R149.1"],
                "sample_id": [
                    "250126R0040",
                    "250127R0041",
                ],
                "R_codes": [
                    ["R208"],
                    ["R149"],
                ],
            }
        )

        pd.testing.assert_frame_equal(processed_df, expected_df)

    def test_preprocess_clarity_df_no_R_code(self):
        example_df = pd.DataFrame(
            {
                "Beaker Procedure Name": ["WES NGS"] * 2,
                "Specimen Identifier": ["SP-250128R0042", "SP-250129R0043"],
                "Test Directory Test Code": [pd.NA, pd.NA],
            }
        )

        processed_df = ceh.preprocess_clarity_extract(example_df)

        expected_df = pd.DataFrame(
            {
                "Beaker Procedure Name": pd.Series([], dtype="object"),
                "Specimen Identifier": pd.Series([], dtype="object"),
                "Test Directory Test Code": pd.Series([], dtype="object"),
                "sample_id": pd.Series([], dtype="object"),
                "R_codes": pd.Series([], dtype="object"),
            }
        )

        pd.testing.assert_frame_equal(processed_df, expected_df)


class TestPreprocessReportDf:
    """Test preprocessing of report DataFrame."""

    def test_preprocess_report_df(self):
        example_df = pd.DataFrame(
            {
                "file_name": ["129740955-24123R0012-24NGCEN42-9527-F-99347387_R228.1_SNV_1.xlsx"],
                "sample_id": ["24123R0012"],
                "project_name": ["002_240516_A01303_0387_BH7F2WDRX5_38_CEN"],
            }
        )

        base_path = "/test_workbooks/"
        processed_df = ceh.preprocess_report_df(example_df, base_path)

        expected_df = pd.DataFrame(
            {
                "file_name": ["129740955-24123R0012-24NGCEN42-9527-F-99347387_R228.1_SNV_1.xlsx"],
                "sample_id": ["24123R0012"],
                "project_name": ["002_240516_A01303_0387_BH7F2WDRX5_38_CEN"],
                "Assay": ["CEN"],
                "path": [Path("/test_workbooks/CEN/Run folders/240516_A01303_0387_BH7F2WDRX5_CEN/129740955-24123R0012-24NGCEN42-9527-F-99347387_R228.1_SNV_1.xlsx")],
                "instrument_id": ["129740955"],
                "full_sample_id": ["129740955-24123R0012"],
                "report_r_code": ["R228.1"],
            }
        )

        pd.testing.assert_frame_equal(processed_df, expected_df)


class TestFilteringReports:
    """Test filtering of reports DataFrame."""
    def test_filtering_reports_cnv_code(self):
        example_df = pd.DataFrame(
            {
                "file_name": [
                    "129740957-24123R0014-24NGCEN42-9527-F-99347389_R228.1_SNV_1.xlsx",
                    "129740957-24123R0014-24NGCEN42-9527-F-99347389_R228.1_CNV_1.xlsx",
                ],
                "sample_id": [
                    "24123R0014",
                    "24123R0014",
                ],
                "report_r_code": [
                    "R228.1",
                    "R228.1",
                ],
                "R_codes": [
                    ["R228"],
                    ["R228"]
                ]
            }
        )

        filtered_df, missing_data_df = ceh.filtering_reports(example_df)

        expected_filtered_df = pd.DataFrame(
            {
                "file_name": [
                    "129740957-24123R0014-24NGCEN42-9527-F-99347389_R228.1_SNV_1.xlsx",
                ],
                "sample_id": [
                    "24123R0014",
                ],
                "report_r_code": [
                    "R228.1",
                ],
                "R_codes": [
                    ["R228"]
                ],
            }
        )

        expected_missing_data_df = pd.DataFrame({
            'file_name': pd.Series(dtype='object'),
            'sample_id': pd.Series(dtype='object'),
            'report_r_code': pd.Series(dtype='object'),
            'R_codes': pd.Series(dtype='object')
        })

        pd.testing.assert_frame_equal(filtered_df, expected_filtered_df)
        pd.testing.assert_frame_equal(missing_data_df, expected_missing_data_df)

    def test_filtering_r_code_mismatch(self):
        example_df = pd.DataFrame(
            {
                "file_name": [
                    "129740958-24123R0015-24NGCEN42-9527-F-99347390_R228.1_SNV_1.xlsx",
                    "129740958-24123R0015-24NGCEN42-9527-F-99347390_R149.1_SNV_1.xlsx",
                ],
                "sample_id": [
                    "24123R0015",
                    "24123R0015",
                ],
                "report_r_code": [
                    "R228.1",
                    "R149.1",
                ],
                "R_codes": [
                    ["R228"],
                    ["R228"]
                ]
            }
        )

        filtered_df, missing_data_df = ceh.filtering_reports(example_df)

        expected_filtered_df = pd.DataFrame({
            'file_name': ["129740958-24123R0015-24NGCEN42-9527-F-99347390_R228.1_SNV_1.xlsx"],
            'sample_id': ["24123R0015"],
            'report_r_code': ["R228.1"],
            'R_codes': [["R228"]]
        })

        pd.testing.assert_frame_equal(filtered_df, expected_filtered_df)

        expected_filtered_df = pd.DataFrame({
            'file_name': pd.Series(dtype='object'),
            'sample_id': pd.Series(dtype='object'),
            'report_r_code': pd.Series(dtype='object'),
            'R_codes': pd.Series(dtype='object')
        })

        pd.testing.assert_frame_equal(missing_data_df, expected_filtered_df)

    def test_filtering_no_filename(self):
        example_df = pd.DataFrame(
            {
                "file_name": [
                    pd.NA,
                ],
                "sample_id": [
                    "24123R0015",
                ],
                "report_r_code": [
                    pd.NA
                ],
                "R_codes": [
                    ["R228"],
                ]
            }
        )

        filtered_df, missing_data_df = ceh.filtering_reports(example_df)
        expected_filtered_df = pd.DataFrame({
            'file_name': pd.Series(dtype='object'),
            'sample_id': pd.Series(dtype='object'),
            'report_r_code': pd.Series(dtype='object'),
            'R_codes': pd.Series(dtype='object')
        })

        pd.testing.assert_frame_equal(filtered_df, expected_filtered_df)

        expected_missing_data_df = pd.DataFrame(
            {
                "file_name": [pd.NA],
                "sample_id": ["24123R0015"],
                "report_r_code": [pd.NA],
                "R_codes": [["R228"]],
            }
        )

        pd.testing.assert_frame_equal(missing_data_df, expected_missing_data_df)
