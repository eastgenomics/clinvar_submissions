import unittest
import unittest.mock as mock
from unittest.mock import ANY
from unittest.mock import call
from freezegun import freeze_time
import utils.database_actions as db
import pandas as pd
from itertools import chain


class TestDatabaseEngine(unittest.TestCase):
    """
    Test all the functions in database_actions which generate SQL queries to
    read from the database using the SQLAlchemy engine directly
    """

    variants = ["uid_12345", "uid_67890"]

    @freeze_time("2024-07-10 22:22:22")
    def test_add_wb_to_db(self):
        """
        Test that add_wb_to_db is called with the expected SQL when given
        example inputs
        """
        # Prepare mock engine and connection
        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn

        expected_call = call(
            ANY, {"wb": "test_workbook.xlsx", "date": mock.ANY, "status": "FAIL"}
        )

        db.add_wb_to_db("test_workbook.xlsx", "FAIL", mock_engine)
        mock_conn.execute.assert_called_once_with(
            mock.ANY, {"wb": "test_workbook.xlsx", "date": mock.ANY, "status": "FAIL"}
        )
        # Check expected call was made
        self.assertIn(expected_call, mock_conn.execute.call_args_list)

    def test_update_db_for_parsed_wb(self):
        # Prepare mock engine and connection
        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn

        db.update_db_for_parsed_wb("test_workbook.xlsx", mock_engine)

        # Check that execute was called once
        mock_conn.execute.assert_called_once()

        # Get the call arguments
        call_args = mock_conn.execute.call_args
        sql_text_obj = call_args[0][0]  # First argument (the text object)
        params = call_args[0][1]  # Second argument (the parameters dict)

        # Convert text object to string to check SQL content
        actual_sql = str(sql_text_obj)

        with self.subTest("SQL contains expected UPDATE statement"):
            self.assertIn("UPDATE testdirectory.inca_workbooks", actual_sql)
            self.assertIn("SET parse_status = TRUE", actual_sql)
            self.assertIn("WHERE workbook_name = :wb", actual_sql)

        with self.subTest("Parameters are correct"):
            expected_params = {"wb": "test_workbook.xlsx"}
            self.assertEqual(params, expected_params)

    def test_add_submission_id_to_db_if_submission_id_returned(self):
        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        # Patch engine.begin().__enter__() to return mock_conn
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        response = {"id": "SUB123456"}
        expected_sql = (
            "UPDATE testdirectory.inca SET submission_id = 'SUB123456' "
            "WHERE local_id in ('uid_12345', 'uid_67890')"
        )
        db.add_submission_id_to_db(response, mock_engine, self.variants)
        mock_conn.execute.assert_called_once_with(expected_sql)

    def test_add_submission_id_to_db_if_error_returned(self):
        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        # Patch engine.begin().__enter__() to return mock_conn
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        response = {"message": "No valid API key provided"}
        expected_sql = (
            "UPDATE testdirectory.inca SET clinvar_status = 'ERROR: No valid "
            "API key provided' WHERE local_id in ('uid_12345', 'uid_67890')"
        )
        db.add_submission_id_to_db(response, mock_engine, self.variants)
        mock_conn.execute.assert_called_once_with(expected_sql)

    def test_add_error_to_db(self):
        # Prepare mock engine and connection
        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn

        db.add_error_to_db(mock_engine, "test_workbook.xlsx", "Parsing error")

        # Check that execute was called once
        mock_conn.execute.assert_called_once()

        # Get the call arguments
        call_args = mock_conn.execute.call_args
        sql_text_obj = call_args[0][0]  # First argument (the text object)
        params = call_args[0][1]  # Second argument (the parameters dict)

        # Convert text object to string to check SQL content
        actual_sql = str(sql_text_obj)

        with self.subTest("SQL contains expected UPDATE statement"):
            self.assertIn("UPDATE testdirectory.inca_workbooks", actual_sql)
            self.assertIn("SET parse_status = FALSE", actual_sql)
            self.assertIn("comment = :err", actual_sql)
            self.assertIn("WHERE workbook_name = :wb", actual_sql)

        with self.subTest("Parameters are correct"):
            expected_params = {"err": "Parsing error", "wb": "test_workbook.xlsx"}
            self.assertEqual(params, expected_params)

    def test_add_accession_ids_to_db(self):
        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        accession_ids = {"uid_12345": "SCV000012345", "uid_67890": "SCV000067890"}

        db.add_accession_ids_to_db(accession_ids, mock_engine)

        # Should be called once (batch)
        self.assertEqual(mock_conn.execute.call_count, 1)

        # Get the parameters passed to execute
        params = mock_conn.execute.call_args[0][1]
        expected = [
            {"accession": "SCV000012345", "local_id": "uid_12345"},
            {"accession": "SCV000067890", "local_id": "uid_67890"},
        ]
        # assert that params is a list of dicts with expected content
        assert len(params) == len(expected)
        assert all(param in params for param in expected)

    @mock.patch("utils.database_actions.text")
    def test_add_clinvar_submission_error_to_db(self, mock_text):
        # Prepare mock engine and connection
        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn

        errors = {
            "uid_12345": "This record is submitted as novel but it should be submitted as an update",
            "uid_67890": "The identifier you provided (MONDO:MONDO:0000) cannot be validated",
        }

        # Call the function
        db.add_clinvar_submission_error_to_db(errors, mock_engine)

        # Check that execute was called once with a list of error dicts
        expected_params = [
            {"error": errors["uid_12345"], "local_id": "uid_12345"},
            {"error": errors["uid_67890"], "local_id": "uid_67890"},
        ]
        mock_conn.execute.assert_called_once_with(mock_text.return_value, expected_params)


class TestDatabasePandas(unittest.TestCase):
    """
    Test all the functions in database_actions which interact with the
    database via pandas processes
    """

    data = [
        {
            "local_id": "uid-123456789",
            "linking_id": "uid-123456789",
            "chromosome": 7,
            "start": 117232266,
            "reference_allele": "C",
            "alternate_allele": "CA",
            "gene_symbol": "CFTR",
            "comment_on_classification": "PVS1,PM3_Strong",
            "germline_classification": "Pathogenic",
            "date_last_evaluated": "2024-10-10",
            "preferred_condition_name": "Cystic fibrosis",
            "collection_method": "clinical testing",
            "affected_status": "yes",
            "allele_origin": "germline",
            "ref_genome": "GRCh37.p13",
        }
    ]
    df = pd.DataFrame(data)

    @mock.patch("pandas.DataFrame.to_sql")
    def test_add_variants_to_db(self, pd_to_sql_mock):
        mock_engine = mock.MagicMock()
        mock_conn = mock.MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        db.add_variants_to_db(self.df, mock_engine)
        pd_to_sql_mock.assert_called_once_with(
            "inca", mock_conn, if_exists="append", schema="testdirectory", index=False
        )

    @mock.patch("pandas.read_sql")
    def test_select_variants_from_db(self, pd_read_sql_mock):
        mock_engine = mock.MagicMock()
        pd_read_sql_mock.return_value = self.df

        return_df = db.select_variants_from_db("1234", mock_engine, "NOT NULL")

        with self.subTest("Returns value from pd.read_sql()"):
            pd.testing.assert_frame_equal(return_df, self.df)

        with self.subTest("pd.read_sql() called with text object and params"):
            # Check that pd.read_sql was called with a text object and params
            self.assertEqual(pd_read_sql_mock.call_count, 1)
            call_args = pd_read_sql_mock.call_args
            # First arg should be a text object, second should be engine, third should be params
            self.assertEqual(len(call_args[0]), 2)  # Two positional args
            self.assertIn("params", call_args[1])  # params as keyword arg
            self.assertEqual(call_args[1]["params"], {"org_id": "1234"})

    def assertSQLContains(self, mock_call, expected_fragments, expected_params=None):
        """Helper method to assert SQL content and parameters"""
        call_args = mock_call.call_args

        # Get SQL text
        sql_text_obj = call_args[0][0]
        actual_sql = str(sql_text_obj)

        print(f"Actual SQL: {actual_sql}")

        # Check SQL fragments
        for fragment in expected_fragments:
            self.assertIn(fragment, actual_sql)

        # Check parameters if provided
        if expected_params:
            params = call_args[1].get("params", {})
            for key, value in expected_params.items():
                self.assertEqual(params[key], value)

    @mock.patch("pandas.read_sql")
    def test_select_variants_from_db_with_sql_check(self, pd_read_sql_mock):
        mock_engine = mock.MagicMock()
        pd_read_sql_mock.return_value = self.df

        return_df = db.select_variants_from_db("1234", mock_engine, "NOT NULL")

        # Use helper to check SQL
        expected_fragments = [
            "SELECT * FROM testdirectory.inca",
            "WHERE interpreted = 'yes'",
            "submission_id IS NOT NULL",
            "organisation_id = :org_id",
            "allele_origin = 'germline'",
        ]
        expected_params = {"org_id": "1234"}

        self.assertSQLContains(pd_read_sql_mock, expected_fragments, expected_params)

    @mock.patch("pandas.read_sql")
    def test_select_variants_from_db_with_exclude(self, pd_read_sql_mock):
        """
        Test that when an exclude value is passed to select_variants_from_db
        it is added to the query
        """
        mock_engine = mock.MagicMock()
        exclude = " AND panel != '_HGNC:7527'"
        pd_read_sql_mock.return_value = self.df

        return_df = db.select_variants_from_db("1234", mock_engine, "NULL", exclude)

        with self.subTest("Returns mocked value from pd.read_sql()"):
            pd.testing.assert_frame_equal(return_df, self.df)

        with self.subTest("pd.read_sql() called with text object and params"):
            # Check that pd.read_sql was called with a text object and params
            self.assertEqual(pd_read_sql_mock.call_count, 1)
            call_args = pd_read_sql_mock.call_args
            # Check that exclude parameter was included by inspecting the SQL string
            sql_text = str(call_args[0][0])  # Convert text object to string
            self.assertIn("SELECT * FROM testdirectory.inca", sql_text)
            self.assertIn("WHERE interpreted = 'yes'", sql_text)
            self.assertIn("submission_id IS NULL", sql_text)
            self.assertIn("organisation_id = :org_id", sql_text)
            self.assertIn("allele_origin = 'germline'", sql_text)
            self.assertIn("panel != '_HGNC:7527'", sql_text)  # The exclude part

            self.assertEqual(call_args[1]["params"], {"org_id": "1234"})

    @mock.patch("pandas.read_sql")
    def test_select_wb_from_db(self, pd_read_sql_mock):
        mock_engine = mock.MagicMock()
        data = {"workbook_name": ["test_wb.xlsx"], "parse_status": False}
        df = pd.DataFrame(data)
        pd_read_sql_mock.return_value = df

        return_df = db.select_workbooks_from_db(mock_engine, "parse_status = FALSE")

        with self.subTest("Returns mocked value from pd.read_sql()"):
            pd.testing.assert_frame_equal(return_df, df)

        with self.subTest("pd.read_sql() called with text object"):
            # Check that pd.read_sql was called with a text object
            self.assertEqual(pd_read_sql_mock.call_count, 1)
            call_args = pd_read_sql_mock.call_args
            # Should be called with text object and engine
            self.assertEqual(len(call_args[0]), 2)
            # Check the SQL contains our parameter
            sql_text = str(call_args[0][0])
            self.assertIn("parse_status = FALSE", sql_text)
