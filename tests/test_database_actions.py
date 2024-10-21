import unittest
import unittest.mock as mock
from unittest.mock import call
from freezegun import freeze_time
import utils.database_actions as db
import pandas as pd

class TestDatabaseEngine(unittest.TestCase):
    '''
    Test all the functions in database_actions which generate SQL queries to
    read from the database using the SQLAlchemy engine directly
    '''
    variants = ['uid_12345', 'uid_67890']
    @freeze_time("2024-07-10 22:22:22")
    def test_add_wb_to_db(self):
        '''
        Test that add_wb_to_db is called with the expected SQL when given
        example inputs
        '''
        mock_engine = mock.MagicMock()
        expected_sql = (
            "INSERT INTO testdirectory.inca_workbooks "
            "(workbook_name, date, parse_status) "
            "VALUES ('test_workbook.xlsx', '2024-07-10 22:22:22', FAIL) "
            "ON CONFLICT (workbook_name) DO NOTHING"      
        )
        db.add_wb_to_db('test_workbook.xlsx', 'FAIL', mock_engine)
        mock_engine.execute.assert_called_once_with(expected_sql)

    def test_update_db_for_parsed_wb(self):
        mock_engine = mock.MagicMock()
        expected_sql = (
            "UPDATE testdirectory.inca_workbooks SET parse_status = TRUE "
            "WHERE workbook_name = 'test_workbook.xlsx'"
        )
        db.update_db_for_parsed_wb('test_workbook.xlsx', mock_engine)
        mock_engine.execute.assert_called_once_with(expected_sql)

    def test_add_submission_id_to_db_if_submission_id_returned(self):
        mock_engine = mock.MagicMock()
        response = {'id': 'SUB123456'}
        expected_sql = (
            "UPDATE testdirectory.inca SET submission_id = 'SUB123456' "
            "WHERE local_id in ('uid_12345', 'uid_67890')"
        )
        db.add_submission_id_to_db(response, mock_engine, self.variants)
        mock_engine.execute.assert_called_once_with(expected_sql)

    def test_add_submission_id_to_db_if_error_returned(self):
        mock_engine = mock.MagicMock()
        response = {'message': "No valid API key provided"}
        expected_sql = (
            "UPDATE testdirectory.inca SET clinvar_status = 'ERROR: No valid "
            "API key provided' WHERE local_id in ('uid_12345', 'uid_67890')"
        )
        db.add_submission_id_to_db(response, mock_engine, self.variants)
        mock_engine.execute.assert_called_once_with(expected_sql)

    def test_add_error_to_db(self):
        mock_engine = mock.MagicMock()
        expected_sql = (
            "UPDATE testdirectory.inca_workbooks SET parse_status = FALSE, "
            "comment = 'Parsing error' WHERE workbook_name = "
            "'test_workbook.xlsx'"
        )
        db.add_error_to_db(mock_engine, 'test_workbook.xlsx', 'Parsing error')
        mock_engine.execute.assert_called_once_with(expected_sql)
    
    def test_add_accession_ids_to_db(self):
        mock_engine = mock.MagicMock()
        accession_ids = {
            'uid_12345': 'SCV000012345',
            'uid_67890': 'SCV000067890'
        }
        uid_12345_sql = (
           "UPDATE testdirectory.inca SET accession_id = 'SCV000012345' "
           "WHERE local_id = 'uid_12345'"
        )
        uid_67890_sql = (
           "UPDATE testdirectory.inca SET accession_id = 'SCV000067890' "
           "WHERE local_id = 'uid_67890'"
        )
        db.add_accession_ids_to_db(accession_ids, mock_engine)

        with self.subTest("Assert called twice if two accession IDs to add"):
            assert mock_engine.execute.call_count == 2
        
        mock_engine.execute.assert_has_calls(
            [call(uid_12345_sql), call(uid_67890_sql)], any_order=True
        )

    def test_add_clinvar_submission_error_to_db(self):
        mock_engine = mock.MagicMock()
        errors = {
            'uid_12345': 'This record is submitted as novel but it should be '
            'submitted as an update',
            'uid_67890': 'The identifier you provided (MONDO:MONDO:0000) '
            'cannot be validated'
        }
        uid_12345_sql = (
           "UPDATE testdirectory.inca SET clinvar_status = 'ERROR: This record"
           " is submitted as novel but it should be submitted as an update' "
           "WHERE local_id = 'uid_12345'"
        )
        uid_67890_sql = (
           "UPDATE testdirectory.inca SET clinvar_status = 'ERROR: The "
           "identifier you provided (MONDO:MONDO:0000) cannot be validated' "
           "WHERE local_id = 'uid_67890'"
        )
        db.add_clinvar_submission_error_to_db(errors, mock_engine)

        with self.subTest("Assert called twice if two errors"):
            assert mock_engine.execute.call_count == 2

        mock_engine.execute.assert_has_calls(
            [call(uid_12345_sql), call(uid_67890_sql)], any_order=True
        )


class TestDatabasePandas(unittest.TestCase):
    '''
    Test all the functions in database_actions which interact with the
    database via pandas processes
    '''
    data = [{
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
    }]
    df = pd.DataFrame(data)
    @mock.patch('pandas.DataFrame.to_sql')
    def test_add_variants_to_db(self, pd_to_sql_mock):
        mock_engine = mock.MagicMock()
        db.add_variants_to_db(self.df, mock_engine)
        pd_to_sql_mock.assert_called_once_with(
            "inca", mock_engine, if_exists='append', schema='testdirectory',
            index=False
        )

    @mock.patch('pandas.read_sql')
    def test_select_variants_from_db(self, pd_read_sql_mock):
        mock_engine = mock.MagicMock()
        pd_read_sql_mock.return_value = self.df
        expected_sql = (
            "SELECT * FROM testdirectory.inca WHERE interpreted = 'yes' AND "
            "submission_id = 'SUB12345' AND accession_id is NULL AND "
            "organisation_id = '1234'"
        )
        return_df = db.select_variants_from_db(1234, mock_engine, 'SUB12345')


        with self.subTest("Returns value from pd.read_sql()"):
            pd.testing.assert_frame_equal(return_df, self.df)

        with self.subTest("pd.read_sql() called with correct SQL query"):
            pd_read_sql_mock.assert_called_once_with(expected_sql, mock_engine)

    @mock.patch('pandas.read_sql')
    def test_select_variants_from_db_with_exclude(self, pd_read_sql_mock):
        '''
        Test that when an exclude value is passed to select_variants_from_db
        it is added to the query
        '''
        mock_engine = mock.MagicMock()
        exclude = " AND panel != '_HGNC:7527'"
        pd_read_sql_mock.return_value = self.df
        expected_sql = (
            "SELECT * FROM testdirectory.inca WHERE interpreted = 'yes' AND "
            "submission_id = 'SUB12345' AND accession_id is NULL AND "
            "organisation_id = '1234' AND panel != '_HGNC:7527'"
        )
        return_df = db.select_variants_from_db(
            1234, mock_engine, 'SUB12345', exclude
        )
      
        with self.subTest("Returns value from pd.read_sql()"):
            pd.testing.assert_frame_equal(return_df, self.df)

        with self.subTest("pd.read_sql() called with correct SQL query"):
            pd_read_sql_mock.assert_called_once_with(expected_sql, mock_engine)
