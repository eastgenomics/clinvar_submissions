import unittest
import unittest.mock as mock
from unittest.mock import call
from freezegun import freeze_time
import utils.database_actions as db

class TestDatabase(unittest.TestCase):
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