import pandas as pd
import datetime
from sqlalchemy import text

def add_variants_to_db(df, engine):
    '''
    Update inca table to add variants
    Inputs:
        df (pd.Dataframe): dataframe with variant information
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
    Outputs:
        None, adds data to db
    '''
    with engine.begin() as conn:
        # Ensure the DataFrame is not empty before attempting to write
        if df.empty:
            print("No variants to add to inca table.")
            return
        # Use the to_sql method to write the DataFrame to the database
        rows = df.to_sql(
            "inca",
            conn,
            if_exists='append',
            schema='testdirectory',
            index=False
        )
        print(f"Added {rows} records to inca table")


def add_wb_to_db(workbook, parse_status, engine):
    '''
    Update inca_workbooks table to add workbooks
    Inputs:
        workbook (str): filename of workbook
        parse_status (str): value to use for parse_status
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
    Outputs:
        None, adds data to db
    '''
    now = datetime.datetime.now()

    with engine.begin() as conn:
        result = conn.execute(
            text("INSERT INTO testdirectory.inca_workbooks "
                 "(workbook_name, date, parse_status) "
                 "VALUES (:wb, :date, :status) "
                 "ON CONFLICT (workbook_name) DO NOTHING"),
            {"wb": workbook, "date": now, "status": parse_status}
        )


def update_db_for_parsed_wb(workbook, engine):
    '''
    Update inca_workbooks table to set parse_status to true for parsed
    workbooks
    Inputs:
        workbook (str): filename of workbook
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
    Outputs:
        None, adds data to db
    '''

    with engine.begin() as conn:
        result = conn.execute(
            text("UPDATE testdirectory.inca_workbooks "
                 "SET parse_status = TRUE "
                 "WHERE workbook_name = :wb"),
            {"wb": workbook}
        )


def add_submission_id_to_db(response, engine, variants):
    '''
    Add batch submission ID to inca table for all submitted variants
    Inputs:
        Response (dict): API response
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
        variants (list): list of variants submitted in API call
    Outputs:
        None, adds data to db
    '''
    add_quotes = [f"'{x}'" for x in variants]
    submitted_variants = ", ".join(add_quotes)
    sub_id = response.get('id')
    with engine.begin() as conn:
        # If submission ID exists, update the inca table with it
        # Otherwise, update the inca table with an error message
        if sub_id:
            conn.execute(
                f"UPDATE testdirectory.inca SET submission_id = '{sub_id}' "
                f"WHERE local_id in ({submitted_variants})"
            )
        else:
            error = response.get('message')
            conn.execute(
                f"UPDATE testdirectory.inca SET clinvar_status = 'ERROR: {error}' "
                f"WHERE local_id in ({submitted_variants})"
            )


def select_variants_from_db(organisation_id, engine, submitted, exclude=""):
    '''
    Select variants from inca table
    Inputs:
        organisation_id (str): ClinVar organisation ID for NUH or CUH
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
        submitted (str): value for column submission_id to filter SQL SELECT
        statement on
        exclude (str): Optional string for further filtering.
    Outputs:
        df (pandas.DataFrame): dataframe of records in table that meet the
        given filter
    '''
    # sanitise organisation_id to ensure it is a string
    if not isinstance(organisation_id, str):
        organisation_id = str(organisation_id)

    query_str = (
        "SELECT * FROM testdirectory.inca "
        "WHERE interpreted = 'yes' AND submission_id IS " + submitted +
        " AND accession_id IS NULL AND organisation_id = :org_id"
        " AND allele_origin = 'germline' "
        + exclude
    )
    with engine.connect() as conn:
        # Use text to safely parameterize the query
        df = pd.read_sql(text(query_str), conn, params={"org_id": organisation_id})
    return df


def select_workbooks_from_db(engine, parameter):
    '''
    Select workbooks from inca_workbooks table
    Inputs:
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
        parameter (str): parameter to filter SQL SELECT statement on
    Outputs:
        df (pandas.DataFrame): dataframe of records in table that meet the
        given parameter
    '''
    query = f"SELECT * FROM testdirectory.inca_workbooks WHERE {parameter}"
    with engine.connect() as conn:
        # Use text to safely parameterize the query
        df = pd.read_sql(text(query), conn)
    return df


def add_error_to_db(engine, workbook, error):
    '''
    If a workbook failed parsing, add the reason to the inca_workbooks table
    Inputs:
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
        workbook (str): file name of workbook
        error (str): reason for workbook failing parsing
    Outputs:
        None, adds data to db
    '''
    query = text("""
        UPDATE testdirectory.inca_workbooks
        SET parse_status = FALSE, comment = :err
        WHERE workbook_name = :wb
    """)
    with engine.begin() as conn:
        conn.execute(query, {"err": error, "wb": workbook})


def add_accession_ids_to_db(accession_ids, engine):
    '''
    Add ClinVar accession IDs to INCA database
    Inputs:
        accession_ids (dict): dict mapping local_id to ClinVar accession ID
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
    Outputs:
        None, adds data to db
    '''
    query = text("""
            UPDATE testdirectory.inca
            SET accession_id = :accession
            WHERE local_id = :local_id
        """)
    payload = [{"accession": acc, "local_id": local_id} for local_id, acc in accession_ids.items()]
    with engine.begin() as conn:
        conn.execute(query, payload)


def add_clinvar_submission_error_to_db(errors, engine):
    '''
    Add any ClinVar submission error to INCA database
    Inputs:
        errors (dict): dict mapping local_id to ClinVar submission error
        message
        engine (sqlalchemy.engine.Engine):
            SQLAlchemy engine for connection to AWS db
    Outputs:
        None, adds data to db
    '''
    query = text("""
                UPDATE testdirectory.inca
                SET clinvar_status = :error
                WHERE local_id = :local_id
            """)
    payload = [{"error": err, "local_id": lid} for lid, err in errors.items()]

    with engine.begin() as conn:
        # Batch submission which updates each local_id with its error
        conn.execute(query, payload)

