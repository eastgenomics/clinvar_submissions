import pandas as pd
import datetime

def add_variants_to_db(df, engine):
    '''
    Update inca table to add variants
    Inputs
        df (pd.Dataframe): dataframe with variant information
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
    Outputs
        None, adds data to db
    '''
    rows = df.to_sql(
        "inca",
        engine,
        if_exists='append',
        schema='testdirectory',
        index=False
    )
    print(f"Added {rows} records to inca table")


def add_wb_to_db(workbook, parse_status, engine):
    '''
    Update inca_workbooks table to add workbooks
    Inputs
        workbook (str): filename of workbook
        parse_status (str): value to use for parse_status
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
    Outputs
        None, adds data to db
    '''
    now = datetime.datetime.now()
    engine.execute(
        f"INSERT INTO testdirectory.inca_workbooks"
        " (workbook_name, date, parse_status) "
        f"VALUES ('{workbook}', '{now}', {parse_status}) "
        "ON CONFLICT (workbook_name) DO NOTHING"
    )


def update_db_for_parsed_wb(workbook, engine):
    '''
    Update inca_workbooks table to set parse_status to true for parsed
    workbooks
    Inputs
        workbook (str): filename of workbook
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
    Outputs
        None, adds data to db
    '''
    engine.execute(
        "UPDATE testdirectory.inca_workbooks SET parse_status = TRUE "
        f"WHERE workbook_name = '{workbook}'"
    )


def add_submission_id_to_db(response, engine, variants):
    '''
    Add batch submission ID to inca table for all submitted variants
    Inputs
        Response (dict): API response
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
        variants (list): list of variants submitted in API call
    Outputs
        None, adds data to db
    '''
    add_quotes = [f"'{x}'" for x in variants]
    submitted_variants = ", ".join(add_quotes)
    sub_id = response.get('id')
    if sub_id:
        engine.execute(
            f"UPDATE testdirectory.inca SET submission_id = '{sub_id}' "
            f"WHERE local_id in ({submitted_variants})"
        )
    else:
        error = response.get('message')
        engine.execute(
            f"UPDATE testdirectory.inca SET clinvar_status = 'ERROR: {error}' "
            f"WHERE local_id in ({submitted_variants})"
        )


def select_variants_from_db(organisation_id, engine, submitted, exclude=""):
    '''
    Select variants from inca table
    Inputs
        organisation_id (str): ClinVar organisation ID for NUH or CUH
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
        submitted (str): value for column submission_id to filter SQL SELECT
        statement on
        exclude (str): Optional string for further filtering. 
    Outputs
        df (pandas.DataFrame): dataframe of records in table that meet the
        given filter
    '''
    df = pd.read_sql(
            "SELECT * FROM testdirectory.inca WHERE interpreted = 'yes' AND "
            f"submission_id is {submitted} AND accession_id is NULL AND "
            f"organisation_id = '{organisation_id}'{exclude}",
            engine
        )
    return df


def select_workbooks_from_db(engine, parameter):
    '''
    Select workbooks from inca_workbooks table
    Inputs
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
        parameter (str): parameter to filter SQL SELECT statement on
    Outputs
        df (pandas.DataFrame): dataframe of records in table that meet the
        given parameter
    '''
    df = pd.read_sql(
            f"SELECT * FROM testdirectory.inca_workbooks WHERE {parameter}",
            engine
        )
    return df


def add_error_to_db(engine, workbook, error):
    '''
    If a workbook failed parsing, add the reason to the inca_workbooks table
    Inputs
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
        workbook (str): file name of workbook
        error (str): reason for workbook failing parsing
    Outputs
        None, adds data to db
    '''
    engine.execute(
        "UPDATE testdirectory.inca_workbooks SET parse_status = FALSE, "
        f"comment = '{error}' WHERE workbook_name = '{workbook}'"
    )


def add_accession_ids_to_db(accession_ids, engine):
    '''
    Add ClinVar accession IDs to INCA database
    Inputs
        accession_ids (dict): dict mapping local_id to ClinVar accession ID
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
    Outputs
        None, adds data to db
    '''
    for local_id, accession in accession_ids.items():
        engine.execute(
            f"UPDATE testdirectory.inca SET accession_id = '{accession}' "
            f"WHERE local_id = '{local_id}'"
        )


def add_clinvar_submission_error_to_db(errors, engine):
    '''
    Add any ClinVar submission error to INCA database
    Inputs
        errors (dict): dict mapping local_id to ClinVar submission error
        message
        engine (sqlalchemy.engine.Engine): SQLAlchemy connection to AWS db
    Outputs
        None, adds data to db
    '''
    for local_id, error in errors.items():
        engine.execute(
            f"UPDATE testdirectory.inca SET clinvar_status = 'ERROR: {error}' "
            f"WHERE local_id = '{local_id}'"
        )
