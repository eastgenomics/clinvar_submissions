import re
from datetime import date
from dateutil import parser as date_parser
from utils.database_actions import add_error_to_db
import pandas as pd
import numpy as np
import os
import requests
import json
import uuid
import time


def get_folder_of_input_file(filename: str) -> str:
    '''
    Get the folder of input file
    Inputs:
        filename (str): filename 
    Outputs:
        folder (str): folder name
    '''
    folder = os.path.basename(os.path.normpath(os.path.dirname(filename)))
    return folder


def get_workbook_data(workbook, config, unusual_sample_name, filename, file, engine):
    '''
    Function that runs functions to extract data from each sheet in the
    workbook and merges it together into one dataframe
    Inputs
        workbook (openpyxl wb object): workbook being used
        config (dict): config variable
        unusual_sample_name (bool): whether the sample has an unusual name, if
        True, will skip validating the sample name conventions.
        filename (str): string of workbook name with preceding path
        file (str): string of workbook name without preceding path
        engine (SQLAlchemy engine): connection to database
    Outputs
        df_final (pd.DataFrame): data frame extracted from workbook
    '''
    # get data from summary sheet, included variants sheet and interpret sheets
    df_summary, error = get_summary_fields(workbook, config, unusual_sample_name, filename)
    if error is not None:
        add_error_to_db(engine, file, error)
        return None
    df_included = get_included_fields(workbook, filename)
    df_interpret, error = get_report_fields(workbook, config, df_included)
    if error is not None:
        add_error_to_db(engine, file, error)
        return None

    # merge these to get one df
    if not df_included.empty:
        df_merged = pd.merge(df_included, df_summary, how="cross")
    else:
        df_merged = pd.concat([df_summary, df_included], axis=1)

    df_final = pd.merge(df_merged, df_interpret, on="hgvsc", how="left")

    df_final["germline_classification"] = df_final[
        "germline_classification"
        ].replace(
            {
                "Likely Pathogenic": "Likely pathogenic",
                "Uncertain Significance": "Uncertain significance",
                "Likely Benign": "Likely benign",
            }
        )

    return df_final


def get_summary_fields(workbook, config, unusual_sample_name, filename):
    '''
    Extract data from summary sheet of variant workbook
    Inputs
        workbook (openpyxl wb object): workbook being used
        config (dict): config variable
        unusual_sample_name (bool): whether the sample has an unusual name, if
        True, will skip validating the sample name conventions.
        filename (str): string of workbook name
    Outputs
        df_summary (pd.DataFrame): data frame extracted from workbook summary
        sheet
        err_msg (str): error message
    '''
    sampleID = workbook["summary"]["B1"].value

    clinical_indication = workbook["summary"]["F1"].value
    # Handle cases with multiple clinical indications
    if ";" in clinical_indication:
        split_ci = clinical_indication.split(";")
        condition_names = []
        test_codes = []
        for ci in split_ci:
            condition_names.append(ci.split("_")[1])
            test_codes.append(ci.split("_")[0])
        condition_names = ";".join(condition_names)
        test_codes = ";".join(test_codes)
    else:
        test_codes = clinical_indication.split("_")[0]
        condition_names = clinical_indication.split("_")[1]

    panel = workbook["summary"]["F2"].value
    date_evaluated = workbook["summary"]["G22"].value
    instrument, sample, batch, testcode, _, probeset = sampleID.split("-")
    ref_genome = "not_defined"
    for cell in workbook["summary"]["A"]:
        if cell.value == "Reference:":
            ref_genome = workbook["summary"][f"B{cell.row}"].value

    # checking sample naming
    error_msg = None
    if not unusual_sample_name:
        check_sample_name(
            instrument, sample, batch, testcode, probeset
        )
    d = {
        "instrument_id": instrument,
        "specimen_id": sample,
        "batch_id": batch,
        "test_code": testcode,
        "probeset_id": probeset,
        "r_code": test_codes,
        "preferred_condition_name": condition_names,
        "panel": panel,
        "ref_genome": ref_genome,
        "date_last_evaluated": date_evaluated,
    }
    df_summary = pd.DataFrame([d])

    # If no date last evaluated, use today's date
    df_summary['date_last_evaluated'] = df_summary[
        'date_last_evaluated'
    ].fillna(str(date.today()))

    # Catch if workbook has value for date last evaluated which is not datetime
    # compatible
    # Can test with first item in series as all rows have the same date value
    try:
        r = bool(date_parser.parse(str(df_summary['date_last_evaluated'][0])))
    except date_parser._parser.ParserError:
        error_msg = (
            f"Value for date last evaluated \"{date_evaluated}\" is not "
            "compatible with datetime conversion"
        )
        return df_summary, error_msg

    df_summary["date_last_evaluated"] = pd.to_datetime(
        df_summary["date_last_evaluated"]
    )
    df_summary["institution"] = config.get("Institution")
    df_summary["collection_method"] = config.get("Collection method")
    df_summary["allele_origin"] = config.get("Allele origin")
    df_summary["affected_status"] = config.get("Affected status")

    # getting the folder name of workbook
    # the folder name should return designated folder for either CUH or NUH
    # TODO this whole thing could get removed depending on if we continue to
    # use folders to differentiate between NUH and CUH workbooks
    folder_name = get_folder_of_input_file(filename)
    if folder_name == config.get("CUH folder"):
        df_summary["organisation"] = config.get("CUH Organisation")
        df_summary["organisation_id"] = config.get("CUH org ID")

    elif folder_name == config.get("NUH folder"):
        df_summary["organisation"] = config.get("NUH Organisation")
        df_summary["organisation_id"] = config.get("NUH org ID")

    else:
        error_msg = "Workbook folder is not CUH or NUH folder given in config"

    return df_summary, error_msg


def get_included_fields(workbook, filename) -> pd.DataFrame:
    '''
    Extract data from included sheet of variant workbook
    Inputs:
        workbook (openpyxl wb object): workbook being used
        filename (str): string of workbook name
    Outputs
        df_included (pd.DataFrame): data frame extracted from included sheet
    '''
    num_variants = workbook["summary"]["C38"].value
    df = pd.read_excel(
        filename,
        sheet_name="included",
        usecols= [
            "CHROM",
            "POS",
            "REF",
            "ALT",
            "SYMBOL",
            "HGVSc",
            "Consequence",
            "Interpreted",
            "Comment",
        ],
        nrows=num_variants,
    )
    if len(df["Interpreted"].value_counts()) > 0:
        df["Interpreted"] = df["Interpreted"].str.lower()

    # Rename to match INCA db columns
    df.rename(
        columns={
            "CHROM": "chromosome",
            "SYMBOL": "gene_symbol",
            "POS": "start",
            "REF": "reference_allele",
            "ALT": "alternate_allele",
            "HGVSc": "hgvsc",
            "Interpreted": "interpreted",
            "Consequence": "consequence",
            "Comment": "classification_comment"
        },
        inplace=True,
    )
    df["local_id"] = ""
    for row in range(df.shape[0]):
        unique_id = uuid.uuid1()
        df.loc[row, "local_id"] = f"uid_{unique_id.time}"
        time.sleep(0.5)
    df["linking_id"] = df["local_id"]

    return df


def get_report_fields(workbook, config, df_included):
    '''
    Extract data from interpret sheet(s) of variant workbook
    Inputs:
        workbook (openpyxl wb object): workbook being used
        config (dict): config variable
        df_included (pd.DataFrame): data frame extracted from included sheet
    Outputs
        df_included (pd.DataFrame): dataframe extracted from interpret sheet(s)
        err_msg (str): error message # TODO this error message will be removed

    '''
    field_cells = config.get("field_cells")
    col_name = [i[0] for i in field_cells]
    df_report = pd.DataFrame(columns=col_name)
    report_sheets = [
        idx
        for idx in workbook.sheetnames
        if idx.lower().startswith("interpret")
    ]

    for idx, sheet in enumerate(report_sheets):
        for field, cell in field_cells:
            if workbook[sheet][cell].value is not None:
                df_report.loc[idx, field] = workbook[sheet][cell].value
    df_report.reset_index(drop=True, inplace=True)
    error_msg = None
    if not df_report.empty:
        error_msg = check_interpret_table(df_report, df_included, config)

    if not error_msg:
        acgs = config.get("acgs_criteria")

        df_report = make_acgs_criteria_null_if_not_applied(df_report, acgs)

        df_report = add_comment_on_classification(df_report, acgs, config)

    return df_report, error_msg


def make_acgs_criteria_null_if_not_applied(df, acgs_criteria):
    '''
    The workbook has a value "NA" for ACGS criteria that was not applied. This
    function finds any variant row that had "NA" for a criteria and changes it
    to null. If an ACGS criteria is null, the evidence column for that column
    should also be null, so this function also sets any evidence field for a
    criteria that was not applied to null.
    Inputs:
        df (pd.Dataframe): a dataframe with a column for each ACGS criterion
        and each row the strength of that criteria applied to the row's variant
        acgs_criteria (list): list of ACGS criteria that make up the columns in
        the df
    Outputs:
        df (pd.Dataframe): the same dataframe, now with null instead of "NA"
        for criteria that were not applied, and a null value for evidence for
        all criteria not applied.
    '''
    df[acgs_criteria] = df[acgs_criteria] 

    for index, row, in df.iterrows():
        for criterion in acgs_criteria:
            if row[criterion] == "NA":
                df.loc[index, criterion] = np.nan
            if pd.isna(row[criterion]):
                df.loc[index, criterion + "_evidence"] = np.nan
    return df


def add_comment_on_classification(df, acgs_criteria, config):
    '''
    This function should take in a df with a column for each ACGS criteria with
    the values in that column being the strength of the criteria and return the
    same df but with a value in the comment_on_classification column that
    summarises all the ACGS criteria applied for each variant.
    If the criterion has the same strength as default for that criterion, the
    strength is not included, if it differs it is included
    e.g. if the df has:
        pvs1    pm3         pp3
        null    supporting  supporting
    the comment_on_classification should be PM3_Supporting,PP3

    Inputs:
        df (pd.Dataframe): dataframe with a column for each ACGS criterion and
        each row the strength of that criteria applied to the row's variant
        acgs_criteria (list): list of ACGS criteria that make up the columns in
        the df
        config (dict): config file as dict, contains a dict mapping ACGS
        criteria to their default strength
    Outputs:
        df (pd.Dataframe): the same dataframe, now with a value in the
        comment_on_classification column which summarises all the ACGS criteria
        applied for the variants
    '''
    matched_strength = config.get("matched_strength")
    df["comment_on_classification"] = ""

    for index, row, in df.iterrows():
        acgs = {}
        for criterion in acgs_criteria:
            if pd.notna(row[criterion]):
                acgs[criterion.upper()] = row[criterion]
                if matched_strength[criterion.upper()[:-1]] == row[criterion]:
                    acgs[criterion.upper()] = ""

        comment = ','.join([
            f"{criterion}_{strength}"if strength != ""
            else criterion for criterion, strength in acgs.items()
        ])
        df.loc[index, "comment_on_classification"] = comment

    return df


def select_api_url(clinvar_testing, config):
    '''
    Select which API URL to use depending on if this is a test run or if
    variants are planned to be submitted to ClinVar
    Inputs
        clinvar_testing (bool): if True, use test API. If false, use live API
        config (dict): config variable containing URLS for API
    Outputs
        api_url: clinvar api URL, either for the test API or the live API
    '''
    if clinvar_testing is True:
        api_url = config.get("test_api_endpoint")
        print(
            f"Running in test mode, using {api_url}"
        )
    elif clinvar_testing is False:
        api_url = config.get("live_api_endpoint")
        print(
            f"Running in live mode, using {api_url}"
        )
    else:
        raise ValueError(
            f"Value for testing {clinvar_testing} neither True nor False."
            " Please specify whether this is a test run or not."
        )
    return api_url


def check_interpret_table(df_interpret, df_included, config):
    '''
    Check if ACMG classification and HGVSc are correctly
    filled in in the interpret table(s)
    Inputs
        df_interpret (pd.Dataframe): df from interpret sheet(s)
        df_included (pd.Dataframe): df from included sheet
        config (dict): config variable
    Outputs
      error_msg (str): error message
    '''
    error_msg = []
    strength_dropdown = config.get("strength_dropdown")
    BA1_dropdown = config.get("BA1_dropdown")
    for row in range(df_interpret.shape[0]):
        try:
            assert (
                pd.notna(df_interpret.loc[row, "germline_classification"])
            ), "empty ACMG classification in interpret table"
            assert df_interpret.loc[row, "germline_classification"] in [
                "Pathogenic",
                "Likely Pathogenic",
                "Uncertain Significance",
                "Likely Benign",
                "Benign",
            ], "wrong ACMG classification in interpret table"
            assert (
                pd.notna(df_interpret.loc[row, "hgvsc"])
            ), "empty HGVSc in interpret table"
            assert df_interpret.loc[row, "hgvsc"] in list(df_included["hgvsc"]), (
                "HGVSc in interpret table does not match with that in "
                "included sheet"
            )
            acgs_criteria = config.get("acgs_criteria")
            for criteria in acgs_criteria:
                if pd.notna(df_interpret.loc[row, criteria]):
                    assert (
                        df_interpret.loc[row, criteria] in strength_dropdown
                    ), f"Wrong strength in {criteria}"

            if pd.notna(df_interpret.loc[row, "ba1"]):
                assert (
                    df_interpret.loc[row, "ba1"] in BA1_dropdown
                ), "Wrong strength in BA1"

        except AssertionError as msg:
            error_msg.append(str(msg))

    error_msg = "".join(error_msg)

    if error_msg == "":
        error_msg = None

    return error_msg


def checking_sheets(workbook):
    '''
    Check if extra row(s)/col(s) are added in the sheets
    Inputs
        workbook (openpyxl wb object): object of query workbook with variants
    Outputs
        error_msg (str): error message
    '''
    summary = workbook["summary"]
    reports = [
        idx
        for idx in workbook.sheetnames
        if idx.lower().startswith("interpret")
    ]
    try:
        assert (
            summary["G21"].value == "Date"
        ), "extra col(s) added or change(s) done in summary sheet"
        for sheet in reports:
            report = workbook[sheet]
            assert report["B26"].value == "FINAL ACMG CLASSIFICATION", (
                "extra row(s) or col(s) added or change(s) done in "
                "interpret sheet"
            )
            assert report["L8"].value == "B_POINTS", (
                "extra row(s) or col(s) added or change(s) done in "
                "interpret sheet"
            )
        error_msg = None
    except AssertionError as msg:
        error_msg = str(msg)

    return error_msg


def check_interpreted_col(df):
    '''
    Check if interpreted col in included sheet is correctly filled in
    Inputs
        df (pd.DataFrame): merged dataframe with data from workbook
        error_msg (str): error message
    '''
    row_yes = df[df["Interpreted"] == "yes"].index.tolist()
    error_msg = []
    for row in range(df.shape[0]):
        if row in row_yes:
            try:
                assert (
                    df.loc[row, "germline_classification"].notna()
                ), f"Wrong interpreted column in row {row+1} of included sheet"
            except AssertionError as msg:
                error_msg.append(str(msg))

        else:
            try:
                assert df.loc[row, "interpreted"] == "no", (
                    f"Wrong interpreted column dropdown in row {row+1} "
                    "of included sheet"
                )
                assert (
                    df.loc[row, "germline_classification"].isna()
                ), f"Wrong interpreted column in row {row+1} of included sheet"
            except AssertionError as msg:
                error_msg.append(str(msg))

    error_msg = " ".join(error_msg)

    if error_msg == "":
        error_msg = None

    return error_msg


def check_sample_name(instrumentID, sample_ID, batchID, testcode, probesetID):
    '''
    Checking that individual parts of sample name have expected naming format
    Inputs
      str values for instrumentID, sample_ID, batchID, testcode,
      probesetID
    Outputs
        error_msg (str): error message
    '''
    try:
        assert re.match(
            r"^\d{9}$", instrumentID
        ), "Unusual name for instrumentID"
        assert re.match(r"^\d{5}[A-Z]\d{4}$", sample_ID), "Unusual sampleID"
        assert re.match(r"^\d{2}[A-Z]{5}\d{1,}$", batchID), "Unusual batchID"
        assert re.match(r"^\d{4}$", testcode), "Unusual testcode"
        assert 0 < len(probesetID) < 20, "probesetID is too long/short"
        assert (
            probesetID.isalnum() and not probesetID.isalpha()
        ), "Unusual probesetID"
        error_msg = None
    except AssertionError as msg:
        error_msg = str(msg)

    return error_msg


def submission_status_check(submission_id, headers, api_url):
    '''
    Queries ClinVar API about a submission ID to obtain more details about its
    submission record.
    Inputs:
        submission_id:  the generated submission id from ClinVar when a
        submission has been posted to their API
        headers: the required API url
    Outputs:
        status_response: the API response
    '''

    url = os.path.join(api_url, submission_id, "actions")
    response = requests.get(url, headers=headers)
    response_content = response.content.decode("UTF-8")
    for k, v in response.headers.items():
        print(f"{k}: {v}")
    print(response_content)
    if response.status_code not in [200]:
        raise RuntimeError(
            "Status check failed:\n" + str(headers) + "\n" + url
            + "\n" + response_content
        )

    status_response = json.loads(response_content)

    # Load summary file
    action = status_response["actions"][0]
    status = action["status"]
    print(f"Submission {submission_id} has status {status}")

    responses = action["responses"]
    if len(responses) == 0:
        print("Status 'responses' field had no items, check back later")
    else:
        print(
            "Status response had a response, attempting to "
            "retrieve any files listed"
        )
        try:
            f_url = responses[0]["files"][0]["url"]
        except (KeyError, IndexError) as error:
            f_url = None
            print(
                f"Error retrieving files: {error}.\n No API url for summary"
                "file found. Cannot query API for summary file based on "
                f"response {responses}"
            )

        if f_url is not None:
            print("GET " + f_url)
            f_response = requests.get(f_url, headers=headers)
            f_response_content = f_response.content.decode("UTF-8")
            if f_response.status_code not in [200]:
                raise RuntimeError(
                    "Status check summary file fetch failed:"
                    f"{f_response_content}"
                )
            file_content = json.loads(f_response_content)
            status_response = file_content

    return status, status_response
