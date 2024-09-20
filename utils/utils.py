import dxpy
import re
from datetime import datetime, date
from dateutil import parser as date_parser
import pandas as pd
import numpy as np
import os
import requests
import json
import uuid
import time


def get_folder_of_input_file(filename: str) -> str:
    '''
    TODO note: this could change depending on if we stop being dependent on
    NUH/CUH folder paths in clingen 
    Get the folder of input file
    Inputs:
        filename (str): filename 
    Outputs:
        folder (str): folder name
    '''
    folder = os.path.basename(os.path.normpath(os.path.dirname(filename)))
    return folder


def get_workbook_data(workbook, config, unusual_sample_name):
    '''
    Function that runs functions to extract data from each sheet in the
    workbook and merges it together into one dataframe
    Inputs
        workbook (openpyxl wb object): workbook being used
        config (dict): config variable
        unusual_sample_name (bool): whether the sample has an unusual name, if
        True, will skip validating the sample name conventions.
    Outputs
        df_final (pd.DataFrame): data frame extracted from workbook
    '''
    # get data from summary sheet, included variants sheet and interpret sheets
    df_summary = get_summary_fields(workbook, config, unusual_sample_name)
    df_included = get_included_fields(workbook)
    df_interpret = get_report_fields(workbook, df_included)

    # merge these to get one df
    if not df_included.empty:
        df_merged = pd.merge(df_included, df_summary, how="cross")
    else:
        df_merged = pd.concat([df_summary, df_included], axis=1)

    df_final = pd.merge(df_merged, df_interpret, on="HGVSc", how="left")

    return df_final


def get_summary_fields(workbook, config, unusual_sample_name):
    '''
    Extract data from summary sheet of variant workbook
    Inputs
        workbook (openpyxl wb object): workbook being used
        config (dict): config variable
        unusual_sample_name (bool): whether the sample has an unusual name, if
        True, will skip validating the sample name conventions.
    Outputs
        df_summary (pd.DataFrame): data frame extracted from workbook summary
        sheet
    '''
    sampleID = workbook["summary"]["B1"].value
    clinical_indication = workbook["summary"]["F1"].value
    if ";" in clinical_indication:
        split_CI = clinical_indication.split(";")
        indication = []
        Rcode = []
        for each in split_CI:
            remove_R = each.split("_")[1]
            indication.append(remove_R)
            Rcode.append(each.split("_")[0])
        new_CI = ";".join(indication)
        combined_Rcode = ";".join(Rcode)
    else:
        new_CI = clinical_indication.split("_")[1]
        combined_Rcode = clinical_indication.split("_")[0]

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
        "Instrument ID": instrument,
        "Specimen ID": sample,
        "Batch ID": batch,
        "Test code": testcode,
        "Probeset ID": probeset,
        "Rcode": combined_Rcode,
        "Preferred condition name": new_CI,
        "Panel": panel,
        "Ref_genome": ref_genome,
        "Date last evaluated": date_evaluated,
    }
    df_summary = pd.DataFrame([d])

    # If no date last evaluated, use today's date
    df_summary['Date last evaluated'] = df_summary[
        'Date last evaluated'
    ].fillna(str(date.today()))

    # Catch if workbook has value for date last evaluated which is not datetime
    # compatible
    # Can test with first item in series as all rows have the same date value
    try:
        r = bool(date_parser.parse(str(df_summary['Date last evaluated'][0])))
    except date_parser._parser.ParserError:
        error_msg = (
            f"Value for date last evaluated \"{date_evaluated}\" is not "
            "compatible with datetime conversion"
        )
        return df_summary, error_msg

    df_summary["Date last evaluated"] = pd.to_datetime(
        df_summary["Date last evaluated"]
    )
    df_summary["Institution"] = config.get("Institution")
    df_summary["Collection method"] = config.get("Collection method")
    df_summary["Allele origin"] = config.get("Allele origin")
    df_summary["Affected status"] = config.get("Affected status")

    # getting the folder name of workbook
    # the folder name should return designated folder for either CUH or NUH
    # TODO this whole thing could get removed depending on if we continue to
    # use folders to differentiate between NUH and CUH workbooks
    folder_name = get_folder_of_input_file(workbook.name)
    if folder_name == config.get("CUH folder"):
        df_summary["Organisation"] = config.get("CUH Organisation")
        df_summary["OrganisationID"] = config.get("CUH org ID")

    elif folder_name == config.get("NUH folder"):
        df_summary["Organisation"] = config.get("NUH Organisation")
        df_summary["OrganisationID"] = config.get("NUH org ID")

    else:
        print("Running for the wrong folder") # TODO change this

    return df_summary


def get_included_fields(workbook) -> pd.DataFrame:
    '''
    Extract data from included sheet of variant workbook
    Inputs:
        workbook (openpyxl wb object): workbook being used
    Outputs
        df_included (pd.DataFrame): data frame extracted from included sheet
    '''
    num_variants = workbook["summary"]["C38"].value
    interpreted_col = get_col_letter(workbook["included"], "Interpreted")
    df = pd.read_excel(
        workbook,
        sheet_name="included",
        usecols=f"A:{interpreted_col}",
        nrows=num_variants,
    )
    df_included = df[
        [
            "CHROM",
            "POS",
            "REF",
            "ALT",
            "SYMBOL",
            "HGVSc",
            "Consequence",
            "Interpreted",
            "Comment",
        ]
    ].copy()
    if len(df_included["Interpreted"].value_counts()) > 0:
        df_included["Interpreted"] = df_included["Interpreted"].str.lower()
    df_included.rename(
        columns={
            "CHROM": "Chromosome",
            "SYMBOL": "Gene symbol",
            "POS": "Start",
            "REF": "Reference allele",
            "ALT": "Alternate allele",
        },
        inplace=True,
    )
    df_included["Local ID"] = ""
    for row in range(df_included.shape[0]):
        unique_id = uuid.uuid1()
        df_included.loc[row, "Local ID"] = f"uid_{unique_id.time}"
        time.sleep(0.5)
    df_included["Linking ID"] = df_included["Local ID"]

    return df_included


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
        error_msg = check_interpret_table(df_report, df_included)
    if not error_msg:
        # put strength as nan if it is 'NA'
        for row in range(df_report.shape[0]):
            for column in range(5, df_report.shape[1], 2):
                if df_report.iloc[row, column] == "NA":
                    df_report.iloc[row, column] = np.nan

        # removing evidence value if no strength
        for row in range(df_report.shape[0]):
            for column in range(5, df_report.shape[1], 2):
                if df_report.isnull().iloc[row, column]:
                    df_report.iloc[row, column + 1] = np.nan

        # getting comment on classification for clinvar submission
        matched_strength = config.get("matched_strength")
        df_report["Comment on classification"] = ""
        for row in range(df_report.shape[0]):
            evidence = []
            for column in range(5, df_report.shape[1] - 1, 2):
                if not df_report.isnull().iloc[row, column]:
                    evidence.append(
                        [
                            df_report.columns[column],
                            df_report.iloc[row, column],
                        ]
                    )
            for index, value in enumerate(evidence):
                for st1, st2 in matched_strength:
                    if st1 in value[0] and st2 == value[1]:
                        evidence[index][1] = ""
            evidence_pair = []
            for e in evidence:
                evidence_pair.append("_".join(e).rstrip("_"))
            comment_on_classification = ",".join(evidence_pair)
            df_report.iloc[
                row, df_report.columns.get_loc("Comment on classification")
            ] = comment_on_classification

    return df_report, error_msg


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


def get_col_letter(worksheet, col_name):
    '''
    Getting the column letter with specific col name
    Inputs
        worksheet (openpyxl object): current worksheet
        col_name (str): name of column
    Outputs
        col_letter (str): letter of column
    '''
    col_letter = None
    for column_cell in worksheet.iter_cols(1, worksheet.max_column):
        if column_cell[0].value == col_name:
            col_letter = column_cell[0].column_letter

    return col_letter


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
                df_interpret.loc[row, "Germline classification"].notna()
            ), "empty ACMG classification in interpret table"
            assert df_interpret.loc[row, "Germline classification"] in [
                "Pathogenic",
                "Likely Pathogenic",
                "Uncertain Significance",
                "Likely Benign",
                "Benign",
            ], "wrong ACMG classification in interpret table"
            assert (
                df_interpret.loc[row, "HGVSc"].notna()
            ), "empty HGVSc in interpret table"
            assert df_interpret.loc[row, "HGVSc"] in list(df_included["HGVSc"]), (
                "HGVSc in interpret table does not match with that in "
                "included sheet"
            )
            acgs_criteria = config.get("acgs_criteria")
            for criteria in acgs_criteria:
                if df_interpret.loc[row, criteria].notna():
                    assert (
                        df_interpret.loc[row, criteria] in strength_dropdown
                    ), f"Wrong strength in {criteria}"

            if df_interpret.loc[row, "BA1"].notna():
                assert (
                    df_interpret.loc[row, "BA1"] in BA1_dropdown
                ), "Wrong strength in BA1"

        except AssertionError as msg:
            error_msg.append(str(msg))

    error_msg = "".join(error_msg)

    return error_msg # TODO add error_msg to db rather than return it

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

    return error_msg # TODO add error_msg to db rather than return it


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
                    df.loc[row, "Germline classification"].notna()
                ), f"Wrong interpreted column in row {row+1} of included sheet"
            except AssertionError as msg:
                error_msg.append(str(msg))

        else:
            try:
                assert df.loc[row, "Interpreted"] == "no", (
                    f"Wrong interpreted column dropdown in row {row+1} "
                    "of included sheet"
                )
                assert (
                    df.loc[row, "Germline classification"].isna()
                ), f"Wrong interpreted column in row {row+1} of included sheet"
            except AssertionError as msg:
                error_msg.append(str(msg))

    error_msg = " ".join(error_msg)

    return error_msg  # TODO add error_msg to db rather than return it


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

    return error_msg  # TODO add error_msg to db rather than return it


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

    return status_response
