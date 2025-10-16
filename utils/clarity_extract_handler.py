"""
Handler for extracting workbook file paths from a Clarity extract file.
"""

import pandas as pd
import dxpy
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pandas import DataFrame
from pathlib import Path
import os
import ast


def open_files(clarity):
    """
    Open files and read in file contents to DataFrames
    Inputs:
    clarity : str
        The path to the clarity extract file.
    Outputs:
    clarity_df : pd.DataFrame
        DataFrame containing the contents of the clarity extract file.
    Raises:
        ValueError: If the clarity extract file is empty.
        Exception: If there is an error reading the clarity extract file.
    """
    try:
        clarity_df = pd.read_csv(clarity, delimiter=",")
    except pd.errors.EmptyDataError:
        raise ValueError(f"Clarity extract file is empty: {clarity}")
    except Exception as e:
        raise Exception(f"Error reading clarity extract: {e}")

    return clarity_df


def get_matching_projects(assays):
    """
    Retrieve project IDs matching specific name patterns.
    This function searches for projects with names starting with '002' and ending with either 'CEN' or 'TWE'.
    Inputs:
    assays : list
        The list of assay types to filter projects (e.g., ['CEN', 'TWE']).
    Outputs:
    matching_projects_tuple_list: list
        A list of tuples containing project IDs and their names.
        Each tuple is in the format (project_id, project_name).
    """
    query_str_for_assays = "|".join(assays)
    re_pattern = rf"^002.*_(?:{query_str_for_assays})$"
    matching_projects = list(
        dxpy.find_projects(name={"regexp": re_pattern}, describe=True)
    )
    matching_projects_tuple_list = [
        (proj["id"], proj["describe"]["name"]) for proj in matching_projects
    ]
    print(f"Found {len(matching_projects_tuple_list)} matching projects.")
    return matching_projects_tuple_list


def query_reports_for_project(project_id, sample_ids):
    """
    Query reports for a given project ID and sample IDs.
    This function retrieves file names matching specific patterns and returns a list of records.
    It handles errors for files not found or multiple matches.
    It also extracts the sample ID from the file name.

    Inputs:

    project_id : str
        The ID of the project to query.
    sample_ids : list
        List of sample IDs to search for in the project.

    Outputs:
    records: list
        A list of dictionaries containing sample ID, project ID, and file name.
    """
    records = []
    # Short-circuit if no sample IDs to search
    if not sample_ids:
        return records

    pattern = rf".*({'|'.join(sample_ids)}).*xlsx"
    try:
        matching_files = dxpy.find_data_objects(
            project=project_id,
            name=pattern,
            name_mode="regexp",
            describe={"fields": {"name": True}},
        )
        # If no files found, return empty records
        if not matching_files:
            print(f"No files found for project {project_id} with pattern {pattern}")
            return records
        for file in matching_files:
            file_name = file["describe"]["name"]
            # Robustly extract the sample_id (between first and second hyphen)
            parts = file_name.split("-")
            sample_id = parts[1] if len(parts) > 2 else None
            if sample_id is None:
                print(f"Could not parse sample_id from file name: {file_name}")
                continue

            records.append(
                {
                    "sample_id": sample_id,
                    "project_id": project_id,
                    "file_name": file_name,
                }
            )
    except Exception as e:
        print(f"Error fetching files for sample in project {project_id}: {e}")
    return records


def fetch_all_reports(df, assays, chunk_size=100, max_workers=64):
    """
    Fetch all reports for the given DataFrame of sample IDs.
    This function retrieves project IDs matching specific name patterns,
    queries reports for each project, and merges the results with the original DataFrame.
    It also handles errors for samples found in multiple projects.

    Inputs:

    df : pd.DataFrame
        DataFrame containing sample IDs to search for.
    assays : list
        The list of assay types to filter projects (e.g., ['CEN', 'TWE']).
    chunk_size : int, optional
        set number of ids to process at a time, by default 100
    max_workers : int, optional
        number of maximum workers to destribute across, by default 16

    Outputs:
    final_df : pd.DataFrame
        DataFrame containing the merged results with additional columns.
    """
    sample_ids = df["sample_id"].tolist()
    project_info = get_matching_projects(assays)
    project_dict = dict(project_info)

    # Chunk sample IDs to manage search load
    chunks = [
        sample_ids[i : i + chunk_size] for i in range(0, len(sample_ids), chunk_size)
    ]

    all_records = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for project_id in project_dict.keys():
            for chunk in chunks:
                futures.append(
                    executor.submit(query_reports_for_project, project_id, chunk)
                )

        for future in as_completed(futures):
            try:
                records = future.result()
                all_records.extend(records)
            except Exception as e:
                print(f"Error in future: {e}")

    # Add project names to records
    for record in all_records:
        record["project_name"] = project_dict.get(record["project_id"], "Unknown")

    # Create a DataFrame from the records
    records_df = pd.DataFrame(all_records)
    if records_df.empty:
        print("No records found for the given sample IDs.")
        empty_cols = {"file_name": pd.NA, "project_id": pd.NA, "project_name": pd.NA}
        return df.copy().assign(**empty_cols)
    # Merge with the original df to retain additional columns
    merged_df = pd.merge(df, records_df, on="sample_id", how="left")
    print(f"Merged DataFrame shape: {merged_df.shape}")
    print(merged_df.head())

    # Identify samples with multiple projects
    project_counts = merged_df.groupby("sample_id")["project_id"].nunique()
    multiple_projects = project_counts[project_counts > 1].index.tolist()
    print(f"Samples with multiple projects: {multiple_projects}")
    # Log errors for samples with multiple projects
    for sample in multiple_projects:
        print(f"Error: Sample {sample} found in multiple projects.")

    # Filter out samples with multiple projects
    final_df = merged_df[~merged_df["sample_id"].isin(multiple_projects)]

    return final_df


def create_path(filename, base_path, assay, run):
    """
    Create a path to a specific file on clingen using pathlib.

    Inputs:
        assay (str): either CEN or WES/TWE
        base_path (str or Path): base path to clingen folder
        filename (str): filename for the xlsx report
        run (str): sequencing run name

    Outputs:
        path (Path or None): path to the given filename on clingen
    """
    # Handle NaN values
    if pd.isna(run) or run == "nan":
        return None

    # Ensure run is a string
    run = str(run)

    # Ensure base_path is a Path object
    base_path = Path(base_path)

    # extract run_name from run string
    run_name_match = re.search(
        r"(002_)(\d{6}_[A-Z0-9]+_\d{4}_[A-Z0-9]+)(_\d{2})?_[A-Za-z0-9]+", run
    )
    # group 1 = 002_
    # group 2 = DDMMYY_SEQUENCER_TESTCODE_FLOWCELL
    # group 3 = genome build
    # group 4 = Assay
    run_name = run_name_match.group(2) if run_name_match else None

    # Return None if run_name extraction failed
    if run_name is None:
        print(
            f"Warning: Could not extract run_name from '{run}' for filename {filename}. Returning None."
        )
        return None

    # Build path depending on assay
    if assay == "CEN":
        # Build run_folder name i.e. 002_240923_AH7K2WDMXY_0001_CEN
        run_folder = f"{run_name}_{assay}"
        path = base_path / assay / "Run folders" / run_folder / filename
    elif assay in ("WES", "TWE"):
        # Build run_folder name i.e. 002_240923_AH7K2WDMXY_0001_CEN
        run_folder = f"{run_name}_TWE"
        path = base_path / "WES" / run_folder / filename
    else:
        print(
            f"Warning: Unknown assay '{assay}' for filename {filename}. Returning None."
        )
        return None
    return path


def find_file_name(search_query):
    """
    Find the file name for a given search query on DNAnexus
    Inputs:

        search_query (str): search query for file name when searching DNAnexus
    Outputs:
        filename (str): a file name, or None if no or multiple matches found
    """
    files = list(
        dxpy.find_data_objects(
            name=search_query, name_mode="glob", describe={"fields": {"name": True}}
        )
    )
    filenames = [file.get("describe").get("name") for file in files]
    if len(filenames) != 1:
        print(f"{search_query} returned multiple/no files")
        return None
    else:
        return filenames[0]


def is_report_code_in_list(row) -> bool:
    """
    Check if report_r_code is in R_codes list

    Inputs:
        row (pd.Series): a row from the DataFrame containing 'R_codes' and 'report_r_code'
    Outputs:
        bool: True if report_r_code is in R_codes, False otherwise
    """
    r_codes = row.get("R_codes", [])
    report_code = row.get("report_r_code")

    # Check if report_code is missing
    if pd.isna(report_code):
        return False

    # Handle different types of r_codes input
    if pd.isna(r_codes) or r_codes == [] or r_codes == "[]":
        return False

    # Convert string representation of list to actual list
    if isinstance(r_codes, str):
        try:
            # Use ast.literal_eval instead of json.loads for Python literal evaluation
            r_codes_list = ast.literal_eval(r_codes)
        except (ValueError, SyntaxError):
            # If parsing fails, assume it's not a valid list
            return False
    elif isinstance(r_codes, list):
        r_codes_list = r_codes
    else:
        raise ValueError(f"Unexpected type for R_codes: {type(r_codes)}")

    # Handle empty list
    if not r_codes_list:
        return False

    # Normalize and compare
    target = re.sub(r"\.\d+", "", str(report_code))
    normalized_codes = [
        re.sub(r"\.\d+", "", str(c)) for c in r_codes_list
    ]

    return target in normalized_codes


def extract_assay_from_filename(filename):
    """
    Extract the assay type from the given filename.

    Parameters
    ----------
    filename : str
        The name of the file from which to extract the assay type.

    Returns
    -------
    str or None
        The extracted assay type (CEN or WES) or None if not found.
    """
    if pd.isna(filename):
        return None
    match = re.search(r"(CEN|WES)", filename)
    return match.group(1) if match else None


def filtering_reports(report_df):
    """
    Filter out reports based on if filename doesn't match the verified R codes
    and if filename contains _CNV_ or _mosaic_

    Inputs:
        report_df (pd.DataFrame):
            DataFrame containing report information with a 'file_name' column
            and 'R_codes' column.

    Outputs:
        filtered_df (pd.DataFrame): Filtered DataFrame excluding rows with '_CNV_' or '_mosaic_' in 'file_name'.
    """
    if report_df.empty or report_df is None:
        print("Report DataFrame is empty or None. Skipping filtering.")
        return report_df

    report_df["rcode_match"] = report_df.apply(is_report_code_in_list, axis=1)
    # Filter rows where R code doesn't match
    report_df = report_df[report_df["rcode_match"]].copy()
    # clean up by dropping the rcode_match column
    report_df = report_df.drop(columns=["rcode_match"])
    # Filter out rows where filename contains _CNV_ or _mosaic_
    filtered_df = report_df[
        ~report_df["file_name"].str.contains(r"_(CNV|mosaic)_", na=False)
    ]

    return filtered_df


def remove_missing_and_multiple_reports(report_df):
    """
    Remove reports with missing or multiple files.

    Inputs:
        report_df (pd.DataFrame): DataFrame containing report information with a 'file_name' column.

    Outputs:
        report_df_filtered (pd.DataFrame): Filtered DataFrame excluding rows with missing or multiple files.
        missing_data_df (pd.DataFrame): DataFrame containing rows with missing data in 'file_name' or 'R_codes'.
        multiple_reports_df (pd.DataFrame): DataFrame containing rows with multiple reports per assay.
    """

    missing_data = report_df["file_name"].isna() | report_df["R_codes"].isna()
    missing_data_df = report_df[missing_data]
    if missing_data.any():
        print(
            f"Warning: {missing_data.sum()} rows have missing data in 'file_name' or 'R_codes'."
        )

    # Mask to filter out rows with NaN R codes and empty file names which aren't '' just blank
    report_df = report_df.dropna(subset=["file_name", "report_r_code"]).copy()
    print(
        f"After filtering out NaN R codes and empty file names: {report_df.shape[0]} rows remaining"
    )

    # Drop duplicates on all columns except certain ones
    cols_to_check = [col for col in report_df.columns if col not in ["R_codes"]]
    report_df = report_df.drop_duplicates(subset=cols_to_check).copy()
    print(f"After dropping duplicates: {report_df.shape[0]} rows remaining")

    # Save rows with multiple reports per assay to a separate file
    multiple_reports = (
        report_df.groupby(["sample_id", "Assay", "report_r_code"])
        .size()
        .reset_index(name="report_count")
    ).copy()
    multiple_reports = multiple_reports[multiple_reports["report_count"] > 1]
    multiple_reports_df = pd.DataFrame()
    if not multiple_reports.empty:
        print(f"Samples with multiple reports per assay: {multiple_reports.shape[0]}")
        # Filter the original report_df to keep only samples with single reports
        multiple_reports_df = pd.merge(
            report_df,
            multiple_reports[["sample_id", "Assay", "report_r_code"]],
            on=["sample_id", "Assay", "report_r_code"],
            how="inner",
        ).copy()
    else:
        print("No samples with multiple reports per assay found.")

    # Remove rows with multiple reports per assay
    report_df_grouped = (
        report_df.groupby(["sample_id", "Assay", "report_r_code"])
        .size()
        .reset_index(name="report_count")
    ).copy()
    single_reports_df = report_df_grouped[report_df_grouped["report_count"] == 1]
    # Filter the original report_df to keep only samples with single reports
    report_df_filtered = pd.merge(
        report_df,
        single_reports_df[["sample_id", "Assay", "report_r_code"]],
        on=["sample_id", "Assay", "report_r_code"],
        how="inner",
    ).copy()
    return report_df_filtered, missing_data_df, multiple_reports_df


def preprocess_report_df(report_df, base_path):
    """
    Preprocess the report DataFrame by adding necessary columns and cleaning data.

    Inputs:
        report_df (pd.DataFrame): DataFrame containing report information with a 'file_name' column
            and 'R_codes' column.
        base_path (str or Path): Base path to the clingen folder.
    Outputs:
        report_df (pd.DataFrame): Preprocessed DataFrame with additional columns.
    """
    # Split R codes into a list
    report_df["R_codes"] = (
        report_df["Test Directory Test Code"].fillna("").str.split("|")
    )

    # remove decimal points from R codes
    report_df["R_codes"] = report_df["R_codes"].apply(
        lambda x: [
            re.sub(r"\.\d+", "", code)
            for code in x
            if isinstance(code, str) and code.startswith("R")
        ]
    )

    # Extract assay from filename
    report_df["Assay"] = report_df["file_name"].apply(extract_assay_from_filename)

    # Add the path to the processed reports
    report_df["path"] = report_df.apply(
        lambda x: create_path(x["file_name"], base_path, x["Assay"], x["project_name"]),
        axis=1,
    )

    # Add specimen ID to the processed report_df
    report_df["instrument_id"] = report_df["file_name"].str.split("-").str[0]
    report_df["full_sample_id"] = (
        report_df["instrument_id"] + "-" + report_df["sample_id"]
    )
    # create report_r_code column from file_name
    report_df["report_r_code"] = report_df["file_name"].str.extract(r"_(R\d+\.\d+)_")[0]

    return report_df


def handle_clarity_extract(
    clarity_extract_path, assays, base_path
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Main function to handle clarity extract and return paths to workbooks
    Inputs:

        clarity_extract_path (str): path to clarity extract file
        assays (list): list of assays to process, e.g. ['CEN', 'TWE']
        base_path (str): base path to clingen folder
    Outputs:
        report_df_filtered (pd.DataFrame): DataFrame of workbooks to process
        missing_data_df (pd.DataFrame): DataFrame of workbooks with missing data
        multiple_reports_df (pd.DataFrame): DataFrame of workbooks
            with multiple workbooks per assay
    """
    # Read data into dataframes
    clarity_df = open_files(clarity_extract_path)

    # Process data to construct a path for each specimen
    # create df with column by splitting the Beaker Procedure Name to create a new column for assay
    clarity_df["sample_id"] = clarity_df["Specimen Identifier"].str.split("-").str[1]
    report_df = pd.DataFrame()

    print(f"Processing assays: {assays}")
    report_df = fetch_all_reports(clarity_df, assays)

    # Add check for empty DataFrame
    if report_df.empty:
        print(
            "No reports found after fetching from DNAnexus. Returning empty DataFrames."
        )
        empty_cols = {
            "file_name": pd.NA,
            "project_id": pd.NA,
            "project_name": pd.NA,
            "R_codes": pd.NA,
            "Assay": pd.NA,
            "path": pd.NA,
            "instrument_id": pd.NA,
            "full_sample_id": pd.NA,
            "report_r_code": pd.NA,
        }
        empty_df = pd.DataFrame(
            columns=list(clarity_df.columns) + list(empty_cols.keys())
        )
        return empty_df, empty_df.copy(), empty_df.copy()

    print(f"Total reports fetched: {report_df.shape[0]}")

    # Preprocess report_df
    report_df = preprocess_report_df(report_df, base_path)
    print(f"Report DataFrame after preprocessing: {report_df.shape[0]} rows")
    # Filter out all rows where filename contains _CNV_ or _mosaic_
    report_df = filtering_reports(report_df)

    # Remove missing and multiple reports
    report_df_filtered, missing_data_df, multiple_reports_df = (
        remove_missing_and_multiple_reports(report_df)
    )

    return report_df_filtered, missing_data_df, multiple_reports_df
