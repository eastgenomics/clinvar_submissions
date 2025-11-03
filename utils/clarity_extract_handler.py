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
    except pd.errors.EmptyDataError as e:
        raise RuntimeError(f"Clarity extract file is empty: {clarity}") from e
    except (FileNotFoundError, PermissionError, pd.errors.ParserError) as e:
        raise RuntimeError(f"Error reading clarity extract: {clarity}") from e
    except Exception as e:
        raise RuntimeError(f"Error reading clarity extract: {e}") from e

    return clarity_df


def preprocess_clarity_extract(clarity_df):
    """
    Preprocess the clarity extract DataFrame by adding necessary columns and cleaning data.

    Inputs:
        clarity_df (pd.DataFrame): DataFrame containing clarity extract information.
    Outputs:
        clarity_df (pd.DataFrame): Preprocessed DataFrame with additional
        sample_id and R_codes columns.
    """
    # Split out sample ID to remove "SP-" prefix
    clarity_df["sample_id"] = (
        clarity_df["Specimen Identifier"].str.split("-").str[1]
    )

    # Split unique base R codes into a list
    clarity_df["R_codes"] = (
        clarity_df["Test Directory Test Code"]
        .fillna("")
        .str.split("|")
        .apply(
            lambda codes: sorted(
                {re.sub(r"\.\d+$", "", c.strip()) for c in codes if c}
            )
        )
    )

    missing_r_codes_df = clarity_df[clarity_df["R_codes"].str.len() == 0].copy()
    multiple_r_codes_df = clarity_df[clarity_df["R_codes"].str.len() > 1].copy()

    if not missing_r_codes_df.empty:
        print("Samples with missing R codes in Clarity extract:")
        print(missing_r_codes_df[["Specimen Identifier", "R_codes"]])

    if not multiple_r_codes_df.empty:
        print("Samples with multiple R codes in Clarity extract:")
        print(multiple_r_codes_df[["Specimen Identifier", "R_codes"]])

    # Keep only samples with one R code
    clarity_df = clarity_df[clarity_df["R_codes"].str.len() == 1]

    return clarity_df


def get_matching_projects(assays):
    """
    Retrieve project IDs matching specific name patterns.
    This function searches for projects with names starting with '002' and ending with either 'CEN' or 'TWE'.
    Inputs:
    assays : list
        The list of assay types to filter projects (e.g., ['CEN', 'TWE']).
    Outputs:
    matching_projects_dict: dict
        A dictionary mapping project IDs to project names.
    """
    query_str_for_assays = "|".join(assays)
    re_pattern = rf"^002.*_(?:{query_str_for_assays})$"

    matching_projects = list(
        dxpy.find_projects(
            name={"regexp": re_pattern},
            describe={"fields": {"name": True}}
        )
    )
    matching_projects_dict = {
        proj["id"]: proj["describe"]["name"] for proj in matching_projects
    }
    print(f"Found {len(matching_projects_dict.keys())} matching projects.")

    return matching_projects_dict


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


def fetch_all_reports(df, assays, chunk_size=100, max_workers=16):
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
    project_dict = get_matching_projects(assays)

    # Chunk sample IDs to manage search load
    chunks = [
        sample_ids[i : i + chunk_size] for i in range(0, len(sample_ids), chunk_size)
    ]

    tasks = [
        (proj_id, chunk) for proj_id in project_dict.keys() for chunk in chunks
    ]

    all_records = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(query_reports_for_project, pid, chunk): (
                pid,
                chunk,
            )
            for pid, chunk in tasks
        }

        for future in as_completed(futures):
            pid, chunk = futures[future]
            try:
                result = future.result()
                if result:
                    all_records.extend(result)
            except Exception as e:
                print(f"Error fetching reports for project {pid}: {e}")

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
        print(f"Warning: Could not extract run_name from '{run}' for filename {filename}. Returning None.")
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

    if pd.isna(report_code) or r_codes is None:
        return False

    # Convert string representation like "['R337']" to actual list (if reading
    # from file)
    if isinstance(r_codes, str):
        try:
            r_codes = ast.literal_eval(r_codes)
        except Exception:
            return False

    if not isinstance(r_codes, (list, tuple)) or len(r_codes) == 0:
        return False

    # Normalize and compare
    target = re.sub(r"\.\d+$", "", str(report_code)).strip()
    normalized_codes = [
        re.sub(r"\.\d+$", "", str(c)).strip() for c in r_codes if c
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
    if filename is None or pd.isna(filename):
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
    if report_df is None or report_df.empty:
        print("Report DataFrame is empty or None. Skipping filtering.")
        return (
            pd.DataFrame(columns=["file_name", "sample_id", "report_r_code", "R_codes"]),
            report_df,
        )

    # Remove rows with no DX data (as they would be silently removed by
    # the R code matching below)
    missing_mask = (
        report_df["file_name"].isna() |
        report_df["report_r_code"].isna()
    )
    missing_data_df = report_df[missing_mask].copy()
    if not missing_data_df.empty:
        print(f"{missing_data_df.shape[0]} rows have missing DNAnexus data.")

    # Check R code matches
    report_df = report_df[~missing_mask].copy()

    if not report_df.empty:
        report_df["rcode_match"] = report_df.apply(is_report_code_in_list, axis=1)
        # Filter rows where R code doesn't match
        report_df = report_df[report_df["rcode_match"]].copy()
        # clean up by dropping the rcode_match column
        report_df = report_df.drop(columns=["rcode_match"], errors="ignore")
        # Filter out rows where filename contains _CNV_ or _mosaic_
        filtered_df = report_df[
            ~report_df["file_name"].str.contains(r"_(CNV|mosaic)_", na=False)
        ].copy()
    else:
        filtered_df = pd.DataFrame(columns=report_df.columns)

    # If we've filtered all rows out, ensure we return empty df with expected
    # columns
    if filtered_df.empty:
        filtered_df = pd.DataFrame(columns=["file_name", "sample_id", "report_r_code", "R_codes"])

    return filtered_df, missing_data_df


def remove_multiple_reports(report_df):
    """
    Remove reports with missing or multiple files.

    Inputs:
        report_df (pd.DataFrame): DataFrame containing report information with a 'file_name' column.

    Outputs:
        report_df_filtered (pd.DataFrame): Filtered DataFrame excluding rows with multiple files.
        multiple_reports_df (pd.DataFrame): DataFrame containing rows with multiple reports per assay.
    """
    # Drop duplicates
    cols_to_check = [c for c in report_df.columns if c != "R_codes"]
    report_df = report_df.drop_duplicates(subset=cols_to_check).copy()
    print(f"After dropping duplicates: {report_df.shape[0]} rows remaining")


    # Save rows with multiple reports per assay to a separate file
    counts = (
        report_df.groupby(["sample_id", "Assay", "report_r_code"])
        .size()
        .reset_index(name="report_count")
    )

    multiple_reports = counts.query("report_count > 1")
    single_reports = counts.query("report_count == 1")

    if not multiple_reports.empty:
        print(
            "Samples with multiple reports per assay:"
            f" {multiple_reports.shape[0]}"
        )
        multiple_reports_df = (
            report_df.merge(
                multiple_reports[["sample_id", "Assay", "report_r_code"]],
                on=["sample_id", "Assay", "report_r_code"],
                how="inner"
            )
        )
    else:
        print("No samples with multiple reports per assay found.")
        multiple_reports_df = pd.DataFrame(columns=report_df.columns)

    # Filter the original report_df to keep only samples with single reports
    report_df_filtered = report_df.merge(
        single_reports[["sample_id", "Assay", "report_r_code"]],
        on=["sample_id", "Assay", "report_r_code"],
        how="inner"
    )

    return report_df_filtered, multiple_reports_df


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
    clarity_df_preprocessed = preprocess_clarity_extract(clarity_df)

    if clarity_df_preprocessed.empty:
        print(
            "No valid samples with single R codes found in Clarity extract."
            " Returning empty DataFrames."
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

    print(f"Processing assays: {assays}")
    report_df = fetch_all_reports(clarity_df_preprocessed, assays)

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
    report_df, missing_data_df = filtering_reports(report_df)
    print(
        f"Report DataFrame after filtering CNV/mosaic and R code mismatches:"
        f" {report_df.shape[0]} rows"
    )

    # Remove missing and multiple reports
    report_df_filtered, multiple_reports_df = (
        remove_multiple_reports(report_df)
    )

    return report_df_filtered, missing_data_df, multiple_reports_df
