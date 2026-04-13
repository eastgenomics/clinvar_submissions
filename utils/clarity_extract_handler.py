"""
Handler for extracting workbook file paths from a Clarity extract file.
"""

import pandas as pd
import dxpy
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
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
    Preprocess the clarity extract DataFrame by adding necessary columns and
    cleaning data.

    Inputs:
        clarity_df (pd.DataFrame): DataFrame containing clarity extract information.
    Outputs:
        clarity_df (pd.DataFrame): Preprocessed DataFrame with additional
        sample_id and R_codes columns.
        clarity_issues_df (pd.DataFrame): DataFrame containing samples with
        missing or multiple R codes in the Clarity extract.
    """
    # Split out sample ID to remove "SP-" prefix
    clarity_df["sample_id"] = (
        clarity_df["Specimen Identifier"]
        .str.split("-")
        .apply(lambda parts: parts[1] if len(parts) > 1 else None)
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

    clarity_issues_df = clarity_df[
        (clarity_df["R_codes"].str.len() == 0) |
        (clarity_df["R_codes"].str.len() > 1)
    ].copy()

    if not clarity_issues_df.empty:
        clarity_issues_df["issue"] = (
            "Missing or multiple R codes in Clarity extract"
        )

    # Keep only samples with one R code
    clarity_df = clarity_df[clarity_df["R_codes"].str.len() == 1]

    return clarity_df, clarity_issues_df


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
    print(
        f"Found {len(matching_projects_dict.keys())} matching DNAnexus "
        f"projects for assays {assays}."
    )

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

    pattern = rf"^(?!.*(CNV|mosaic)).*({'|'.join(sample_ids)}).*\.xlsx$"
    try:
        matching_files = dxpy.find_data_objects(
            project=project_id,
            name=pattern,
            name_mode="regexp",
            describe={"fields": {"name": True}},
        )
        # If no files found, return empty records
        if not matching_files:
            print(
                f"No files found for project {project_id} with pattern "
                f"{pattern}"
            )
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
            executor.submit(query_reports_for_project, proj_id, chunk): (
                proj_id,
                chunk,
            )
            for proj_id, chunk in tasks
        }

        for future in as_completed(futures):
            proj_id, chunk = futures[future]
            try:
                result = future.result()
                if result:
                    all_records.extend(result)
            except Exception as e:
                print(f"Error fetching reports for project {proj_id}: {e}")

    if all_records:
        for record in all_records:
            record["project_name"] = project_dict.get(record["project_id"], "Unknown")

        records_df = pd.DataFrame(all_records)
        print(f"Total reports fetched: {records_df.shape[0]}")
        merged_df = pd.merge(df, records_df, on="sample_id", how="left")
    else:
        print("No DNAnexus reports found for any of the given sample IDs.")
        merged_df = df.copy()
        merged_df["file_name"] = pd.NA
        merged_df["project_id"] = pd.NA
        merged_df["project_name"] = pd.NA

    # Identify samples with multiple projects
    project_counts = merged_df.groupby("sample_id")["project_id"].nunique(
        dropna=True
    )
    multiple_projects = project_counts[project_counts > 1].index.tolist()

    if multiple_projects:
        print(f"Samples with multiple projects: {multiple_projects}")
        for sample in multiple_projects:
            print(f"Warning: Sample {sample} found in multiple projects.")
        merged_df = merged_df[~merged_df["sample_id"].isin(multiple_projects)]

    return merged_df


def create_path(filename, base_path, assay, run):
    """
    Create a path to a specific file on clingen using pathlib.

    Inputs:
        filename (str): filename for the xlsx report
        base_path (str or Path): base path to clingen folder
        assay (str): either CEN or WES/TWE
        run (str): sequencing run name

    Outputs:
        path (Path or pd.NA): path to the given filename on clingen
    """
    # Handle NaN values
    if pd.isna(run) or run == "nan":
        return pd.NA

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
            f"Warning: Could not extract run_name from '{run}' for filename "
            f"{filename}. Returning NA."
        )
        return pd.NA

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
        return pd.NA
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

    # Normalise and compare
    target = re.sub(r"\.\d+$", "", str(report_code)).strip()
    normalized_codes = [
        re.sub(r"\.\d+$", "", str(c)).strip() for c in r_codes if c
    ]

    return target in normalized_codes


def filtering_reports(report_df):
    """
    Filter out reports based on if:
    - No files found in DNAnexus for sample
    - R code in filename doesn't match the verified R code
    - Multiple reports found for a sample for the same R code

    Inputs:
        report_df (pd.DataFrame): DataFrame containing report information with
        a 'file_name' column and 'R_codes' column.

    Outputs:
        filtered_df (pd.DataFrame): Filtered DataFrame
        data_issues_df (pd.DataFrame): DataFrame containing rows with samples
        with DNAnexus data issues.
    """
    ## Remove and report rows with no DX data
    data_issues_df = pd.DataFrame(
        columns=report_df.columns.tolist() + ["issue"]
    )
    required = [
        'file_name', 'project_id', 'project_name', 'Assay', 'path',
        'instrument_id', 'full_sample_id', 'report_r_code', 'sample_id'
    ]
    missing_mask = report_df[required].isna().any(axis=1)
    if missing_mask.any():
        missing_rows = report_df[missing_mask].copy()
        missing_rows["issue"] = (
            "No DNAnexus data or cannot parse fields from DNAnexus data"
        )
        data_issues_df = pd.concat(
            [data_issues_df, missing_rows], ignore_index=True
        )

    # Keep only rows with all required data
    working_df = report_df[~missing_mask].copy()

    # If no valid rows remain, return immediately
    if working_df.empty:
        return (
            pd.DataFrame(columns=report_df.columns),
            data_issues_df
        )

    ## Remove and report rows where the R code in DX doesn't match
    working_df["rcode_match"] = working_df.apply(is_report_code_in_list, axis=1)
    valid_samples = set(working_df.loc[working_df["rcode_match"], "sample_id"])
    rcode_mismatch = working_df[
        (~working_df["rcode_match"]) &
        (~working_df["sample_id"].isin(valid_samples))
    ].copy()

    if not rcode_mismatch.empty:
        rcode_mismatch["issue"] = "R code mismatch"
        rcode_mismatch = rcode_mismatch.drop(columns=["rcode_match"], errors="ignore")
        data_issues_df = pd.concat(
            [data_issues_df, rcode_mismatch], ignore_index=True
        )

    # Keep only rows with matching R_code, if empty return
    working_df = working_df[working_df["rcode_match"]].copy()
    working_df = working_df.drop(columns=["rcode_match"], errors="ignore")
    if working_df.empty:
        return (
            pd.DataFrame(columns=report_df.columns),
            data_issues_df
        )

    # Remove rows where there's multiple SNV reports per sample for same R
    # code
    working_df["dup_count"] = working_df.groupby(
        ["Assay", "report_r_code", "sample_id"]
    )["sample_id"].transform("count")

    duplicate_groups = working_df[working_df["dup_count"] > 1]
    if not duplicate_groups.empty:
        # Keep ONE row per duplicated sample for the issues dataframe
        one_issue_row_per_sample = (
            duplicate_groups
            .sort_values(["Assay", "report_r_code", "sample_id"])
            .groupby(["Assay", "report_r_code", "sample_id"], as_index=False)
            .first()
        )
        one_issue_row_per_sample["issue"] = "Multiple reports"
        one_issue_row_per_sample = one_issue_row_per_sample.drop(
            columns=["dup_count"], errors="ignore"
        )

        data_issues_df = pd.concat(
            [data_issues_df, one_issue_row_per_sample],
            ignore_index=True
        )

    # Keep only unique rows in filtered_df
    filtered_df = working_df[working_df["dup_count"] == 1].copy()
    filtered_df = filtered_df.drop(
        columns=["dup_count"], errors="ignore"
    )

    # Ensure filtered_df has all columns even if empty
    if filtered_df.empty:
        filtered_df = pd.DataFrame(columns=report_df.columns)

    return filtered_df, data_issues_df


def preprocess_report_df(report_df, base_path):
    """
    Preprocess the report DataFrame by adding necessary columns and cleaning
    data.

    Inputs:
        report_df (pd.DataFrame): DataFrame containing report information with
        a 'file_name' column and 'R_codes' column.
        base_path (str or Path): Base path to the clingen folder.
    Outputs:
        report_df (pd.DataFrame): Preprocessed DataFrame with additional
        columns.
    """
    # Extract assay from filename
    report_df["Assay"] = (
        report_df["project_name"]
        .str.extract(r"_([^_]+)$", expand=False)
        .where(report_df["project_name"].notna(), pd.NA)
    )

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
    report_df["report_r_code"] = (
        report_df["file_name"]
        .str.extract(r"_(R\d+\.\d+)_", expand=False)
        .where(report_df["file_name"].notna(), pd.NA)
    )

    return report_df


def handle_clarity_extract(
    clarity_extract_path, assays, base_path
):
    """
    Main function to handle clarity extract and return paths to workbooks
    Inputs:
        clarity_extract_path (str): path to clarity extract file
        assays (list): list of assays to process, e.g. ['CEN', 'TWE']
        base_path (str): base path to clingen folder
    Outputs:
        report_df_filtered (pd.DataFrame): DataFrame of workbooks to process
        data_issues_df (pd.DataFrame): DataFrame of workbooks with data issues
        clarity_issues_df (pd.DataFrame): DataFrame of samples with clarity issues
    """
    # Read data into dataframes
    clarity_df = open_files(clarity_extract_path)
    clarity_df_preprocessed, clarity_issues_df = preprocess_clarity_extract(
        clarity_df
    )

    if clarity_df_preprocessed.empty:
        print(
            "No valid samples with single R codes found in Clarity extract."
            " Returning empty DataFrames."
        )
        empty_df = pd.DataFrame(
            columns=list(clarity_df.columns)
            + [
                'file_name', 'project_id', 'project_name', 'R_codes', 'Assay',
                'path', 'instrument_id', 'full_sample_id', 'report_r_code'
            ]
        )
        return clarity_df_preprocessed, empty_df.copy(), clarity_issues_df

    print(f"Processing assays: {assays}")
    report_df = fetch_all_reports(clarity_df_preprocessed, assays)

    # Add additional required columns
    report_df = preprocess_report_df(report_df, base_path)

    # Filter out all rows with no DX data, no reports matching the R code
    # or multiple reports for same R code
    filtered_df, data_issues_df = filtering_reports(report_df)
    print(
        f"Total reports remaining after filtering: {filtered_df.shape[0]}"
    )

    return filtered_df, data_issues_df, clarity_issues_df