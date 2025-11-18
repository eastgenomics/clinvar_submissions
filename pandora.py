"""
Script to add variants from workbooks to Shire database and submit variants
from Shire database to ClinVar
"""

import json
import argparse
import os.path
import glob
import utils.utils as utils
import utils.clinvar as clinvar
import utils.database_actions as db
import utils.clarity_extract_handler as clarity_handler
import warnings
from openpyxl import load_workbook
import pandas as pd
import re
from sqlalchemy import create_engine
from datetime import datetime as dt
from pathlib import Path


def open_json(file: str) -> dict:
    """
    Inputs:
        file (str): path to json file
    Outputs:
        contents (dict): the contents of that JSON as a dict
    """
    with open(file, encoding="utf8") as f:
        contents = json.load(f)
    return contents


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description="", formatter_class=(argparse.ArgumentDefaultsHelpFormatter)
    )
    parser.add_argument(
        "--clinvar_api_key",
        required=True,
        help="JSON containing CUH and NUH ClinVar API keys",
    )
    parser.add_argument(
        "--clinvar_testing",
        action="store_true",
        help="Boolean determining whether to use the ClinVar test endpoint",
    )
    parser.add_argument(
        "--print_submission_json",
        action="store_true",
        help="Boolean determining whether to print ClinVar submission JSONs",
    )
    parser.add_argument(
        "--hold_for_review",
        action="store_true",
        help="Boolean determining whether to hold submission of variants, "
        "allowing for manual review in the db before submission.",
    )
    parser.add_argument(
        "--db_credentials",
        required=True,
        help="JSON containing credentials to connect to AWS database",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--path_to_workbooks", help="Path to variant workbooks")
    group.add_argument(
        "--clarity_extract", help="Path to file containing clarity extract"
    )
    group.add_argument(
        "--samples_file",
        help="Path to file containing Excel paths to workbooks in clingen",
    )
    parser.add_argument(
        "--config", required=True, help="JSON config file containing required inputs"
    )
    parser.add_argument(
        "--organisation",
        choices=["CUH", "NUH"],
        required=True,
        help="Organisation: CUH or NUH",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Run the script without making any changes "
        "to the database or submitting to ClinVar",
    )
    parser.add_argument(
        "--no_retry", action="store_true", help="Do not retry failed submissions"
    )
    parser.add_argument(
        "--use_paths",
        action="store_true",
        help="Use paths from clarity extract directly",
    )
    parser.add_argument(
        "--output_dir",
        type=validate_output_dir,
        help="Directory to output any inconsistent files to",
        required=True,
    )
    args = parser.parse_args()
    return args


def validate_output_dir(path_str: str) -> Path:
    """
    Validate output directory exists or create it
    Inputs:
        output_dir (str): path to output directory
    Outputs:
        output_dir (str): validated path to output directory
    Side effects:
        Creates output directory if it does not exist
    """

    path = Path(path_str).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"{path} is not a valid directory")
    return path


def output_inconsistent_files(
    data_issues_df, clarity_issues_df, timestamp=None, output_dir=None
):
    """
    Output any inconsistent files from clarity extract handling for review
    Inputs:
        data_issues_df (pd.DataFrame): dataframe of samples with data issues
        (i.e. no DX data, multiple reports in DX, file does not exist in path)
        clarity_issues_df (pd.DataFrame): dataframe of samples with clarity issues
        timestamp (str): timestamp to append to filenames
        output_dir (str): directory to output inconsistent files to
    Outputs:
        None
    Side effects:
        Outputs CSV files if any inconsistent data found
    """
    if timestamp is None:
        print("Timestamp not provided. Not outputting inconsistent files.")
        return

    # Output any inconsistent files for review
    if not data_issues_df.empty:
        print(
            f"{data_issues_df.shape[0]} samples with data issues found in "
            f"clarity extract. See data_issues_{timestamp}.csv for details."
        )
        path_to_missing = os.path.join(
            output_dir, f"data_issues_{timestamp}.csv"
        )
        data_issues_df.to_csv(path_to_missing, index=False)

    if not clarity_issues_df.empty:
        print(
            f"{clarity_issues_df.shape[0]} samples with issues found in "
            f"input clarity extract. See clarity_issues_clarity_extract_{timestamp}.csv for details."
        )
        path_to_clarity_issues = os.path.join(
            output_dir, f"clarity_issues_clarity_extract_{timestamp}.csv"
        )
        clarity_issues_df.to_csv(path_to_clarity_issues, index=False)
    if data_issues_df.empty and clarity_issues_df.empty:
        print("No inconsistent data found.")


def main():
    """
    Script entry point
    """
    args = parse_args()
    # Get current date and time for later use in filenames
    now = dt.now()
    # Format it safely for filenames (e.g. 2025-10-07_14-32-10)
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    # Read files
    config = open_json(args.config)
    api_keys = open_json(args.clinvar_api_key)
    db_creds = open_json(args.db_credentials)

    # Set up API headers and select API url
    cuh_api_key = api_keys["cuh"]
    nuh_api_key = api_keys["nuh"]

    cuh_header = clinvar.create_header(cuh_api_key)
    nuh_header = clinvar.create_header(nuh_api_key)
    api_url = utils.select_api_url(args.clinvar_testing, config)

    # Create SQLAlchemy engine to connect to AWS database
    url = (
        "postgresql+psycopg2://"
        f"{db_creds['user']}:{db_creds['pwd']}@{db_creds['endpoint']}/ngtd"
    )

    engine = create_engine(url)

    # Ignore UserWarnings from setting dataframe attributes
    warnings.simplefilter(action="ignore", category=UserWarning)

    if args.dry_run:
        print(
            "Dry run specified. No need to query DB and clinvar API. "
            "No changes will be submitted to the database or ClinVar."
        )
    else:
        # Identify cases in database which have a submission ID but no accession ID
        print("Searching for variants with no accession ID...")
        cuh_submission_df = db.select_variants_from_db("288359", engine, "NOT NULL")
        nuh_submission_df = db.select_variants_from_db("509428", engine, "NOT NULL")

        print(
            f"Found {nuh_submission_df.shape[0]} with submission IDs but no "
            f"accession IDs for NUH.\nFound {cuh_submission_df.shape[0]} "
            "with submission IDs but no accession IDs for CUH."
        )

        cuh_submission_df.header = cuh_header
        nuh_submission_df.header = nuh_header

        # If any exist, query clinvar API to retrieve accession IDs
        for df in [cuh_submission_df, nuh_submission_df]:
            if not df.empty:
                for submission_id in list(df["submission_id"].unique()):
                    status, response = utils.submission_status_check(
                        submission_id, df.header, api_url
                    )
                    accession_ids, errors = clinvar.process_submission_status(
                        status, response
                    )

                    if accession_ids != {}:
                        db.add_accession_ids_to_db(accession_ids, engine)

                    if errors != {}:
                        db.add_clinvar_submission_error_to_db(errors, engine)

    # Gather workbooks to process
    workbooks_to_process = []
    # Get config values
    if args.path_to_workbooks:
        print(f"Searching {args.path_to_workbooks}...")
        filenames = glob.glob(os.path.join(args.path_to_workbooks, "*.xlsx"))
        # remove any CNV workbooks
        workbooks_to_process = [
            f for f in filenames if not re.search(
                r"(CNV|mosaic)", f, re.IGNORECASE
            )
        ]
        if not filenames:
            print("No workbooks found in the specified path.")
            raise SystemExit(1)
        print(f"Found {len(workbooks_to_process)} workbooks")
    elif args.samples_file:
        print(f"Reading samples from {args.samples_file}...")
        samples_df = pd.read_csv(f"{args.samples_file}")
        # Check files exist and exclude any that don't
        samples_df, missing_data_df = utils.check_files_exist_and_exclude(
            samples_df, "path"
        )
        # remove any CNV workbooks and mosaic workbooks
        workbooks_to_process = samples_df[
            ~samples_df["file_name"].str.contains("CNV|mosaic", case=False, na=False)
        ]["path"].tolist()
        print(f"Found {len(workbooks_to_process)} workbooks")
        # Output any inconsistent files for review
        output_inconsistent_files(
            missing_data_df, pd.DataFrame(), timestamp, args.output_dir
        )
    elif args.clarity_extract:
        print("Authenticating DNAnexus...")
        dnanexus_api_key = api_keys["dnanexus"]
        utils.dx_login(dnanexus_api_key)
        print(f"Reading clarity extract from {args.clarity_extract}...")
        rd_assays = config.get("rare_disease_assays", [])
        base_path = config.get("base_path", "")
        if rd_assays == [] or base_path == "":
            print(
                "RD Assays and base_path must be specified in the config file "
                "when using clarity extract."
            )
            raise SystemExit(1)

        clarity_df, missing_data_df, clarity_issues_df = (
            clarity_handler.handle_clarity_extract(
                args.clarity_extract, rd_assays, base_path
            )
        )
        if clarity_df.empty:
            print(
                "Report dataframe generated from Clarity extract is empty "
                "after preprocessing. Skipping processing."
            )
            output_inconsistent_files(
                missing_data_df, clarity_issues_df, timestamp, args.output_dir
            )
        else:
            if args.use_paths:
                print("Using paths from clarity extract directly.")
                # Check files exist and exclude any that don't
                clarity_df, missing_file_paths_df = utils.check_files_exist_and_exclude(
                    clarity_df, "path"
                )
                if not missing_file_paths_df.empty:
                    missing_data_df = pd.concat(
                        [missing_data_df, missing_file_paths_df],
                        ignore_index=True
                    )

                workbooks_to_process = clarity_df["path"].dropna().tolist()
                print(f"Found {len(workbooks_to_process)} workbooks")

                # Output any inconsistent files for review
                output_inconsistent_files(
                    missing_data_df, clarity_issues_df, timestamp, args.output_dir
                )
            else:
                print(
                    "--use_paths not specified so outputting clarity extract dataframes for review."
                )
                print("These can be processed by using --samples_file option.")
                # Output samples file for review named with timestamp
                path_to_parsed_clarity = os.path.join(
                    args.output_dir, f"clarity_extract_parsed_paths_{timestamp}.csv"
                )
                clarity_df.to_csv(
                    path_to_parsed_clarity,
                    index=False,
                )
                print(
                    f"Clarity extract parsed paths output to {path_to_parsed_clarity}"
                )
                output_inconsistent_files(
                    missing_data_df, clarity_issues_df, timestamp, args.output_dir
                )

    # Get previously parsed workbooks
    parsed_workbook_df = db.select_workbooks_from_db(engine, "parse_status = TRUE")
    parsed_list = parsed_workbook_df["workbook_name"].values
    failed_parsing_df = db.select_workbooks_from_db(engine, "parse_status = FALSE")
    failed_list = failed_parsing_df["workbook_name"].values

    # Process workbooks
    for filename in workbooks_to_process:
        print(f"Processing {filename}")
        # check if wb has not already been processed
        file = os.path.basename(filename)
        if file not in parsed_list:
            print(
                f"{file} has not previously been parsed successfully.\n"
                f"Parsing {file}..."
            )
            workbook = load_workbook(filename)
            if file not in failed_list:
                # Was "NULL" but None in SQLAlchemy becomes NULL in SQL
                db.add_wb_to_db(file, None, engine)
            # Get a df of data from each sheet in workbook:
            df = utils.get_workbook_data(
                workbook, config, filename, file, engine, args.organisation
            )
            if df is None:
                print("No data parsed from workbook.")
                continue
            # If we reach this point, we have valid data
            if args.dry_run:
                print(f"Parsed data:\n{df.head()}\n{df.shape[0]} rows in total.")
                df = None
            else:
                if not df.empty:
                    print(f"{df.shape[0]} variants to add to inca table.")
                    db.add_variants_to_db(df, engine)
                db.update_db_for_parsed_wb(file, engine)
        else:
            print(f"{file} has already been parsed. Skipping...")

    # Select all variants that have interpreted = yes and are not submitted
    # Also exclude any variants meeting exclusion criteria set in the config
    if not args.hold_for_review:
        exclude = config["exclude"]
        # Add DUP to the accession_id field for duplicates to exclude them
        # from submission

        if not args.dry_run:
            print(
            "Checking for duplicate variants... and setting accession_id to 'DUP'"
            )
            db.set_DUP_for_germline_duplicates(engine)
        else:
            print("Dry run specified. No changes will be made to the database.")

        cuh_df = db.select_variants_from_db("288359", engine, "NULL", exclude)
        nuh_df = db.select_variants_from_db("509428", engine, "NULL", exclude)
        print(
            f"Found {nuh_df.shape[0]} interpreted variants to submit for NUH.\n"
            f"Found {cuh_df.shape[0]} interpreted variants to submit for CUH."
        )
        cuh_df.url, cuh_df.header = config.get("CUH_acgs_url"), cuh_header
        nuh_df.url, nuh_df.header = config.get("NUH_acgs_url"), nuh_header

        # Get clinvar information from each variant and submit
        for df in [cuh_df, nuh_df]:
            if not df.empty:
                variants = clinvar.collect_clinvar_data_to_submit(
                    df, config["ref_genomes"]
                )
                response = clinvar.clinvar_api_request(
                    api_url,
                    df.header,
                    variants,
                    df.url,
                    args.print_submission_json,
                    args.no_retry,
                )
                print(f"Submission response: {response.status_code}, {response.text}")
                if args.clinvar_testing is False:
                    db.add_submission_id_to_db(
                        response.json(), engine, df["local_id"].tolist()
                    )
    else:
        print("hold_for_review specified. Variants will not be submitted.")


if __name__ == "__main__":
    main()
