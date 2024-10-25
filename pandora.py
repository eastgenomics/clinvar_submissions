"""
Script to add variants from workbooks to Shire database and submit variants
from Shire database to ClinVar
Version: 1.0.0
"""
import json
import argparse
import os.path
import glob
import utils.utils as utils
import utils.clinvar as clinvar
import utils.database_actions as db
import warnings
from openpyxl import load_workbook
from sqlalchemy import create_engine


def open_json(file):
    '''
    Inputs
        file (str): path to json file
    Outputs
        contents (dict): the contents of that JSON as a dict
    '''
    with open(file) as f:
        contents = json.load(f)
    return contents


def parse_args():
    '''
    Parse command line arguments
    '''
    parser = argparse.ArgumentParser(
        description="",
        formatter_class=(
            argparse.ArgumentDefaultsHelpFormatter
        )
    )
    parser.add_argument(
        '--clinvar_api_key', required=True,
        help='JSON containing CUH and NUH ClinVar API keys'
        )
    parser.add_argument(
        '--clinvar_testing', action='store_true',
        help='Boolean determining whether to use the ClinVar test endpoint'
        )
    parser.add_argument(
        '--print_submission_json', action='store_true',
        help='Boolean determining whether to print ClinVar submission JSONs'
        )
    parser.add_argument(
        '--hold_for_review', action='store_true',
        help='Boolean determining whether to hold submission of variants, '
        'allowing for manual review in the db before submission.'
        )
    parser.add_argument(
        '--db_credentials', required=True,
        help='JSON containing credentials to connect to AWS database'
        )
    parser.add_argument(
        '--path_to_workbooks', help='Path to variant workbooks'
        )
    parser.add_argument(
        '--config', required=True,
        help='JSON config file containing required inputs'
        )
    args = parser.parse_args()
    return args


def main():
    '''
    Script entry point
    '''
    args = parse_args()

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
    warnings.simplefilter(action='ignore', category=UserWarning)

    # Identify cases in database which have a submission ID but no accession ID
    print("Searching for variants will no accession ID...")
    cuh_submission_df = db.select_variants_from_db(288359, engine, "NOT NULL")
    nuh_submission_df = db.select_variants_from_db(509428, engine, "NOT NULL")

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
                    db.add_clinvar_submission_error_to_db(
                        errors, engine.connect()
                    )

    # Get any new workbooks and re-run any failed workbooks in given path
    if args.path_to_workbooks:
        print(f"Searching {args.path_to_workbooks}...")
        filenames = glob.glob(args.path_to_workbooks + "*.xlsx")
        print(f"Found {len(filenames)} workbooks")

        # Get previously parsed workbooks
        parsed_workbook_df = db.select_workbooks_from_db(
            engine, "parse_status = TRUE"
        )
        parsed_list = parsed_workbook_df['workbook_name'].values
        failed_parsing_df = db.select_workbooks_from_db(
            engine, "parse_status = FALSE"
        )
        failed_list = failed_parsing_df['workbook_name'].values

        for filename in filenames:
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
                    db.add_wb_to_db(file, "NULL", engine.connect())

                # Get a df of data from each sheet in workbook:
                df = utils.get_workbook_data(
                    workbook, config, filename, file, engine.connect()
                )
                if df is not None:
                    if not df.empty:
                        print(f"{df.shape[0]} variants to add to inca table.")
                        db.add_variants_to_db(df, engine.connect())
                    db.update_db_for_parsed_wb(file, engine.connect())
            else:
                print(f"{file} has already been parsed. Skipping...")

    else:
        print("no path_to_workbooks to specified. Nothing to parse")

    # Select all variants that have interpreted = yes and are not submitted
    # Also exclude any variants meeting exclusion criteria set in the config
    if not args.hold_for_review:
        exclude = config["exclude"]
        cuh_df = db.select_variants_from_db(288359, engine, "NULL", exclude)
        nuh_df = db.select_variants_from_db(509428, engine, "NULL", exclude)
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
                    df, config['ref_genomes']
                )
                response = clinvar.clinvar_api_request(
                    api_url, df.header, variants, df.url,
                    args.print_submission_json
                )
                if args.clinvar_testing is False:
                    db.add_submission_id_to_db(
                        response.json(),
                        engine.connect(),
                        df['local_id'].values
                    )
    else:
        print("hold_for_review specified. Variants will not be submitted.")

if __name__ == "__main__":
    main()
