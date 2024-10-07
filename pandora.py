"""
Script to add variants from workbooks to Shire database and submit variants
from Shire database to ClinVar
Version: 1.0.0
"""
import json
import requests
from requests.adapters import HTTPAdapter, Retry
import argparse
import os.path
import glob
from utils.utils import *
import pyodbc
import dxpy
import numpy as np
from openpyxl import load_workbook


def modify_for_R444_clinvar_submission(clinvar_dict):
    '''
    Edit clinvar_submission dict to add drug responsiveness information if this
    is a R444 case, as these are pharmacogenomic cases.
    Inputs:
        clinvar_dict (dict): dict of data to submit to clinvar for one variant
    Outputs:
        clinvar_dict (dict): dict of data to submit to clinvar for one variant,
        modified to make suitable for a pharmacogenomic variant submission
    '''
    # TODO fix this based on feedback from ClinVar
    print(clinvar_dict)
    interpretation = clinvar_dict['clinicalSignificance'][
        'clinicalSignificanceDescription'
    ]
    translate = {
        "Pathogenic": "No function",
        "Likely pathogenic": "Uncertain function",
        "Uncertain significance": "Uncertain function",
        "Likely benign": "Uncertain function",
        "Benign": "Allele function"
    }
    drug_response_details = translate.get(interpretation)
    # drug_response = translate.get(interpretation)

    # if can't be translated, stop + add error to db
    if drug_response_details == None:
        # TODO this functionality can be added once an Error column is added
        # to the db
        query = ("") # TODO fill this in with appropriate query
        return

    clinvar_dict['clinicalSignificance'][
        "clinicalSignificanceDescription"
    ] = "drug response"
    clinvar_dict['clinicalSignificance'][
        "explanationOfDrugResponse"
    ] = "Allele function"
    clinvar_dict["conditionSet"] = {
        "drugResponse": [
            {
            "condition": [{
                "name": clinvar_dict["conditionSet"]["condition"][0]["name"]
             }],
            "db": "MedGen",
            "id": "CN224080"
            }
        ]   
    }

    return clinvar_dict


def extract_clinvar_information(variant_row):
    '''
    Extract information from Shire variant record and reformat into dictionary
    Inputs:
        variant: row from variant dataframe with data for one variant
    outputs:
        clinvar_dict: dictionary of data to submit to clinvar
    '''
    if variant_row["Comment on classification"].isna():
        variant_row["Comment on classification"] = ""

    if variant_row["Ref_genome"] not in ["GRCh37.p13", "GRCh38.p13"]:
        raise ValueError("Invalid genome build")

    assembly = variant_row["Ref_genome"].split('.')[0]

    clinvar_dict = {
            'clinicalSignificance': {
                'clinicalSignificanceDescription': variant_row["Germline classification"],
                'comment': variant_row["Comment on classification"],
                'dateLastEvaluated': variant_row["Date last evaluated"]
            },
            'conditionSet': {
                'condition': [{'name': variant_row['Preferred condition name']}]
            },
            'localID': variant_row["Local ID"],
            'localKey': variant_row["Linking ID"],
            'observedIn': [{
                'affectedStatus': variant_row['Affected status'],
                'alleleOrigin': variant_row["Allele origin"],
                'collectionMethod': variant_row['Collection method']
            }],
            'recordStatus': "novel",
            'variantSet': {
                'variant': [{
                    'chromosomeCoordinates': {
                        'assembly': assembly,
                        'alternateAllele': variant_row['Alternate allele'],
                        'referenceAllele': variant_row['Reference allele'],
                        'chromosome': str(variant_row['Chromosome']),
                        'start': variant_row['Start']
                    },
                    'gene': [{
                        'symbol': variant_row["Gene symbol"]
                    }],
                }],
            },
        }

    return clinvar_dict


def collect_clinvar_data_to_submit(clinvar_df):

    variants = []
    for index, variant in clinvar_df.iterrows():
        clinvar_dict = extract_clinvar_information(variant)
        # R444 is a pharmacogenomic test, so should be submitted differently
        if variant["Rcode"] == "R444.1":
            clinvar_dict = modify_for_R444_clinvar_submission(clinvar_dict)
        variants.append(clinvar_dict)

    cuh_variants = [x for x in variants if variant["OrganisationID"] == 288359]
    nuh_variants = [x for x in variants if variant["OrganisationID"] == 509428]

    return cuh_variants, nuh_variants



def clinvar_api_request(url, header, var_list, org_guidelines_url):
    '''
    Make request to the ClinVar API endpoint specified.
    Inputs:
        url (str): API endpoint URL
        header (dict): headers for API call
        var_list (list): list of variant data for each clinvar variant
        org_guidelines_url (str): url for the ACGS guidelines. These are
        different for CUH and NUH.
    Returns:
        response: API response object
    '''
    clinvar_data = {
        "actions": [
            {
            "type": "AddData",
            "targetDb": "clinvar",
            "data": {
                "content": {
                    'clinvarSubmission': [var_list],
                    'assertionCriteria': {
                        'url': org_guidelines_url
                        }
                    }
                }
            }
        ]
    }

    print("JSON to submit:")
    print(json.dumps(clinvar_data, indent='⠀⠀'))

    s = requests.Session()
    retries = Retry(total=10, backoff_factor=0.5)
    s.mount('https://', HTTPAdapter(max_retries=retries))
    response = s.post(url, data=json.dumps(clinvar_data), headers=header)
    return response


def add_variants_to_db(df, cursor, conn):
    '''
    TODO write docstring
    '''
    # query
    for i in range(df.shape[0]):
        temp_df = df.loc[[i]]
        temp_df = temp_df[temp_df.columns[~temp_df.isnull().all()]]
        qmarks = ", ".join("?" * temp_df.shape[1])
        cols = ", ".join([f"[{x}]" for x in temp_df.columns])
        qry = f"Insert Into [Shiredata].[dbo].[INCA] ({cols}) Values ({qmarks})"
        # convert int64 to int
        item_to_insert = []
        for item in list(temp_df.iloc[0]):
            if isinstance(item, np.int64):
                item = int(item)
            item_to_insert.append(item)
        cursor.execute(qry, item_to_insert)
        conn.commit()


def add_wb_to_db(workbook, cursor, conn):
    '''
    TODO write docstring
    '''
    # TODO once workbooks table is created
    # add a record for the sample to workbooks table.
    query = ("INSERT INTO  [Shiredata].[dbo].[Workbooks]")

def add_submission_id_to_db(response, cursor, conn):
    '''
    Inputs
        Response (dict): API response
    '''
    if response.get('id'):
        sub_id = response.get('id')
        execute_query(
            cursor,
            conn,
            f"UPDATE dbo.INCA SET [Submission ID] = '{sub_id}'"
        )
    else:
        query = ()
        # TODO handle fails, should we leave this blank

def submit_to_clinvar():

def connect_to_db(args):
    '''
    Connect to the database
    Inputs:
        args (argparse.Namespace): command line arguments
    Outputs:
        cursor: pyodbc cursor connection 
    '''
    conn_str = (
        f"DSN=gemini;DRIVER={{SQL Server Native Client 11.0}};"
        f"UID={args.uid};PWD={args.password}"
    ) 
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return cursor, conn



def execute_query(cursor, conn, query):
    '''
    TODO add docstring
    '''
    cursor.execute(query)
    conn.commit()
    return cursor

def parse_args():
    parser = argparse.ArgumentParser(
        description="",
        formatter_class=(
            argparse.ArgumentDefaultsHelpFormatter
        )
    )
    # TODO add help strings
    parser.add_argument('--clinvar_api_key') # TODO delete this? find a better way of passing the API key?
    parser.add_argument('--clinvar-testing', action='store_true')
    parser.add_argument('--path_to_workbooks')
    parser.add_argument('--config')
    parser.add_argument('--uid')
    parser.add_argument('--password')
    args = parser.parse_args()
    return args

def main():
    '''
    Script entry point
    '''
    args = parse_args()

    # Read files
    with open(args.config) as f:
        config = json.load(f)
    # TODO change so this uses the dx file in 001
    # or should it !! could this be a nextflow thing !! much to think of
    with open(args.clinvar_api_key) as f:
        api_key = f.readlines()[0].strip()

    cursor, conn = connect_to_db(args)

    # Get workbooks
    print(f"Searching {args.path_to_workbooks}...")
    filenames = glob.glob(args.path_to_workbooks + "*.xlsx")
    for filename in filenames:
        workbook = load_workbook(filename)
        # add_wb_to_db(workbook, cursor, conn) # TODO

        # Get a df of data from each sheet in workbook:
        df = get_workbook_data(workbook, config, True)

        add_variants_to_db(df, cursor, conn)
 
    # Select all variants that have interpreted = yes and are not submitted
    clinvar_df = pd.read_sql(
        "SELECT * FROM dbo.INCA WHERE Interpreted = 'yes'"
        "AND [Submission ID] is NULL AND [Accession ID] is NULL;",
        conn
    )
     
    # Get clinvar information from each variant to be submitted
    # subset this df for testing TODO delete this subset in prod would want to do all
    clinvar_df = clinvar_df.iloc[:1]

    

    cuh_variants, nuh_variants = collect_clinvar_data_to_submit()

    # Submit all the variants to ClinVar
    api_url = select_api_url(args.clinvar_testing, config)

    if cuh_variants:
        cuh_headers = {
            "SP-API-KEY": cuh_api_key,
            "Content-type": "application/json"
        }
        response = clinvar_api_request(
            api_url, cuh_headers, cuh_variants, config['CUH_acgs_url']
        )
        add_submission_id_to_db(response.json(), cursor, conn)

    if nuh_variants:
        nuh_headers = {
            "SP-API-KEY": nuh_api_key,
            "Content-type": "application/json"
        }
        response = clinvar_api_request(
            api_url, nuh_headers, nuh_variants, config['NUH_acgs_url']
        )
        add_submission_id_to_db(response.json(), cursor, conn)

    # Select all with submission IDs but no accession IDs
    # TODO figure out how to hold these until approved.
    submission_df = pd.read_sql(
        "SELECT [Submission ID] FROM dbo.INCA WHERE Interpreted = 'yes'"
        "AND [Submission ID] is not NULL AND [Accession ID] is NULL;",
        conn
    )

    # Need to make this work for CUH and NUH separately (blah)
    for index, variant in submission_df.iterrows():
        response = submission_status_check(variant["Submission ID"], headers, api_url)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()