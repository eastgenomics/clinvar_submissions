import json
import requests
from requests.adapters import HTTPAdapter, Retry
import argparse
import os.path
import glob
from utils.utils import *
import pyodbc

def parp_inhibitor_submission(clinvar_dict):
    '''
    Edit clinvar_submission dict to add drug responsiveness information if this
    is a R444 case, as these are pharmacogenomic cases.
    '''
    print(clinvar_dict)
    interpretation = clinvar_dict['clinicalSignificance'][
        'clinicalSignificanceDescription'
    ]
    translate = {
        "Pathogenic": "Responsive",
        "Likely pathogenic": "Likely responsive",
        "Uncertain significance": "Uncertain responsiveness",
        "Likely benign": "Likely unresponsive",
        "Benign": "Unresponsive"
    }
    translate2 = {
        "Pathogenic": "No function",
        "Likely pathogenic": "Uncertain function",
        "Uncertain significance": "Uncertain function",
        "Likely benign": "Uncertain function",
        "Benign": "Allele function"
    }
    drug_response_details = translate2.get(interpretation)
    #drug_response = translate.get(interpretation)

    # if can't be translated, stop + add error to db
    if drug_response_details == None:
        query = ("db error! value not translateable")
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


def extract_clinvar_information(variant):
    '''
    Extract information from variant CSV and reformat into dictionary.
    Inputs:
        variant: row from variant dataframe with data for one variant
    outputs:
        clinvar_dict: dictionary of data to submit to clinvar
    '''
    if str(variant["Comment on classification"]) == "nan":
        variant["Comment on classification"] = "None"

    assembly = (
        variant["Ref_genome"].split('.')[0] if variant["Ref_genome"] in
        ["GRCh37.p13", "GRCh38.p13"] else RuntimeError(
            f"Invalid genome build"
        )
    )

    clinvar_dict = {
            'clinicalSignificance': {
                'clinicalSignificanceDescription': variant["Germline classification"],
                'comment': variant["Comment on classification"],
                'dateLastEvaluated': variant["Date last evaluated"]
            },
            'conditionSet': {
                'condition': [{'name': variant['Preferred condition name']}]
            },
            'localID': variant["Local ID"],
            'localKey': variant["Linking ID"],
            'observedIn': [{
                'affectedStatus': variant['Affected status'],
                'alleleOrigin': variant["Allele origin"],
                'collectionMethod': variant['Collection method']
            }],
            'recordStatus': "novel",
            'variantSet': {
                'variant': [{
                    'chromosomeCoordinates': {
                        'assembly': assembly,
                        'alternateAllele': variant['Alternate allele'],
                        'referenceAllele': variant['Reference allele'],
                        'chromosome': str(variant['Chromosome']),
                        'start': variant['Start']
                    },
                    'gene': [{
                        'symbol': variant["Gene symbol"]
                    }],
                }],
            },
        }

    return clinvar_dict


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
        cols = [f"[{x}]" for x in temp_df.columns]
        columns = ", ".join(cols)
        qmarks = ", ".join("?" * temp_df.shape[1])
        qry = "Insert Into [Shiredata].[dbo].[INCA] (%s) Values (%s)" % (
            columns,
            qmarks
        )
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

def parse_args():
    parser = argparse.ArgumentParser(
                            description="",
                            formatter_class=(
                                argparse.ArgumentDefaultsHelpFormatter
                                )
                        )
    # TODO add help strings
    parser.add_argument('--clinvar_api_key')
    parser.add_argument('--clinvar_testing')
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
    print(f"Searching {args.path_to_workbooks}...")
    workbooks = glob.glob(args.path_to_workbooks + "*.xlsx")
    with open(args.config) as f:
        config = json.load(f)

    conn_str = (
        f"DSN=gemini;DRIVER={{SQL Server Native Client 11.0}};"
        f"UID={args.uid};PWD={args.password}"
    ) 
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    # TODO change so this uses the dx file in 001
    # or should it !! could this be a nextflow thing !! much to think of
    with open(args.clinvar_api_key) as f:
        api_key = f.readlines()[0].strip()

    for workbook in workbooks:
        print(workbook)
        # add_wb_to_db(workbook, cursor, conn) # TODO
        # Get a df of data from each sheet in workbook:
        # summary sheet, included variants sheet and any interpret sheets
        df_summary = get_summary_fields(workbook, config, True)
        df_included = get_included_fields(workbook)
        df_interpret = get_report_fields(workbook, df_included)
        # merge these to get one df
        if not df_included.empty:
            df_merged = pd.merge(df_included, df_summary, how="cross")
        else:
            df_merged = pd.concat([df_summary, df_included], axis=1)

        df_final = pd.merge(df_merged, df_interpret, on="HGVSc", how="left")
        print(df_final)
        add_variants_to_db(df_final, cursor, conn)

    # Select all variants that have interpreted = yes and are not submitted
    query = (
        "SELECT * FROM dbo.INCA WHERE Interpreted = 'yes'"
        "AND [Submission ID] is NULL AND [Accession ID] is NULL;"
    )
    cursor.execute(query)
    conn.commit() 
    clinvar_df = pd.read_sql(query, conn)
    conn.close()
    # subset this df for testing TODO delete this subset in prod would want to do all
    print(clinvar_df)
    clinvar_df = clinvar_df.iloc[:1]

    # for variant in db if status = clinvar + not submitted
    variants = []
    cuh_variants = []
    nuh_variants = []
    for index, variant in clinvar_df.iterrows():
        clinvar_dict = extract_clinvar_information(variant)
        # R444 is a pharmacogenomic test, so should be submitted differently
        if variant["Rcode"] == "R444.1":
            parp_inhibitor_submission(clinvar_dict)
        variants.append(clinvar_dict)
    print(variants)

    # Submit all the variants to ClinVar
    api_url = select_api_url("True")
    nuh_headers = {
            "SP-API-KEY": nuh_api_key,
            "Content-type": "application/json"
        }

    cuh_headers = {
            "SP-API-KEY": cuh_api_key,
            "Content-type": "application/json"
        }

    cuh_variants = [x for x in variants if variant["OrganisationID"] == 288359]
    nuh_variants = [x for x in variants if variant["OrganisationID"] == 509428]

    # submit CUH variants
    if cuh_variants:
        response = clinvar_api_request(
            api_url, cuh_headers, cuh_variants, config['CUH_acgs_url']
        )
        response_dict = response.json()

        if response_dict.get('id'):
            sub_id = response_dict.get('id')
            query = (f"UPDATE dbo.INCA SET [Submission ID] = '{sub_id}'")
        else:
            query = ()
            # TODO handle fails, should we leave this blank

    response_dict = response.json()
    print(response_dict)

    # Select all with submission IDs but no accession IDs
    query = (
        "SELECT [Submission ID] FROM dbo.INCA WHERE Interpreted = 'yes'"
        "AND [Submission ID] is not NULL AND [Accession ID] is NULL;"        
    )
    cursor.execute(query)
    conn.commit()
    submission_df = pd.read_sql(query, conn)

    # Need to make this work for CUH and NUH separately (blah)
    for index, variant in submission_df.iterrows():
        response = submission_status_check(variant["Submission ID"], headers, api_url)


if __name__ == "__main__":
    main()