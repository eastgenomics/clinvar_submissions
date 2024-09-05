import json
import requests
from requests.adapters import HTTPAdapter, Retry
import argparse
import os.path
import glob
from utils.utils import *

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
        variant["Ref genome"].split('.')[0] if variant["Ref genome"] in
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
    # add a record for the sample to workbooks table.
    query = ("INSERT INTO  [Shiredata].[dbo].[Workbooks]")

def parse_args():
    parser = argparse.ArgumentParser(
                            description="",
                            formatter_class=(
                                argparse.ArgumentDefaultsHelpFormatter
                                )
                        )
    parser.add_argument('--clinvar_api_key')
    parser.add_argument('--clinvar_testing')
    parser.add_argument('--path_to_workbooks')
    parser.add_argument('--config')
    args = parser.parse_args()
    return args



def main():
    '''
    Script entry point
    '''
    args = parse_args()

    workbooks = glob.glob(args.path_to_workbooks + "*.xlsx")
    with open(args.config) as f:
        config = json.load(f)
    
    # TODO change so this uses the dx file in 001
    with open(args.clinvar_api_key) as f:
        api_key = f.readlines()[0].strip()

    for workbook in workbooks:
        print(workbook)
        add_wb_to_db(workbook, cursor=None, conn=None)
        get_summary_fields(workbook, config, True)
        df_included = get_included_fields(workbook)
        df_final = get_report_fields(workbook, df_included)
        print(df_final)
    exit()

    headers = {
            "SP-API-KEY": api_key,
            "Content-type": "application/json"
        }
    
    # # Get variants from db
    # query = (
    #     "SELECT relevant fields from db WHERE Interpreted = Yes"
    # )

    # variants = execute(query)

    # # for variant in db if status = clinvar + not submitted
    # list_to_submit = []
    # for variant in variants:
    #     clinvar_dict = extract_clinvar_information(variant)
    #     # R444 is a pharmacogenomic test, so should be submitted differently
    #     if variant["Rcode"] == "R444.1":
    #         parp_inhibitor_submission(clinvar_dict)
    #     list_to_submit.append(clinvar_dict)
    with open("/home/katherine/clinvar_submissions/test.json") as f:
        r = json.load(f)

    with open("/home/katherine/clinvar_submissions/test2.json") as f:
        s = json.load(f)

    #cuh_variants = [x for x in variants if variant["OrganisationID"] == 288359]
    #nuh_variants = [x for x in variants if variant["OrganisationID"] == 509428]

    api_url = select_api_url("True")
    drug = parp_inhibitor_submission(s)
    response = submission_status_check("SUB14707775", headers, api_url)
    print(response)
    
    response = clinvar_api_request(api_url, headers, drug, 'https://submit.ncbi.nlm.n'
        'ih.gov/api/2.0/files/kf4l0sn8/uk-practice-guidelines-for-variant-clas'
        'sification-v4-01-2020.pdf/?format=attachment')
    response_dict = response.json()
    print(response_dict)
    if response_dict.get('id'):
        query = ("SET submission ID = ID")
    else:
        query = ()
        # TODO handle fails, should we leave this blank


if __name__ == "__main__":
    main()