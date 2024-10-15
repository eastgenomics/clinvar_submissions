import pandas as pd
import requests
import json
from requests.adapters import HTTPAdapter, Retry

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
    if pd.isna(variant_row["comment_on_classification"]):
        variant_row["comment_on_classification"] = ""

    if variant_row["ref_genome"] not in ["GRCh37.p13", "GRCh38.p13"]:
        raise ValueError("Invalid genome build")

    assembly = variant_row["ref_genome"].split('.')[0]

    clinvar_dict = {
            'clinicalSignificance': {
                'clinicalSignificanceDescription': variant_row["germline_classification"],
                'comment': variant_row["comment_on_classification"],
                'dateLastEvaluated': variant_row["date_last_evaluated"]
            },
            'conditionSet': {
                'condition': [{'name': variant_row['preferred_condition_name']}]
            },
            'localID': variant_row["local_id"],
            'localKey': variant_row["linking_id"],
            'observedIn': [{
                'affectedStatus': variant_row['affected_status'],
                'alleleOrigin': variant_row["allele_origin"],
                'collectionMethod': variant_row['collection_method']
            }],
            'recordStatus': "novel",
            'variantSet': {
                'variant': [{
                    'chromosomeCoordinates': {
                        'assembly': assembly,
                        'alternateAllele': variant_row['alternate_allele'],
                        'referenceAllele': variant_row['reference_allele'],
                        'chromosome': str(variant_row['chromosome']),
                        'start': variant_row['start']
                    },
                    'gene': [{
                        'symbol': variant_row["gene_symbol"]
                    }],
                }],
            },
        }

    return clinvar_dict


def collect_clinvar_data_to_submit(clinvar_df):
    '''
    TODO
    '''
    variants = []
    for index, variant in clinvar_df.iterrows():
        clinvar_dict = extract_clinvar_information(variant)
        variants.append(clinvar_dict)

    return variants


def create_header(api_key):
    '''
    TODO
    '''
    header = {
        "SP-API-KEY": api_key,
        "Content-type": "application/json"
    }
    return header


def clinvar_api_request(url, header, var_list, org_guidelines_url):
    '''
    Make request to the ClinVar API endpoint specified.
    Inputs:
        url (str): API endpoint URL
        api_key (dict): ClinVar API key
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
                    'clinvarSubmission': var_list,
                    'assertionCriteria': {
                        'url': org_guidelines_url
                        }
                    }
                }
            }
        ]
    }

    print("JSON to submit:")
    print(json.dumps(clinvar_data, indent=4, default=str))

    s = requests.Session()
    retries = Retry(total=10, backoff_factor=0.5)
    s.mount('https://', HTTPAdapter(max_retries=retries))
    response = s.post(url, data=json.dumps(clinvar_data, default=str), headers=header)
    return response


def process_submission_status(status, response):
    '''
    TODO write docstring
    '''
    accession_ids = {}

    # check status
    # nb: status 'error' can be partial success; some submitted, some failed
    if status in ["processed", "error"]:
        batch_status = response.get('batchProcessingStatus')

        if batch_status == "Error":
            print("All submissions failed. No accession IDs.")

        else:
            print(
                f"{response['totalSuccess']} successfully submitted.\n"
                f"{response['totalErrors']} failed.\nGetting accession IDs..."
            )
            for submission in response.get("submissions"):
                if submission.get('errors', None) is not None:
                    # Record this somewhere or PASS
                    print(submission['identifiers'], submission.get('errors'))
                else:
                    accession_ids[submission['identifiers'][
                        'localID']
                        ] = submission['identifiers'][
                        'clinvarAccession']
    else:
        print(
            f"Batch submission has status {status}; not yet processed by "
            "ClinVar"
        )

    return accession_ids
