import pandas as pd
import requests
import json
from requests.adapters import HTTPAdapter, Retry

def extract_clinvar_information(variant_row, ref_genomes):
    '''
    Extract information from Shire variant record and reformat into dictionary
    Inputs:
        variant: row from variant dataframe with data for one variant
        ref_genomes (list): list of valid reference genome values from config 
    outputs:
        clinvar_dict: dictionary of data to submit to clinvar
    '''
    if variant_row["ref_genome"] not in ref_genomes:
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


def collect_clinvar_data_to_submit(clinvar_df, ref_genomes):
    '''
    Cycle through a dataframe, and extract variants for each row. Call the
    function to reformat this into a dictionary for submission to ClinVar and
    return a list of these dictionaries
    Inputs
        clinvar_df (pandas.Dataframe): variant dataframe
        ref_genomes (list): list of valid reference genome values from config
    Outputs
        variants (list): list of dictionaries with variant data for submission
        to ClinVar
    '''
    variants = []
    for index, variant in clinvar_df.iterrows():
        clinvar_dict = extract_clinvar_information(variant, ref_genomes)
        variants.append(clinvar_dict)

    return variants


def create_header(api_key):
    '''
    Format header for ClinVar API submission
    Inputs:
        api_key (str): API key for ClinVar
    Outputs:
        header (dict): Header for ClinVar API query
    '''
    header = {
        "SP-API-KEY": api_key,
        "Content-type": "application/json"
    }
    return header


def clinvar_api_request(url, header, var_list, org_guidelines_url, print_json):
    '''
    Make request to the ClinVar API endpoint specified.
    Inputs:
        url (str): API endpoint URL
        api_key (dict): ClinVar API key
        var_list (list): list of variant data for each clinvar variant
        org_guidelines_url (str): url for the ACGS guidelines. These are
        different for CUH and NUH.
        print_json (boolean): controls whether or not to print each submission
        JSON
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

    if print_json is True:
        print("JSON to submit:")
        print(json.dumps(clinvar_data, indent=4, default=str))

    s = requests.Session()
    retries = Retry(total=10, backoff_factor=0.5)
    s.mount('https://', HTTPAdapter(max_retries=retries))
    response = s.post(url, data=json.dumps(clinvar_data, default=str), headers=header)
    return response


def process_submission_status(status, response):
    '''
    Process response to API query about submission status.
    Inputs
        status (str): Overall submission status
        response (dict): API response, which is a breakdown the response for
        each variant or errors if submission failed.
    Outputs:
        accession_ids (dict): dict of accession IDs, or empty dict if
        submission is not yet processed
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
