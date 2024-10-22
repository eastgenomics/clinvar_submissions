import utils.clinvar as clinvar
import pandas as pd
import unittest
import unittest.mock as mock
import json
from copy import deepcopy

class TestClinvar(unittest.TestCase):
    ref_genomes = ["GRCh37.p13", "GRCh38.p13"]
    data = [{
        "local_id": "uid-123456789",
        "linking_id": "uid-123456789",
        "chromosome": 7,
        "start": 117232266,
        "reference_allele": "C",
        "alternate_allele": "CA",
        "gene_symbol": "CFTR",
        "comment_on_classification": "PVS1,PM3_Strong",
        "germline_classification": "Pathogenic",
        "date_last_evaluated": "2024-10-10",
        "preferred_condition_name": "Cystic fibrosis",
        "collection_method": "clinical testing",
        "affected_status": "yes",
        "allele_origin": "germline",
        "ref_genome": "GRCh37.p13",
    }]

    df = pd.DataFrame(data)

    correct_submission_dict = {
        "clinicalSignificance": {
            "clinicalSignificanceDescription": "Pathogenic",
            "comment": "PVS1,PM3_Strong",
            "dateLastEvaluated": "2024-10-10",
        },
        "conditionSet": {"condition": [
            {"name": "Cystic fibrosis"}
        ]},
        "localID": "uid-123456789",
        "localKey": "uid-123456789",
        "observedIn": [
            {
                "affectedStatus": "yes",
                "alleleOrigin": "germline",
                "collectionMethod": "clinical testing",
            }
        ],
        "recordStatus": "novel",
        "variantSet": {
            "variant": [
                {
                    "chromosomeCoordinates": {
                        "assembly": "GRCh37",
                        "alternateAllele": "CA",
                        "referenceAllele": "C",
                        "chromosome": "7",
                        "start": 117232266,
                    },
                    "gene": [{"symbol": "CFTR"}],
                }
            ]
        },
    }

    submission_response = {
        "batchProcessingStatus": "Partial success",
        "totalCount": 2,
        "totalErrors": 1,
        "totalSuccess": 1,
        "submissions": [
            {
            "identifiers": {
                "localID": "uid_12345",
            },
            "errors": [
                { "output": {
                    "errors": [
                    {
                        "userMessage": "The identifier cannot be validated"
                    }
                    ]
                }
                }
            ]
            },
            {
            "identifiers": {
                "localID": "uid_67890",
                "clinvarAccession": "SCV000067890"
            }
        }
        ]
    }
    
    def test_extract_clinvar_information_reformats_data_correctly(self):
        clinvar_submission = clinvar.extract_clinvar_information(
            self.df.iloc[0], self.ref_genomes
        )
        assert clinvar_submission == self.correct_submission_dict

    def test_error_if_ref_genome_not_in_list_of_ref_genomes(self):
        invalid_df = self.df.copy()
        invalid_df.loc[0, 'ref_genome'] = 'invalid'
        with self.assertRaises(ValueError, msg="Invalid genome build"):
            clinvar.extract_clinvar_information(
                invalid_df.iloc[0], self.ref_genomes
            )

    def test_variants_accrued_correctly(self):
        '''
        Multiply original dataframe, to give new df with three identical
        variants, check that passing this df to collect_clinvar_data_to_submit
        yields a list three clinvar submission dicts, one for each variant row
        '''
        copy_df = self.df.copy()
        multiple_variants_df = pd.concat([copy_df] * 3, ignore_index=True)
        variant_list = clinvar.collect_clinvar_data_to_submit(
            multiple_variants_df, self.ref_genomes
        )
        assert variant_list == [
            self.correct_submission_dict,
            self.correct_submission_dict,
            self.correct_submission_dict
        ]

    def test_create_header(self):
        assert clinvar.create_header('foobar') == (
            {"SP-API-KEY": 'foobar', "Content-type": "application/json"}
        )

    @mock.patch('requests.Session')
    def test_clinvar_api_request(self, mock_session):
        '''
        Test that tge clinvar API request function makes post requests as
        expected.
        '''
        mock_session.return_value.post.return_value = {'id': 'SUB12345'}

        # Define inputs to clinvar_api_request
        api_url = 'https://clinvar-api.fake-url.com/submit'
        org_url = 'https://clinvar.com/fake-acgs-guidelines'
        headers = {"SP-API-KEY": 'foobar', "Content-type": "application/json"}

        # Define expected data for clinvar_api_request to use for API POST
        correct_data = {
            "actions": [
                {
                "type": "AddData",
                "targetDb": "clinvar",
                "data": {
                    "content": {
                        'clinvarSubmission': [self.correct_submission_dict],
                        'assertionCriteria': {
                            'url': 'https://clinvar.com/fake-acgs-guidelines'
                            }
                        }
                    }
                }
            ]
        }

        response = clinvar.clinvar_api_request(
            api_url, headers, [self.correct_submission_dict], org_url, False
        )
        with self.subTest("Check the function was called with expected data"):
            mock_session.return_value.post.assert_called_once_with(
                api_url, data=json.dumps(correct_data), headers=headers
            )

        with self.subTest("Check the function returned the mocked response"):
            assert response == {'id': 'SUB12345'}


    def test_process_submission_status_with_one_error_message(self):
        assert clinvar.process_submission_status(
            'error', self.submission_response
            ) == (
            {"uid_67890": "SCV000067890"},
            {"uid_12345": "The identifier cannot be validated"}
        )
    
    def test_process_submission_status_with_two_error_messages(self):
        response = deepcopy(self.submission_response)
        # Add extra error
        response['submissions'][0]['errors'][0]['output']['errors'].append(
            {'userMessage': 'Matching record already exists.'}
        )

        assert clinvar.process_submission_status('error', response) == (
            {"uid_67890": "SCV000067890"},
            {"uid_12345": "The identifier cannot be validated, Matching record"
             " already exists."}
        )
    
    def test_process_submission_status_if_all_successful(self):
        response = deepcopy(self.submission_response)
        # Overwrite error with mock successful submission
        response['submissions'][0] = {
            "identifiers": {
                "localID": "uid_12345",
                "clinvarAccession": "SCV000012345"
                }
            }
        assert clinvar.process_submission_status('processed', response) == (
            {"uid_67890": "SCV000067890", "uid_12345": "SCV000012345"}, {}
        )
