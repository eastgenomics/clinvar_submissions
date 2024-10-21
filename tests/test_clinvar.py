import utils.clinvar as clinvar
import pandas as pd
import unittest
import numpy as np

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

    def test_clinvar_data_reformatted_correctly(self):
        '''
        Test that when given a dataframe, this function reformats it correctly
        '''
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

