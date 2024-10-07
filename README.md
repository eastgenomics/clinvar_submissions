# clinvar submissions process

> [!WARNING]  
> Shire is a live database, so be careful when running!


## What does this script do?
This script searches a given folder for an Excel variant workbook, extracts those variants and adds to the Shire database in the dbo.INCA table. It then checks all the variants in the dbo.INCA table for an Interpreted status of 'yes' and submits those that have been interpreted to ClinVar.

# Process map

**Inputs (required)**:
* `--uid`: user ID to connect to the database server
* `--password`: password to connect to the database server
* `--path_to_workbooks`: local path to Excel workbooks that need submitting
* `--config`: config file, should be the config.json in this repo.

**Inputs (optional)**:
* `--clinvar_testing`: Default is False, if specified as True will use the test clinvar endpoint
* `--download_path`: if specified, will download workbooks to the specified path