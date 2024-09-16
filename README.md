# clinvar submissions process

> [!WARNING]  
> Shire is a live database, so be careful when running!


## What does this script do?
This script searches a given folder for an Excel variant workbook, extracts those variants and adds to the Shire database in the dbo.INCA table. It then checks all the variants in the dbo.INCA table for an Interpreted status of 'yes' and submits those that have been interpreted.

# Process map

**Inputs (required)**:
* `--uid`: user ID to connect to the database server
* `--password`: password to connect to the database server

**Inputs (optional)**:
* `--testing`: if specified, will run first 5 records in database only.
* `--download_path`: if specified, will download workbooks to the specified path