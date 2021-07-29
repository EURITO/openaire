

## Installation

Prior to performing these steps, make sure you have an Azure blob storage and SQL database. With some modifications, though, these steps can work with other cloud providers.

Then, perform each of those steps:

* Step 1. Create OpenAIRE and CORD19 publication, project, relationship tables in the database, by running `DB_transformation/1_create_tables.sql`
* Step 2. Run `process_folder.py` in `files_to_DB` folder to get data from OpenAIRE and CORD and store it in the database
* Steps 3-5. Run SQL scripts (3-5) in `DB_transformation` folder to perform merges and data transformations between the tables.
* Step 6. Run `5_join_pubs_projects.py` in files_to_DB repo to create an interim table
* Step 7. Run `DB_transformation/7_join_all_pubs_projs.sql` a staging table
* Step 8. Run Jupyter Notebook to generate output files from the staging table.