**Overview**:
This document explains the process involved while loading the submission data which is in Azure Storage and how it lands to Big Query through series of tasks that from data ingestion to parsing to ETL. 
Data ingestion and cleansing and parsing and loading to landing layer or stage is handled by Dataflow jobs and ETL process from stage to core tables load is by BQ Merge scripts. The whole process is scheduled and orchestrated in Cloud Composer. 

**Detailed Process Flow Description**
Input files are stored in Azure Storage Blob as XML Files. The data load process begins by retrieving a connection URL to the Azure SQL database from a secret manager. It then uses the connection URL to run a query that selects the most recent participant submission records from the database. The results of the query are loaded into a Big 
Based on the XML file and location details from the Participant_Submission_Record_Stage table into two separate GCS buckets: one for base data and one for follow-up data. The base data includes the participant's demographics and clinical information, while the follow up data includes any additional information that was submitted after the initial submission. 
The input Blob files are copied from Azure Storage to Google Cloud Storage bucket and processed and load it into Big Query using Dataflow. The Dataflow jobs use a custom pipeline template that is defined in the templates directory. The template performs a range of transformations on the data, including:
•	Parsing the XML files
•	Cleaning the data
•	Normalizing the data
•	Enriching the data with additional information from other datasets
The processed data is then loaded into two Big Query tables: one for base data and one for follow-up data.
Once stage is loaded, views are created in the staging dataset to implement ETL process and the based on the view logic, data is loaded to BQ Core tables using merge for insert and update. The merge queries ensure that the data in the core tables is always up-to-date.

