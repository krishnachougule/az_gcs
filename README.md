Overview:
This document explains the process involved while loading the submission data which is in Azure Storage and how it lands to Big Query through series of tasks that from data ingestion to parsing to ETL. 
Data ingestion and cleansing and parsing and loading to landing layer or stage is handled by Dataflow jobs and ETL process from stage to core tables load is by BQ Merge scripts. The whole process is scheduled and orchestrated in Cloud Composer. 
