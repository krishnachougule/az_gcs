import xml.etree.ElementTree as ET
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
#import pyodbc
import pandas as pd
from datetime import datetime, date
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv
from datetime import date
import traceback
from google.cloud import bigquery
load_dotenv()
import re
project_Id = os.getenv("PROJECT")
dataset_Id = "CDRA_Staging"
table_Id = "Participant_Submission_Record_Stage"
query = '''
SELECT * FROM `hca-bpg-cdr-dev.CDRA_Staging.Participant_Submission_Record_Stage` 
where Case_Type = 'B'
'''
connection_string = os.getenv("CONN_STRING")
pipeline_options = {
    'project':os.getenv("PROJECT"),
    'runner':'DirectRunner',
    #'runner':'DataflowRunner',
    'region':os.getenv("REGION"),
    'worker_region':os.getenv("REGION"),
    'staging_location':os.getenv("STAG_LOC"),
    'temp_location':os.getenv("TEMP_LOC"),
    'network':os.getenv("NETWORK"),
    'service_account_email':os.getenv("SERVICE_ACCOUNT_EMAIL"),
    'subnetwork':os.getenv("SUBNETWORK"),
    'requirements_file': 'requirements.txt',
    'use_public_ips':False,
    #'save_main_session':True 
    #'template_location':'gs://cdra_submitted/templates/az_gcs_base'
    }
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
def execute_sql_query(query):
    from google.cloud import bigquery
    project_Id = os.getenv("PROJECT")
    #dataset_Id = "CDRA_Staging"
    #table_Id = "Participant_Submission_Record_Stage"
    client = bigquery.Client(project_Id)
    df = client.query(query).to_dataframe()
    selected_columns =['Participant_Id','Registry_Id','Case_Type','Quarter_Year_Text','Final_Xml_Name','Final_XML_Location_Text','Create_Date_Time']
    gcpDF= df[selected_columns]
    gcpDF = gcpDF.rename(columns={'Final_Xml_Name': 'Final_XML_Name'})
    return gcpDF


    
def process(element):
    from azure.storage.blob import BlobServiceClient
    from datetime import datetime, date
    def upload_blob(blob_name, source_blob_data, destination_bucket_name):
        from google.cloud import storage
        gcs_client = storage.Client()
        bucket = gcs_client.get_bucket(destination_bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(source_blob_data)
    basePath=[]
    destFile=[]
    source_container_name = "ncdr-tvt"
    destination_bucket_name = "cdra_submitted"
    #today_folder = date.today().strftime("%Y-%m-%d") + '/' + 'base' + '/'
    element['Create_Date'] = pd.to_datetime(element['Create_Date_Time'])
    bucketDate = element['Create_Date'].dt.date
    bucketDate_str = bucketDate.iloc[0].strftime('%Y-%m-%d')
    today_folder = bucketDate_str + '/' + 'base' + '/'
    connection_string_storage = "DefaultEndpointsProtocol=https;AccountName=paracdraprodsa01;AccountKey=yfBMZXbSfUIl4BabH9HbC6c1VfW6RT1f21xejORCNmwkpf5PC0BjDZTUvyQXalBKx9jgH4Gl/ThT/2fShDdlJw==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string_storage)
    container_client = blob_service_client.get_container_client(source_container_name)
    xml_file_name = element['Final_XML_Name'].tolist()
    xml_file_path = element['Final_XML_Location_Text'].tolist()
    for file_name,file_path in zip(xml_file_name,xml_file_path):
        full_path = file_path + '/' + file_name
        blob_list = container_client.list_blobs(name_starts_with=full_path)
        for blob in blob_list:
            source_blob_name = blob.name
            baseName = file_name
            destination_blob_name = os.path.join(today_folder,os.path.basename(blob.name))
            source_blob_client = blob_service_client.get_blob_client(container=source_container_name, blob=source_blob_name)
            source_blob_data = source_blob_client.download_blob().readall()
            upload_blob(destination_blob_name, source_blob_data, destination_bucket_name)
            basePath.append(baseName)
            destFile.append(destination_blob_name)
    element['Destination_XML_Location']=destFile
    element['Destination_XML_File_Name']=basePath
    element['Destination_Uploaded_Date'] = datetime.now()
    element['Last_Update_Date_Time'] = datetime.now()
    #print(element)
    return(element)

def df_to_dict1(data):
    return data.to_dict('records')


def pipeline1():
    with beam.Pipeline(options=pipeline_options) as p1:
        sql_data = p1 | 'Read SQL Data' >> beam.Create([query]) | beam.Map(execute_sql_query) 
        #sql_data | "Print" >> beam.Map(print)
    # Extract required columns and process each row
        processed_data = (sql_data | 'Process Rows' >> beam.Map(process) 
                            | "df to dictionary" >> beam.Map(df_to_dict1)
                            | "Flatmap1" >> beam.FlatMap(lambda res1:res1)                           
                            
        ) 

    # Write the processed data to BigQuery

        processed_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table='Participant_Submission_Record',
            dataset='CDRA_Submitted',
            project=os.getenv("PROJECT"),
            schema='Participant_Id:INTEGER, Registry_Id:INTEGER, Case_Type:STRING,Quarter_Year_Text:STRING, Final_XML_Name:STRING, Final_XML_Location_Text:STRING, Create_Date_Time:TIMESTAMP, Destination_Uploaded_Date:TIMESTAMP, Destination_XML_Location:STRING, Destination_XML_File_Name:STRING, Last_Update_Date_Time:TIMESTAMP',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location = 'gs://cdra_submitted/temp'
            )

if __name__ == '__main__':  
   pipeline1()


