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
connection_string = os.getenv("CONN_STRING")
pipeline_options = {
    'project':os.getenv("PROJECT"),
    #'runner':'DirectRunner',
    'runner':'DataflowRunner',
    'region':os.getenv("REGION"),
    'worker_region':os.getenv("REGION"),
    'staging_location':os.getenv("STAG_LOC"),
    'temp_location':os.getenv("TEMP_LOC"),
    'network':os.getenv("NETWORK"),
    'service_account_email':os.getenv("SERVICE_ACCOUNT_EMAIL"),
    'subnetwork':os.getenv("SUBNETWORK"),
    #'requirements_file': 'requirements.txt',
    'use_public_ips':False,
    #'save_main_session':True 
    'template_location':'gs://cdra_submitted/templates/gcs_bq_base'
    }
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

def parse_xml(element):
    def read_blob_content_part(file_location):
        from google.cloud import storage
        storage_client = storage.Client()
        bucket_name = "cdra_submitted"
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_location)
        #print(blob)
        if not blob.exists():
            raise ValueError("Blob does not exist")
        blob_bytes = blob.download_as_bytes()
        xml_content = blob_bytes.decode('utf-8')
        return xml_content
    def parse_into_df(xml_content):
        def sanitize_xml_content(xml_content):
            # Replace ampersands with "and"
            sanitized_content = re.sub(r'&(?!amp;)', 'and', xml_content)
            # Remove any other invalid characters
            #sanitized_content = re.sub(r'[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]+', '', sanitized_content)
            return sanitized_content
        try:
            import xml.etree.ElementTree as ET
            sanitized_content = sanitize_xml_content(xml_content)
            root = ET.fromstring(sanitized_content)
        except ET.ParseError as e:
            traceback.print_exc()
            raise ValueError("Invalid XML content")
        data = []
        import pandas as pd
        for submission in root.iter('submission'):
                submission_xmsnId = submission.get('xmsnId')
                ncdrPatientId = 'None'
                for section in submission.iter('section'):
                    parentSection_code = section.get('code')
                    parentSection_displayName = section.get('displayName')
                    episodeKey = 'None'
                    if section.find('section') is not None:
                        counter = 1
                        for subsection in section.iter('section'):
                            childSection_code = subsection.get('code')
                            childSection_displayName = subsection.get('displayName')
                            subsection_Code=f"{subsection.get('code')}_{counter}"
                            counter += 1
                            for element in subsection.iter('element'):
                                Element_code = element.get('code')
                                Element_codeSystem = element.get('codeSystem')
                                Element_displayName = element.get('displayName')                                
                                value = element.find('value')
                                if value is not None:
                                    value_code = value.get('code')
                                    value_codeSystem = value.get('codeSystem')
                                    value_displayName = value.get('displayName')
                                    value_type = value.get('{http://www.w3.org/2001/XMLSchema-instance}type')
                                    value_value = value.get('value')
                                    value_unit = value.get('unit')
                                    if Element_displayName is None:
                                        Element_displayName = f"{parentSection_displayName}_{value_type}"
                                else:
                                    value_code = None
                                    value_codeSystem = None
                                    value_displayName = None
                                    value_type = None
                                    value_value = None
                                    value_unit = None
                                    if Element_displayName is None:
                                        Element_displayName = f"{parentSection_displayName}_{value_type}"
                                data.append([submission_xmsnId, ncdrPatientId,parentSection_code, parentSection_displayName, childSection_code, subsection_Code, childSection_displayName,episodeKey, Element_code, Element_codeSystem, Element_displayName, value_code, value_codeSystem, value_displayName, value_type, value_value, value_unit])        
                    else:
                        childSection_code = None
                        childSection_displayName = None 
                        subsection_Code=None
                        for element in section.iter('element'):
                                Element_code = element.get('code')
                                Element_codeSystem = element.get('codeSystem')
                                Element_displayName = element.get('displayName')                                
                                value = element.find('value')
                                if value is not None:
                                    value_code = value.get('code')
                                    value_codeSystem = value.get('codeSystem')
                                    value_displayName = value.get('displayName')
                                    value_type = value.get('{http://www.w3.org/2001/XMLSchema-instance}type')
                                    value_value = value.get('value')
                                    value_unit = value.get('unit')
                                    if Element_displayName is None:
                                        Element_displayName = f"{parentSection_displayName}_{value_type}"
                                else:
                                    value_code = None
                                    value_codeSystem = None
                                    value_displayName = None
                                    value_type = None
                                    value_value = None
                                    value_unit = None
                                    if Element_displayName is None:
                                        Element_displayName = f"{parentSection_displayName}_{value_type}"
                                data.append([submission_xmsnId, ncdrPatientId,parentSection_code, parentSection_displayName, childSection_code,subsection_Code, childSection_displayName,episodeKey, Element_code, Element_codeSystem, Element_displayName, value_code, value_codeSystem, value_displayName, value_type, value_value, value_unit])        
            
        for patient in root.iter('patient'):
            submission_xmsnId = 'None'
            ncdrPatientId = patient.get('ncdrPatientId')
            for section in patient.iter('section'):
                episodeKey = 'None'
                parentSection_code = section.get('code')
                parentSection_displayName = section.get('displayName')
                childSection_code = None
                childSection_displayName = None 
                subsection_Code=None
                for element in section.iter('element'):
                        Element_code = element.get('code')
                        Element_codeSystem = element.get('codeSystem')
                        Element_displayName = element.get('displayName')                                
                        value = element.find('value')
                        if value is not None:
                            value_code = value.get('code')
                            value_codeSystem = value.get('codeSystem')
                            value_displayName = value.get('displayName')
                            value_type = value.get('{http://www.w3.org/2001/XMLSchema-instance}type')
                            value_value = value.get('value')
                            value_unit = value.get('unit')
                            if Element_displayName is None:
                                Element_displayName = f"{parentSection_displayName}_{value_type}"
                        else:
                            value_code = None
                            value_codeSystem = None
                            value_displayName = None
                            value_type = None
                            value_value = None
                            value_unit = None
                            if Element_displayName is None:
                                Element_displayName = f"{parentSection_displayName}_{value_type}"
                        data.append([submission_xmsnId, ncdrPatientId,parentSection_code, parentSection_displayName, childSection_code,subsection_Code, childSection_displayName,episodeKey, Element_code, Element_codeSystem, Element_displayName, value_code, value_codeSystem, value_displayName, value_type, value_value, value_unit])        
        
            for episode in patient.iter('episode'):
                episodeKey = episode.get('episodeKey')
                for section in episode.iter('section'):
                    parentSection_code = section.get('code')
                    parentSection_displayName = section.get('displayName')
                    if section.find('section') is not None:
                        counter = 1
                        for subsection in section.iter('section'):
                            childSection_code = subsection.get('code')
                            childSection_displayName = subsection.get('displayName')
                            subsection_Code=f"{subsection.get('code')}_{counter}"
                            counter += 1
                            for element in subsection.iter('element'):
                                Element_code = element.get('code')
                                Element_codeSystem = element.get('codeSystem')
                                Element_displayName = element.get('displayName')                            
                                value = element.find('value')
                                if value is not None:
                                    value_code = value.get('code')
                                    value_codeSystem = value.get('codeSystem')
                                    value_displayName = value.get('displayName')
                                    value_type = value.get('{http://www.w3.org/2001/XMLSchema-instance}type')
                                    value_value = value.get('value')
                                    value_unit = value.get('unit')
                                    if Element_displayName is None:
                                        Element_displayName = f"{parentSection_displayName}_{value_type}"
                                else:
                                    value_code = None
                                    value_codeSystem = None
                                    value_displayName = None
                                    value_type = None
                                    value_value = None
                                    value_unit = None
                                    if Element_displayName is None:
                                        Element_displayName = f"{parentSection_displayName}_{value_type}"
                                data.append([submission_xmsnId, ncdrPatientId,parentSection_code, parentSection_displayName, childSection_code,subsection_Code, childSection_displayName,episodeKey, Element_code, Element_codeSystem, Element_displayName, value_code, value_codeSystem, value_displayName, value_type, value_value, value_unit])        
                    else:
                        childSection_code = None
                        childSection_displayName = None 
                        subsection_Code=None
        
                        for element in section.iter('element'):
                                Element_code = element.get('code')
                                Element_codeSystem = element.get('codeSystem')
                                Element_displayName = element.get('displayName')                                
                                value = element.find('value')
                                if value is not None:
                                    value_code = value.get('code')
                                    value_codeSystem = value.get('codeSystem')
                                    value_displayName = value.get('displayName')
                                    value_type = value.get('{http://www.w3.org/2001/XMLSchema-instance}type')
                                    value_value = value.get('value')
                                    value_unit = value.get('unit')
                                    if Element_displayName is None:
                                        Element_displayName = f"{parentSection_displayName}_{value_type}"
                                else:
                                    value_code = None
                                    value_codeSystem = None
                                    value_displayName = None
                                    value_type = None
                                    value_value = None
                                    value_unit = None
                                    if Element_displayName is None:
                                        Element_displayName = f"{parentSection_displayName}_{value_type}"
                                data.append([submission_xmsnId, ncdrPatientId,parentSection_code, parentSection_displayName, childSection_code,subsection_Code, childSection_displayName,episodeKey, Element_code, Element_codeSystem, Element_displayName, value_code, value_codeSystem, value_displayName, value_type, value_value, value_unit])        

                #data.append([submission_xmsnId, ncdrPatientId,parentSection_code, parentSection_displayName, childSection_code, childSection_displayName,episodeKey, Element_code, Element_codeSystem, Element_displayName, value_code, value_codeSystem, value_displayName, value_type, value_value, value_unit]
        import pandas as pd
        df = pd.DataFrame(data, columns=['SubmissionId', 'PatientId','parentSection_code','parentSection_displayName','childSection_code','subsection_Code','childSection_displayName','episodeKey','Element_code','Element_codeSystem','Element_displayName','value_code','value_codeSystem','value_displayName','value_type','value_value','value_unit'])
        df_unique = df.drop_duplicates()

        df_unique1 = df_unique.copy()

        ParticipantName = df.loc[df['Element_displayName'] =="Participant Name", 'value_value'].values[0]
        df_unique1['Participant_Name'] = ParticipantName
        #df_final = df_unique1[df_unique1['parentSection_code'] != df_unique1['childSection_code']]
        #print(df_unique)
        return df_unique1

    def parse_xml_and_add_participant_id(file_location, participant_id,Final_XML_Name,Create_Date_Time):
        try:
            xml_content = read_blob_content_part(file_location)
            df = parse_into_df(xml_content)
            df['Participant_Id'] = participant_id
            df['Final_XML_Name'] = Final_XML_Name
            df['Create_Date_Time'] = Create_Date_Time
            #print(df.head(5))
            return df
        except ValueError as e:
            traceback.print_exc()
            return pd.DataFrame()
    def df_to_dict2(data):
        return data.to_dict('records')
    def process_xml_file(blob_file, participant_id,Final_XML_Name,Create_Date_Time):
        try:
            df = parse_xml_and_add_participant_id(blob_file, participant_id,Final_XML_Name,Create_Date_Time)
            dict_records = df_to_dict2(df)
            return dict_records
        except ValueError as e:
            traceback.print_exc()
            return []
    xml_file_locations = element['Destination_XML_Location']
    participant_ids = element['Participant_Id']
    Final_XML_Names = element['Final_XML_Name']
    Create_Date_Time = element['Create_Date_Time']
    parsed_data = []

    if not isinstance(xml_file_locations, list):
        xml_file_locations = [xml_file_locations]

    if not isinstance(participant_ids, list):
        participant_ids = [participant_ids]
    if not isinstance(Final_XML_Names, list):
        Final_XML_Names = [Final_XML_Names]
    if not isinstance(Create_Date_Time, list):
        Create_Date_Time = [Create_Date_Time]  

    # Parse XML files and add participant IDs
    for file_location, participant_id, Final_XML_Name,Create_Date_Time in zip(xml_file_locations, participant_ids,Final_XML_Names,Create_Date_Time):
        parsed_xml_data = process_xml_file(file_location, participant_id,Final_XML_Name,Create_Date_Time)
        parsed_data.extend(parsed_xml_data)

    return parsed_data

def pipeline2():
    import xml.etree.ElementTree as ET
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    import os
    from dotenv import load_dotenv
    load_dotenv()    
    with beam.Pipeline(options=pipeline_options) as p2:
        import xml.etree.ElementTree as ET
        import apache_beam as beam
        #import pyodbc
        import pandas as pd
        from datetime import datetime, date
        from google.cloud import storage
        from datetime import date
        import traceback
        from google.cloud import bigquery
        import re
        # Read the processed data from Pipeline 1
        today = date.today()
        #yesterday = today - datetime.timedelta(days=1)
        createdDate='2023-07-14'
        query = f"""
        SELECT *
        FROM `CDRA_Submitted.Participant_Submission_Record`
        WHERE TIMESTAMP_TRUNC(Create_Date_Time, DAY) = TIMESTAMP('{createdDate}') and Case_Type = "B"
        """
        sql_data = p2 | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(
            query=query,
            project='hca-bpg-cdr-dev',
            use_standard_sql=True,
            flatten_results=True
        )

        # Parse XML data and add participant IDs
        parsed_data = sql_data | 'Parse XML and Add Participant IDs' >> beam.FlatMap(parse_xml)

        # Print the parsed data for debugging
        #parsed_data | 'Print Parsed Data' >> beam.Map(print)
                               
        # today_date = datetime.today().date()

        # Write the parsed data to a new location
        #today_date = datetime.today().date()
        #table_old= 'cdra_base_composer' + today_date.isoformat().replace('-', '_')
        table= 'cdra_base'
        #table='cdra_base_2023_06_05'
        table_schema = 'Create_Date_Time:TIMESTAMP,Participant_Id:INTEGER,Participant_Name:STRING,SubmissionId:STRING,PatientId:STRING,Final_XML_Name:STRING,parentSection_code:STRING,parentSection_displayName:STRING,childSection_code:STRING,subsection_Code:STRING,childSection_displayName:STRING,episodeKey:STRING,element_code:STRING,element_codeSystem:STRING,element_displayName:STRING,value_code:STRING,value_codeSystem:STRING,value_displayName:STRING,value_type:STRING,value_value:STRING,value_unit:STRING'
        parsed_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=table,
            dataset='CDRA_Staging',
            schema=table_schema,
            project=os.getenv("PROJECT"),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location = 'gs://cdra_submitted/temp' 
        )

# Run the pipelines sequentially
if __name__ == '__main__':  
   pipeline2()

