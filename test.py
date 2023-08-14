from airflow import models
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
#from airflow.providers.apache.beam.operators.beam_run_python_pipeline import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
#from airflow.contrib.operators.dataflow_operator import DataflowTemplatedJobStartOperator
#from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
#from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.empty import EmptyOperator
load_dotenv()

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dataflow_options = {
    'project': "hca-bpg-cdr-dev",
    'runner': 'DataflowRunner',
    'staging_location': "gs://cdra_submitted/staging",
    'temp_location': "gs://cdra_submitted/temp",
    'jobName': 'az-to-bq-dataflow',
    'region': "us-east4",
    'serviceAccountEmail' : 'hca-bpg-cdr-dev-comp-sa@hca-bpg-cdr-dev.iam.gserviceaccount.com',
    'network' : 'hca-para-bpg-shared-vpc',
    'subnetwork' : 'https://www.googleapis.com/compute/v1/projects/hca-para-bpg-shared-networks/regions/us-east4/subnetworks/hca-bpg-cdr-dev-dataflow-subnet-us-east4',
    'ipConfiguration': 'WORKER_IP_PRIVATE',
    'workerRegion': 'us-east4',
    'requirements_file': '/home/airflow/gcs/dags/requirements.txt',
    #'input': 'gs://your-bucket-name/input-data',
    #'output': 'gs://your-bucket-name/output-data',
}
with models.DAG('Azure_To_Bigquery_Xml_Parser',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    #dummy start task
    start = EmptyOperator(
        task_id='start',
        dag=dag,
    )

    AzureToBq = DataflowTemplatedJobStartOperator(
    task_id='dataflow_job_azuresql',
    template='gs://dataflow-templates-us-east4/latest/Jdbc_to_BigQuery',
    parameters={
        'driverClassName': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'driverJars' : 'gs://cdra_submitted/mssql-jdbc-12.2.0.jre11.jar',
        'connectionURL': f'jdbc:sqlserver://{os.getenv("SERVER")}:1433;database={os.getenv("DATABASE")};user={os.getenv("ACCOUNT")};password={os.getenv("PASSWORD")};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30',
        'query': 'select Participant_Id,Registry_Id,Case_Type,Quarter_Year_Text,Final_Xml_Name,Final_XML_Location_Text,Create_Date_Time from ( select *, ROW_NUMBER() OVER(PARTITION BY Participant_Id, Registry_Id, Case_Type, Quarter_Year_Text ORDER BY Create_Date_Time DESC) as rn from Sub.Participant_Submission PS1 where cast(PS1.Create_Date_Time as date) = cast(DATEADD(DAY, -18, GETDATE()) AS DATE) ) PS1 where rn = 1',
        'outputTable': 'hca-bpg-cdr-dev:CDRA_Staging.Participant_Submission_Record_Stage',
        'bigQueryLoadingTemporaryDirectory':'gs://cdra_submitted/BigQueryTemp/'
        },
    dataflow_default_options=dataflow_options,
    dag=dag
)

    LoadData_Base_AZ_GCS = DataflowTemplatedJobStartOperator(
        task_id='dataflow_job_az_gcs_base',
        template='gs://cdra_submitted/templates/az_gcs_base',

        dataflow_default_options=dataflow_options,
        dag=dag,
        wait_until_finished=True
    )
    LoadData_Base_GCS_BQ = DataflowTemplatedJobStartOperator(
        task_id='dataflow_job_gcs_bq_base',
        template='gs://cdra_submitted/templates/gcs_bq_base',
        dataflow_default_options=dataflow_options,
        dag=dag,
        wait_until_finished=True
    )
 
    # Dataflow job to process the followup data
    LoadData_Followup_AZ_GCS = DataflowTemplatedJobStartOperator(
        task_id='dataflow_job_az_gcs_followup',
        template='gs://cdra_submitted/templates/az_gcs_followup',

        dataflow_default_options=dataflow_options,
        dag=dag,
        wait_until_finished=True
    )
    LoadData_Followup_GCS_BQ = DataflowTemplatedJobStartOperator(
        task_id='dataflow_job_gcs_bq_followup',
        template='gs://cdra_submitted/templates/gcs_bq_followup',
        dataflow_default_options=dataflow_options,
        dag=dag,
        wait_until_finished=True
    )
            # Dummy end task
    LoadData_Stage_Core = EmptyOperator(
        task_id='LoadData_Stage_Core',
        dag=dag,
    )
    Load_Patient_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Patient.py',
       dag=dag
    )
    Load_Clinical_Adjudication_Event_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Clinical_Adjudication_Event.py',
       dag=dag
    )
    Load_Clinical_Adjudication_Event_Evaluation_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Clinical_Adjudication_Event_Evaluation.py',
       dag=dag
    )
    Load_Condition_History_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Condition_History.py',
       dag=dag
    )
    Load_Device_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Device.py',
       dag=dag
    )
    Load_Discharge_Summary_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Discharge_Summary.py',
       dag=dag
    )
    Load_Encounter_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Encounter.py',
       dag=dag
    )
    Load_Followup_Assessment_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Followup_Assessment.py',
       dag=dag
    )
    Load_Lab_Measurement_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Lab_Measurement.py',
       dag=dag
    )
    Load_Medication_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Medication.py',
       dag=dag
    )
    Load_Observation_History_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Observation_History.py',
       dag=dag
    )
    Load_Patient_Evaluation_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Patient_Evaluation.py',
       dag=dag
    )
    Load_Procedure_Event_List_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Procedure_Event_List.py',
       dag=dag
    )
    Load_Procedure_History_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Procedure_History.py',
       dag=dag
    )
    Load_Procedure_Info_Stage_Core = BashOperator(
       task_id='Load_Procedure_Info_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Procedure_Info.py',
       dag=dag
    )
    Load_Patient_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Patient.py',
       dag=dag
    )
    Load_Procedure_Occurrence_Stage_Core = BashOperator(
       task_id='Load_Data_Into_Submitted',
       bash_command='python /home/airflow/gcs/dags/scripts/sql_load/Procedure_Occurrence.py',
       dag=dag
    )
    
        # Dummy end task
    end = EmptyOperator(
        task_id='End',
        dag=dag,
    )    


# Setting up Task dependencies using Airflow standard notations        
start >> AzureToBq
AzureToBq >> [LoadData_Base_AZ_GCS,LoadData_Followup_AZ_GCS]
LoadData_Base_AZ_GCS >> LoadData_Base_GCS_BQ
LoadData_Followup_AZ_GCS >>  LoadData_Followup_GCS_BQ
[LoadData_Base_GCS_BQ , LoadData_Followup_GCS_BQ] >> LoadData_Stage_Core
LoadData_Stage_Core >> [Load_Patient_Stage_Core,Load_Clinical_Adjudication_Event_Stage_Core,Load_Clinical_Adjudication_Event_Evaluation_Stage_Core,Load_Condition_History_Stage_Core,Load_Device_Stage_Core,Load_Discharge_Summary_Stage_Core,Load_Encounter_Stage_Core,Load_Followup_Assessment_Stage_Core,Load_Lab_Measurement_Stage_Core,Load_Medication_Stage_Core,Load_Observation_History_Stage_Core,Load_Patient_Evaluation_Stage_Core,Load_Procedure_Event_List_Stage_Core,Load_Procedure_History_Stage_Core,Load_Procedure_Info_Stage_Core,Load_Patient_Stage_Core,Load_Procedure_Occurrence_Stage_Core]
Load_Patient_Stage_Core >> end
Load_Clinical_Adjudication_Event_Stage_Core >> end
Load_Clinical_Adjudication_Event_Evaluation_Stage_Core >> end
Load_Condition_History_Stage_Core >> end
Load_Device_Stage_Core >> end
Load_Discharge_Summary_Stage_Core >> end
Load_Encounter_Stage_Core >> end
Load_Followup_Assessment_Stage_Core >> end
Load_Lab_Measurement_Stage_Core >> end
Load_Medication_Stage_Core >> end
Load_Observation_History_Stage_Core >> end
Load_Patient_Evaluation_Stage_Core >> end
Load_Procedure_Event_List_Stage_Core >> end
Load_Procedure_History_Stage_Core >> end
Load_Procedure_Info_Stage_Core >> end
Load_Patient_Stage_Core >> end
Load_Procedure_Occurrence_Stage_Core >> end