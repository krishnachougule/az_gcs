from google.cloud import bigquery

def read_query_from_file(filename):
    with open(filename, "r") as file:
        return file.read()

def execute_queries():
        # Set up the BigQuery client
        client = bigquery.Client()

        # Read the queries from .sql files

        Patient = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Patient.sql"),
        Clinical_Adjudication_Event = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Clinical_Adjudication_Event.sql")
        Clinical_Adjudication_Event_Evaluation = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Clinical_Adjudication_Event_Evaluation.sql")
        Condition_History  = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Condition_History.sql")
        Device = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Device.sql")
        Discharge_Summary = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Discharge_Summary.sql")
        Encounter = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Encounter.sql")
        Followup_Assessment = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Follow_Up_Assessment.sql")
        Lab_Measurement = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Lab_Measurement.sql")
        Medication = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Medication.sql")
        Observation_History = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Observation_History.sql")
        Patient_Evaluation = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Patient_Evaluation.sql")
        Procedure_Event_List = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Procedure_Event_List.sql")
        Procedure_History = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Procedure_History.sql")
        Procedure_Info = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Procedure_Info.sql")
        Procedure_Occurrence = read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Procedure_Occurrence.sql")
        
        # creates a query job for the Bigquery
        Patient_query_job= client.query(Patient)
        Clinical_Adjudication_Event_query_job = client.query(Clinical_Adjudication_Event)
        Clinical_Adjudication_Event_Evaluation_query_job = client.query(Clinical_Adjudication_Event_Evaluation)
        Condition_History_query_job = client.query(Condition_History)
        Device_query_job = client.query(Device)
        Discharge_Summary_query_job = client.query(Discharge_Summary)
        Encounterquery_job = client.query(Encounter)
        Followup_Assessment_query_job = client.query(Followup_Assessment)
        Lab_Measurement_query_job = client.query(Lab_Measurement)
        Medication_query_job = client.query(Medication)
        Observation_History_query_job = client.query(Observation_History)
        Patient_Evaluation_query_job = client.query(Patient_Evaluation)
        Procedure_Event_List_query_job = client.query(Procedure_Event_List)
        Procedure_History_query_job = client.query(Procedure_History)
        Procedure_Info_query_job = client.query(Procedure_Info)
        Procedure_Occurrence_query_job = client.query(Procedure_Occurrence)

        #waits for the query job to finish and returns the results of the query
        Patient_query_job.result()
        Clinical_Adjudication_Event_query_job.result()
        Clinical_Adjudication_Event_Evaluation_query_job.result()
        Condition_History_query_job.result()
        Device_query_job.result()
        Discharge_Summary_query_job.result()
        Encounterquery_job.result()
        Followup_Assessment_query_job.result()
        Lab_Measurement_query_job.result()
        Medication_query_job.result()
        Observation_History_query_job.result()
        Patient_Evaluation_query_job.result()
        Procedure_Event_List_query_job.result()
        Procedure_History_query_job.result()
        Procedure_Info_query_job.result()
        Procedure_Occurrence_query_job.result()
if __name__ == '__main__':  
   execute_queries()
   
