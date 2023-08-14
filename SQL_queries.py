from google.cloud import bigquery

def read_query_from_file(filename):
    with open(filename, "r") as file:
        return file.read()

def execute_queries():
        # Set up the BigQuery client
        client = bigquery.Client()

        # Read the queries from .sql files
        queries = [
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Patient.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Clinical_Adjudication_Event.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Clinical_Adjudication_Event_Evaluation.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Condition_History.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Device.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Discharge_Summary.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Encounter.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Follow_Up_Assessment.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Lab_Measurement.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Medication.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Observation_History.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Patient_Evaluation.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Procedure_Event_List.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Procedure_History.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Procedure_Info.sql"),
            read_query_from_file("/home/airflow/gcs/dags/scripts/queries/Procedure_Occurrence.sql")
        ]

        # Iterate over the queries and run them
        for query in queries:
            try:
                query_job = client.query(query)
                query_job.result()
            except Exception as e:
                print(f"Error running query: {e}")
                print(f"Query name: {query.split('`')[1]}")

if __name__ == '__main__':  
   execute_queries()
