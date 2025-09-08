
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import BaseOperator
from datetime import datetime
from airflow import DAG
import pendulum

# Custom Operator Definition
class StartFlexTemplateCustomOperator(BaseOperator):
    def __init__(
        self,
        task_id,
        location,
        project_id,
        job_name,
        container_spec_gcs_path,
        parameters,
        environment,
        gcp_conn_id='google_cloud_default',
        *args, **kwargs
    ):
        super().__init__(task_id=task_id, *args, **kwargs)
        self.location = location
        self.project_id = project_id
        self.job_name = job_name
        self.container_spec_gcs_path = container_spec_gcs_path
        self.parameters = parameters
        self.environment = environment
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = DataflowHook(gcp_conn_id=self.gcp_conn_id)
        body = {
            "launchParameter": {
                "jobName": self.job_name,
                "containerSpecGcsPath": self.container_spec_gcs_path,
                "parameters": self.parameters,
                "environment": self.environment
            }
        }
        hook.start_flex_template(body=body, location=self.location, project_id=self.project_id)


# Default arguments for the DAG
default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

# Timezone
local_tz = pendulum.timezone('Asia/Kolkata')

# DAG Definition
dag = DAG(
    dag_id='CNDR_1_EMPLOYEE_21_1',
    default_args=default_dag_args,
    start_date=datetime(
        2025, 7, 20, 12, 40, tzinfo=local_tz
    ),
    catchup=False,
    schedule_interval='@monthly'
)

# Dataflow Flex Template Operator
DIF_INGESTION = StartFlexTemplateCustomOperator(
    task_id = 'CNDR_{{user_id}}_EMPLOYEE_{{wf_id}}_{{job_id}}_BASH_TASK',
    location = 'us-central1',
    project_id = 'dm-condor-dev',
    job_name = "cndr-{{user_id}}-employee-{{wf_id}}-{{job_id}}-1240-m",
    container_spec_gcs_path = 'gs://condor_dev_gcs/onix_condor_core_flex_template_v1.json',
    parameters = {
             "project": "dm-condor-dev",
             "task_prop_table": "dm-condor-dev.condor_dev.task_prop_table",
             "bq_project": "dm-condor-dev",
             "task_id": "CNDR_1_EMPLOYEE_21_1",
    },
    environment = {
                "tempLocation": "gs://condor_dev_gcs/temp/",
                "stagingLocation": "gs://condor_dev_gcs/temp/",
                "network": "dm-primary-network",
                "subnetwork": "https://www.googleapis.com/compute/v1/projects/dm-network-host-project/regions/us-central1/subnetworks/subnet-dm-delivery-us-central1",
                "serviceAccountEmail": "sa-dataflow@dm-condor-dev.iam.gserviceaccount.com",
                "additionalExperiments": ["use_runner_v2"],
                "workerRegion": "us-central1",
                "ipConfiguration": "WORKER_IP_PRIVATE",
                "numWorkers": "1",
                "machineType":"n1-standard-4",
                "maxWorkers":"2"
          },
    dag=dag
)

            