INSERT INTO dataingestion_workflow (user_id, wf_id, wf_desc, wf_name, active, src_type_id, src_sub_type_id, trg_type_id, trg_sub_type_id, src_key, edit_action, trg_key, src_metadata, trg_metadata, created_ts, updated_ts, workflow_metadata, user_database_name, data_flow_props, business_unit, template_id, df_template_id, file_push_data, version) VALUES (1, '235', 'test_oracle', 'test_oracle', 'True', 1, 1, 1, 1, 'projects/641772187113/secrets/cndr-prod-oracle-db', '{}', 'dm-condor-dev', '{"tbls_info":[{"Rows":null,"Table":"#T17_sid:287468553_4_FiltFilte","Average_row_size":"None"},{"Rows":6,"Table":"A","Average_row_size":"40960"},{"Rows":0,"Table":"AA2","Average_row_size":"0"}]}', '{}', '2025-06-04 11:50:04.304264', '2025-06-04 11:50:04.307032', '{"etl_steps":[]}', 'DM_PRODUCT', '{"region":"us-central1","network":"dm-primary-network","project":"dm-condor-dev","bq_dataset":"condor_dev","bq_project":"dm-condor-dev","subnetwork":"https://www.googleapis.com/compute/v1/projects/dm-network-host-project/regions/us-central1/subnetworks/subnet-dm-delivery-us-central1","code_bucket":"condor_dev_gcs","data_bucket":"condor_dev_gcs","num_workers":"1","temp_location":"gs://condor_dev_gcs/temp/","airflow_project":"dmgcp-del-composer","max_num_workers":"2","service_account":"sa-dataflow@dm-condor-dev.iam.gserviceaccount.com","staging_location":"gs://condor_dev_gcs/temp/","template_location":"gs://condor_dev_gcs/onix_condor_core_flex_template_v1.json","etl_dataset_region":"US","airflow_bucket_name":"dm-del-composer-bucket","worker_machine_type":"n1-standard-4","gcs_glossary_location":"condor_dev_gcs/condor_glossary_files/etlTest.json","gcs_dag_gen_folder_path":"dags/DM_CONDOR_DAGS/","scd_stored_procs_dataset":"condor_scd_procs","template_file_gcs_location":"gs://condor_dev_gcs/onix_condor_core_flex_template_v1.json","etl_processing_sqls_location":"gs://dm-del-composer-bucket/data/CONDOR_ETL_PROCESSING"}', NULL, 18, 1, NULL, NULL);INSERT INTO dataingestion_workflowjobtables (user_id, wf_id, job_id, col_id, job_name, job_tags, src_cust_schema, src_tbl_name, src_tbl_active, src_col_name, src_col_type, incr_cols, trg_tbl_name, target_col_name, target_col_type, target_col_active, dt_rules, tranformation_rules, is_scheduled, created_ts, updated_ts, action, split_col, clause, table_rules, split_col_format, incr_col_format, no_of_split, policy_tag, fetch_size, generated_col, sql_metadata, custom_dataflow_props, version, is_imported, raven_dag_name) VALUES (1, '235', '156', '942', 'cndr_1_AA2_128_89', NULL, 'DM_PRODUCT', 'AA2', 'True', 'NUM1', 'NUMBER', 'False', 'AA2_test', 'NUM1', 'NUMERIC', 'True', NULL, NULL, 'False', '2025-06-05 06:17:04.461681', '2025-06-05 06:20:57.118453', 'add', 'False', '', '{"executionSteps":[{}],"use_incremental":false}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'False', NULL);INSERT INTO dataingestion_workflowschedule (user_id, sch_id, wf_id, job_id, table_name, type, days, time, sch_json, dag_file_name, active, create_ts, upd_ts, create_usr, sch_date, group_name, dag_location, dag_code, version) VALUES (1, '83', '235', '156', 'cndr_1_AA2_235_156', 'weekly', '', '11:51', '{"wf_id":"128","job_id":"89","frequency":[{"date":"2025-06-05","days":"","time":"11:51","type":"weekly","sch_id":"","intervals":"","operation":"edit"}],"table_name":"cndr_1_AA2_128_89"}', NULL, 'True', '2025-06-05 06:21:46.403237', NULL, NULL, '2025-06-05', NULL, 'github/condor_dev/CNDR_1_AA2_128_89', '
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
        gcp_conn_id=''google_cloud_default'',
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
    ''owner'': ''airflow'',
    ''depends_on_past'': False,
}

# Timezone
local_tz = pendulum.timezone(''Asia/Kolkata'')

# DAG Definition
dag = DAG(
    dag_id=''CNDR_1_AA2_128_89'',
    default_args=default_dag_args,
    start_date=datetime(
        2025, 6, 5, 11, 51, tzinfo=local_tz
    ),
    catchup=False,
    schedule_interval=''@weekly''
)

# Dataflow Flex Template Operator
DIF_INGESTION = StartFlexTemplateCustomOperator(
    task_id = ''CNDR_1_AA2_128_89_BASH_TASK'',
    location = ''us-central1'',
    project_id = ''dm-condor-dev'',
    job_name = "cndr-1-aa2-128-89-1151-w",
    container_spec_gcs_path = ''gs://condor_dev_gcs/onix_condor_core_flex_template_v1.json'',
    parameters = {
             "project": "dm-condor-dev",
             "task_prop_table": "dm-condor-dev.condor_dev.task_prop_table",
             "bq_project": "dm-condor-dev",
             "task_id": "CNDR_1_AA2_128_89",
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

            ', NULL);