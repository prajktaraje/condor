-- Workflow
INSERT INTO {{PG_SCHEMA}}.dataingestion_workflow
(user_id, wf_id, wf_desc, wf_name, active, src_type_id, src_sub_type_id, trg_type_id, trg_sub_type_id, src_key, edit_action, trg_key, src_metadata, trg_metadata, created_ts, updated_ts, workflow_metadata, user_database_name, data_flow_props, business_unit, template_id, df_template_id, file_push_data, version)
VALUES
({{user_id}}, {{wf_id}}, 'rdbms_bq_test', 'rdbms_bq_test', TRUE, 1, 2, 1, 1, 'projects/641772187113/secrets/cndr-psql_test', '{}', 'dm-condor-dev', '{"tbls_info": [{"Rows": 3, "Table": "employee", "Average_row_size": "40960"}]}'::json, '{}'::json, {{CREATED_TS}}, {{UPDATED_TS}}, '{"etl_steps": []}'::json, 'public', '{"region": "{{LOCATION}}", "network": "dm-primary-network", "project": "dm-condor-dev", "bq_dataset": "{{DATASET_ID}}", "bq_project": "dm-condor-dev"}'::json, NULL, 23, 1, NULL, NULL);

-- Workflow job table (example for department column)
INSERT INTO {{PG_SCHEMA}}.dataingestion_workflowjobtables
(user_id, wf_id, job_id, col_id, job_name, job_tags, src_cust_schema, src_tbl_name, src_tbl_active, src_col_name, src_col_type, incr_cols, trg_tbl_name, target_col_name, target_col_type, target_col_active, dt_rules, tranformation_rules, is_scheduled, created_ts, updated_ts, action, split_col, clause, table_rules, split_col_format, incr_col_format, no_of_split, policy_tag, fetch_size, generated_col, sql_metadata, custom_dataflow_props, version, is_imported, raven_dag_name)
VALUES
({{user_id}}, {{wf_id}}, {{job_id}}, {{col_id}}, 'cndr_{{user_id}}_employee_{{wf_id}}_{{job_id}}', NULL, 'public', 'employee', TRUE, 'department', 'character varying', FALSE, 'employee_testv1', 'department', 'STRING', TRUE, NULL, NULL, FALSE, {{CREATED_TS}}, {{UPDATED_TS}}, 'add', FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{"executionSteps": [{}], "use_incremental": FALSE}'::json, NULL, NULL, FALSE, NULL);

-- Repeat similar INSERTs for email, employee_id, first_name, hire_date, job_title, last_name, phone_number, salary
-- Just replace src_col_name and target_col_name accordingly

-- Workflow schedule
INSERT INTO {{PG_SCHEMA}}.dataingestion_workflowschedule
(user_id, sch_id, wf_id, job_id, table_name, type, days, time, sch_json, dag_file_name, active, create_ts, upd_ts, create_usr, sch_date, group_name, dag_location, dag_code, version)
VALUES
({{user_id}}, {{sch_id}}, {{wf_id}}, {{job_id}}, 'cndr_{{user_id}}_employee_{{wf_id}}_{{job_id}}', 'monthly', '', '12:40', '{"wf_id": "{{wf_id}}", "job_id": "{{job_id}}", "frequency": [{"date": "2025-07-20", "days": "", "time": "12:40", "type": "monthly", "sch_id": "", "intervals": "", "operation": "edit"}], "table_name": "cndr_{{user_id}}_employee_{{wf_id}}_{{job_id}}"}'::json, NULL, TRUE, {{CREATED_TS}}, {{UPDATED_TS}}, NULL, '2025-07-20', NULL, 'github/condor/CNDR_1_EMPLOYEE_21_1', '{}'::json, NULL);
