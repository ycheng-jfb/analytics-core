CREATE TABLE IF NOT EXISTS work.dbo.maintenance_test
(
    dag_id STRING,
    comment STRING,
    meta_create_datetime TIMESTAMP_LTZ default current_timestamp(3)
);

INSERT INTO work.dbo.maintenance_test (dag_id, comment) VALUES ('duplo_test_maintenance_test_dag_runs_dag', current_timestamp);
