CREATE TABLE table_data AS
SELECT dag_id, execution_date FROM dag_run
ORDER BY execution_date;
