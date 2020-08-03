import snowflake.connector as sf
from LoadResultsToSnowflake import config


def est_connection():
    connection = sf.connect(
        user=config.user,
        password=config.password,
        account=config.account,
        role=config.role)
    return connection


def sql_commands(conn):
    conn.cursor().execute("CREATE DATABASE IF NOT EXISTS RAW_DATA")
    conn.cursor().execute("USE DATABASE RAW_DATA")
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS RAW")
    conn.cursor().execute("USE SCHEMA RAW")

    sql_integration_object = """create or replace storage integration gcs_int 
                                type = external_stage
                                storage_provider = gcs
                                enabled = true
                                storage_allowed_locations = ('gcs://ioki_datasets/');"""
    conn.cursor().execute(sql_integration_object)

    file_format = """create or replace file format my_csv_format
                     type=csv field_delimiter=',' skip_header = 1 null_if =('NULL', 'null')
                     empty_field_as_null=true
                     VALIDATE_UTF8=FALSE"""
    conn.cursor().execute(file_format)

    stage_location = """create or replace stage loading_stage
                                url = 'gcs://ioki_datasets/test_average_scores.csv'
                                storage_integration = gcs_int
                                file_format = my_csv_format;"""
    conn.cursor().execute(stage_location)

    sql_test_level = """create or replace table test_average_scores (
                           class_id INTEGER,
                           class_name STRING,
                           teaching_hours STRING,
                           test_created_at STRING,
                           test_authorized_at STRING,
                           avg_class_test_overall_score NUMERIC);"""
    conn.cursor().execute(sql_test_level)

    sql_copy_data = """copy into RAW.test_average_scores
                            from (select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6 from @stage_location/ t)
                            on_error = 'CONTINUE';"""
    conn.cursor().execute(sql_copy_data)


try:
    conn = est_connection()
    sql_commands(conn)
finally:
    conn = est_connection()
    conn.cursor().close()
