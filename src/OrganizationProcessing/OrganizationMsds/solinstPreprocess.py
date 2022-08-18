import os

import awswrangler as wr
import pandas as pd

from OrganizationProcessing.org_processing_util import (
    add_sensor_metadata_columns,
    add_timezone_col,
    create_primary_key,
    get_solinst_level_unit,
    log_sheet_column_validation,
    process_log_times,
    read_excel_file_s3,
    return_sheet_val,
)

msds_staged_bucket_path = "mtsu-msds-data-lake-staged/source_multi_sensor_data_system"


def preprocessSolinstData(event, config):

    config = config["solinst"]

    obj_key = f"test/sensor-files/{event['file_name']}"

    # this should be passed in via the event argument
    source_object_key = f"s3://{os.environ['infra']}-mtsu-msds-data-lake-source/{obj_key}"

    level_unit = get_solinst_level_unit(obj_key)

    df = wr.s3.read_csv(
        source_object_key,
        skiprows=config["skip_rows"],
        names=config["column_names"],
        encoding=config["encoding"],
    )

    df["manufacturer_sensor"] = "solinst"

    df["level_unit"] = level_unit

    df = add_sensor_metadata_columns(event["file_name"], df)

    df = create_primary_key(df, config["data_primary_key_columns"], "sensor_data_key")

    df["date_time_et"] = pd.to_datetime(df["date"] + " " + df["time"])

    df = add_timezone_col(
        df,
        "date_time_et",
        "date_time_utc_tz",
        "US/Eastern",
    )

    # define processing object key
    staged_object_key = f"s3://{os.environ['infra']}-{msds_staged_bucket_path}/sensor_data_solinst/"

    # write to parquet
    wr.s3.to_parquet(
        df=df,
        path=staged_object_key,
        dtype={"unit_serial_number": "string"},
        dataset=True,
        database=f"{os.environ['infra']}_source_multi_sensor_data_system",
        table="sensor_data_solinst",
        mode="overwrite_partitions",
        partition_cols=["sensor_data_file_name"],
        compression="snappy",
    )

    return {"file_name": event["file_name"]}


def preprocessSolinstLogFile(event, config):

    config = config["solinst"]

    log_sheet_schema_name = f"sensor_log_solinst_schema_v{config['current_log_sheet_schema']}"
    log_sheet_time_config = f"sensor_log_time_v{config['current_log_sheet_schema']}"

    log_sheet_config = config[log_sheet_schema_name]

    s3_obj_key = f"test/log-sheet-files/{event['file_name']}"

    sheet = read_excel_file_s3(s3_obj_key)

    df = pd.DataFrame(columns=log_sheet_config.keys())

    for key in log_sheet_config:

        df.loc[0, key] = return_sheet_val(sheet, log_sheet_config, key)

    # add sensor config key
    df = create_primary_key(df, config["sensor_config_key_columns"], "sensor_config_key")

    # add sensor deployment key
    df = create_primary_key(df, config["sensor_deployment_key_columns"], "sensor_deployment_key")

    # add pre calibration key
    df = create_primary_key(
        df, config["pre_sensor_deployment_calibration_key_columns"], "pre_sensor_deployment_calibration_key"
    )

    # add post calibration key
    df = create_primary_key(
        df, config["post_sensor_deployment_calibration_key_columns"], "post_sensor_deployment_calibration_key"
    )

    df["log_sheet_file_name"] = event["file_name"]

    df["manufacturer_sensor"] = "solinst"

    staged_object_key = f"s3://{os.environ['infra']}-{msds_staged_bucket_path}/{log_sheet_schema_name}/"

    col_types = {}

    for col in df.columns:
        col_types[col] = "string"

    df = process_log_times(df, config[log_sheet_time_config])

    df_val = log_sheet_column_validation(df, config, log_sheet_schema_name)

    table_name = "sensor_log_sheet_column_validation"

    column_validation_table_path = f"s3://{os.environ['infra']}-{msds_staged_bucket_path}/{table_name}/"

    df_val["log_sheet_file_name"] = event["file_name"]

    wr.s3.to_parquet(
        df=df_val,
        path=column_validation_table_path,
        dataset=True,
        database=f"{os.environ['infra']}_source_multi_sensor_data_system",
        table=table_name,
        mode="overwrite_partitions",
        partition_cols=["log_sheet_file_name"],
        compression="snappy",
    )

    wr.s3.to_parquet(
        df=df,
        path=staged_object_key,
        dtype=col_types,
        dataset=True,
        database=f"{os.environ['infra']}_source_multi_sensor_data_system",
        table=log_sheet_schema_name,
        mode="overwrite_partitions",
        partition_cols=["log_sheet_file_name"],
        compression="snappy",
    )

    return {"file_name": event["file_name"]}
