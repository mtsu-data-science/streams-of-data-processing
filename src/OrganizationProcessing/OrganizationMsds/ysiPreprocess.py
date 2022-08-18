import os

import awswrangler as wr
import pandas as pd

from OrganizationProcessing.org_processing_util import (
    add_sensor_metadata_columns,
    add_timezone_col,
    create_primary_key,
    log_sheet_column_validation,
    process_log_times,
    read_excel_file_s3,
    return_sheet_val,
)

msds_staged_bucket_path = "mtsu-msds-data-lake-staged/source_multi_sensor_data_system"


def preprocessYsiData(event, config):
    """Lambda function for pre-processing YSI excel files.
    Config for this can be found at `pre-processing-config/pre-processing-ysi.json`
    """

    config = config["ysi"]

    # this should be passed in via the event argument
    source_object_key = f"s3://{os.environ['infra']}-mtsu-msds-data-lake-source/sensor-files/{event['file_name']}"

    df = wr.s3.read_excel(
        source_object_key, skiprows=config["skip_rows"], names=config["column_names"], dtype=config["dtypes"]
    )

    table_name = "sensor_data_ysi"

    df = add_sensor_metadata_columns(event["file_name"], df)

    df["date_time"] = pd.to_datetime(df["date_time"])

    # add data primary key
    df = add_timezone_col(
        df,
        "date_time",
        "date_time_utc_tz",
        "US/Eastern",
    )

    df = create_primary_key(df, config["data_primary_key_columns"], "sensor_data_key")

    df["manufacturer_sensor"] = "ysi"

    # define staged object key
    staged_object_key = f"s3://{os.environ['infra']}-{msds_staged_bucket_path}/{table_name}/"

    # write to parquet
    wr.s3.to_parquet(
        df=df,
        path=staged_object_key,
        dtype={"unit_serial_number": "string"},
        dataset=True,
        database=f"{os.environ['infra']}_source_multi_sensor_data_system",
        table=table_name,
        mode="overwrite_partitions",
        partition_cols=["sensor_data_file_name"],
        compression="snappy",
    )

    return {"file_name": event["file_name"]}


def preprocessYsiLogFile(event, config):

    config = config["ysi"]

    # this mechanism checks to see if the log sheet being
    # processed is associated with a non-current schema
    # if so, it will instead return the associated schema instead of the
    # current schema
    if event["file_name"] in config["log_sheet_files_old"].keys():
        log_sheet_schema_name = f"sensor_log_ysi_schema_v{config['log_sheet_files_old'][event['file_name']]}"
        cal_log_sheet_schema_name = (
            f"sensor_calibration_ysi_schema_v{config['log_sheet_files_old'][event['file_name']]}"
        )
        log_sheet_time_config = f"sensor_log_time_v{config['log_sheet_files_old'][event['file_name']]}"
    else:
        log_sheet_schema_name = f"sensor_log_ysi_schema_v{config['current_log_sheet_schema']}"
        cal_log_sheet_schema_name = f"sensor_calibration_ysi_schema_v{config['current_log_sheet_schema']}"
        log_sheet_time_config = f"sensor_log_time_v{config['current_log_sheet_schema']}"

    log_sheet_config = config[log_sheet_schema_name]

    s3_object_key = f"log-sheet-files/{event['file_name']}"

    sheet = read_excel_file_s3(s3_object_key)

    df_log = pd.DataFrame(columns=log_sheet_config.keys())

    for key in log_sheet_config:
        df_log.loc[0, key] = return_sheet_val(sheet, log_sheet_config, key)

    # add sensor config key
    df_log = create_primary_key(df_log, config["sensor_config_key_columns"], "sensor_config_key")

    # add sensor deployment key
    df_log = create_primary_key(df_log, config["sensor_deployment_key_columns"], "sensor_deployment_key")

    # add pre calibration key
    df_log = create_primary_key(
        df_log, config["pre_sensor_deployment_calibration_key_columns"], "pre_sensor_deployment_calibration_key"
    )

    # add post calibration key
    df_log = create_primary_key(
        df_log, config["post_sensor_deployment_calibration_key_columns"], "post_sensor_deployment_calibration_key"
    )

    df_log["log_sheet_file_name"] = event["file_name"]

    df_log["manufacturer_sensor"] = "ysi"

    staged_object_key = f"s3://{os.environ['infra']}-{msds_staged_bucket_path}/{log_sheet_schema_name}/"

    col_types = {}

    for col in df_log.columns:
        col_types[col] = "string"

    df_log = process_log_times(df_log, config[log_sheet_time_config])

    # create calibration file
    # will use the same excel file but using a different log sheet schema
    cal_log_sheet_config = config[cal_log_sheet_schema_name]

    df_cal = pd.DataFrame(
        columns=[
            "sensor_deployment_calibration_key",
            "sensor_deployment_calibration_data_name",
            "parameter",
            "actual",
        ]
    )

    index = 0

    for parameter in cal_log_sheet_config:

        if parameter[:3] == "pre":
            sensor_deployment_calibration_key = df_log.loc[0, "pre_sensor_deployment_calibration_key"]
        elif parameter[:4] == "post":
            sensor_deployment_calibration_key = df_log.loc[0, "post_sensor_deployment_calibration_key"]
        else:
            raise

        parameter_set = cal_log_sheet_config[parameter]

        df_cal.loc[index, "sensor_deployment_calibration_key"] = sensor_deployment_calibration_key
        df_cal.loc[index, "sensor_deployment_calibration_data_name"] = parameter

        df_cal.loc[index, "parameter"] = parameter_set["parameter"]
        df_cal.loc[index, "actual"] = return_sheet_val(
            sheet,
            cal_log_sheet_config,
            parameter,
            "actual",
        )

        index = index + 1

    df_cal["log_sheet_file_name"] = event["file_name"]

    df_cal = create_primary_key(
        df_cal,
        ["sensor_deployment_calibration_key", "sensor_deployment_calibration_data_name"],
        "sensor_deployment_calibration_data_key",
    )

    calibration_table_s3_path = f"s3://{os.environ['infra']}-{msds_staged_bucket_path}/{cal_log_sheet_schema_name}/"

    df_val = log_sheet_column_validation(df_log, config, log_sheet_schema_name)

    df_val["log_sheet_file_name"] = event["file_name"]

    table_name = "sensor_log_sheet_column_validation"

    column_validation_table_path = f"s3://{os.environ['infra']}-{msds_staged_bucket_path}/{table_name}/"

    wr.s3.to_parquet(
        df=df_cal,
        path=calibration_table_s3_path,
        dataset=True,
        database=f"{os.environ['infra']}_source_multi_sensor_data_system",
        table=cal_log_sheet_schema_name,
        mode="overwrite_partitions",
        partition_cols=["log_sheet_file_name"],
        compression="snappy",
    )

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
        df=df_log,
        path=staged_object_key,
        dtype=col_types,
        dataset=True,
        database=f"{os.environ['infra']}_source_multi_sensor_data_system",
        table=log_sheet_schema_name,
        mode="overwrite_partitions",
        partition_cols=["log_sheet_file_name"],
    )

    return {"file_name": event["file_name"]}
