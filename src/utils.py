import hashlib
import os
from io import BytesIO
from typing import Dict, List, Union

import awswrangler as wr
import boto3
import pandas as pd
import pytz
from openpyxl import load_workbook, worksheet
from pandas import DataFrame, Series


def read_excel_file_s3(s3_obj_key: str) -> worksheet:
    """
    Read in an excel file from s3

    Args:
        s3_obj_key (str): The s3 object key for the log sheet file

    Returns:
        sheet: Processed worksheet
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=f"{os.environ['infra']}-mtsu-msds-data-lake-source", Key=s3_obj_key)
    binary_data = obj["Body"].read()
    wb = load_workbook(BytesIO(binary_data), data_only=True)
    # returns the first worksheet
    return wb[wb.worksheets[0].title]


def load_text_file(s3_obj_key: str, decode_type: str = None) -> List:
    """
    load_text_file This takes in an object key and returns the
    a byte object. I am not decoding

    Args:
        s3_obj_key (str): S3 Object Key

    Returns:
        List: A list of the text file split on \n
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=f"{os.environ['infra']}-mtsu-msds-data-lake-source", Key=s3_obj_key)
    obj_read = obj["Body"].read()

    if decode_type is None:
        text_list = obj_read.decode("utf-8").split("\n")
    else:
        text_list = obj_read.decode(decode_type).split("\n")

    return text_list


def get_solinst_level_unit(s3_obj_key: str) -> str:
    """
    get_solinst_level_unit This parses the object file to get the unit
    for a file.

    Args:
        s3_obj_key (str): S3 Object Key

    Returns:
        str: Returns the unit
    """

    text_list = load_text_file(s3_obj_key, decode_type="cp1252")

    index = 0
    for item in text_list:
        if "LEVEL" in item:
            unit = text_list[index + 1].split(":")[1].strip().strip(",")
            break
        else:
            index = index + 1

    return unit


def add_sensor_metadata_columns(file_name: str, df: DataFrame) -> DataFrame:
    """
    add_sensor_metadata_columns queries athena to get a sensor files
    information.

    Args:
        file_name (String): The name of the file for the sensor deployment file

    Returns:
        Series: Returns a pandas series
    """

    df_athena = wr.athena.read_sql_query(
        f"""
    select
        d.sensor_data_file_name,
        d.site_name,
        d.station_name,
        c.unit_name,
        cast(d.deploy_date_time_utc as varchar) as deploy_date,
        c.unit_serial_number,
        d.sensor_config_key,
        d.sensor_deployment_key
    from dw_sensor_deployment as d
    left join dw_sensor_config as c
    on d.sensor_config_key = c.sensor_config_key
    where sensor_data_file_name like '{file_name}'
    """,
        database=f"{os.environ['infra']}_multi_sensor_data_system",
        s3_output=f"s3://{os.environ['infra']}-mtsu-data-science-athena-query-result/",
    )

    if df_athena.shape[0] == 0:
        raise Exception(f"Metadata Columns empty: {file_name} | DF Shape: {df_athena.shape}")

    df["sensor_data_file_name"] = df_athena.loc[0, "sensor_data_file_name"]
    df["site_name"] = df_athena.loc[0, "site_name"]
    df["station_name"] = df_athena.loc[0, "station_name"]
    df["unit_name"] = df_athena.loc[0, "unit_name"]
    df["deploy_date"] = df_athena.loc[0, "deploy_date"]
    df["unit_serial_number"] = df_athena.loc[0, "unit_serial_number"]
    df["sensor_config_key"] = df_athena.loc[0, "sensor_config_key"]
    df["sensor_deployment_key"] = df_athena.loc[0, "sensor_deployment_key"]

    return df


def hash_column(row: Series, column_key_list: List) -> str:
    """
    hash_column uses hashlib to create an md5 checksum of the string
    to create a unique key

    Args:
        row (Series): The row in the pandas dataframe
        column_key_list (List): A list of columns to combine together

    Returns:
        Series: Returns a pandas series
    """

    str2hash = ""
    for col in column_key_list:
        str2hash = str2hash + str(row[col])
    result = hashlib.md5(str2hash.encode())
    return result.hexdigest()


def create_primary_key(df: DataFrame, column_key_list: List, primary_key_name: str) -> DataFrame:
    """Uses the pandas hash_pandas_object

    Args:
        df (DataFrame): A DataFrame for adding a primary key to
        column_key_list (List): Columns to combine into a primary key
        primary_key_name (str): The name of the primary key

    Returns:
        DataFrame: A DataFrame that has a primary key column added
    """

    df[primary_key_name] = df.apply(lambda row: hash_column(row, column_key_list), axis=1)

    return df


def add_timezone_col(df: DataFrame, timestamp_col: str, new_timestamp_col: str, timezone: str) -> DataFrame:
    """
    add_timezone_to_col Replaces the timestamp column with a column with the appropriate timezone.
    In Athena this will convert it to UTC but that is okay.

    Args:
        df (DataFrame): The dataframe with the timestamp column
        timestamp_col (str): The column name
        timezone (str): The timezone for the column such as US/Eastern or UTC
        new_timestamp_col (str): The name of the timestamp column to be added

    Returns:
        DataFrame: The dataframe with the adjusted timezone column
    """
    tz = pytz.timezone(timezone)

    df["temp_utc_col"] = pd.to_datetime(df[timestamp_col]) - tz.utcoffset(df.loc[0, timestamp_col])

    df[new_timestamp_col] = df["temp_utc_col"].dt.tz_localize("UTC")

    df = df.drop(columns=["temp_utc_col"])

    return df


def process_log_times(df: DataFrame, time_config: Dict) -> DataFrame:
    """
    process_log_times This adds time zone based on the time_config in the config file.

    Args:
        df (DataFrame): The log sheet dataframe
        time_config (Dict): A dictionary of the time config.

    Returns:
        DataFrame: A DataFrame with the columns added.
    """

    for column in time_config.keys():

        time_col_info = time_config[column]

        temp_time_col = f"{column}_tmp"

        if time_col_info["type"] == "sep":
            date_col = time_config[column]["date"]
            time_col = time_config[column]["time"]
            time_zone = time_config[column]["zone"]

            # set up temp datetime column
            if df.loc[0, date_col] is not None and df.loc[0, time_col] is not None:
                df[temp_time_col] = pd.to_datetime(df[date_col][0][:10] + " " + df[time_col][0][-8:])
                # add time zone
                df[column] = pd.to_datetime(df[temp_time_col]).dt.tz_localize(time_zone)
            else:
                df[temp_time_col] = pd.to_datetime("1900-01-05 00:00:00")
                df[column] = pd.to_datetime(df[temp_time_col]).dt.tz_localize(time_zone)

        elif time_col_info["type"] == "datetime":

            datetime_col = time_config[column]["datetime"]
            time_zone = time_config[column]["zone"]

            if df.loc[0, datetime_col] is not None:
                df[temp_time_col] = pd.to_datetime(df[datetime_col])
                df[column] = pd.to_datetime(df[temp_time_col]).dt.tz_localize(time_zone)
            else:
                df[temp_time_col] = pd.to_datetime("1900-01-05 00:00:00")
                df[column] = pd.to_datetime(df[temp_time_col]).dt.tz_localize(time_zone)

        df = df.drop(columns=[temp_time_col])

    return df


def return_sheet_val(sheet: worksheet, config: dict, param: str, index: str = None) -> Union[str, None]:
    """
    This function streamlines pulling values out of an excel file and handles converting None or NA into
    None so this translates into a null in the database.

    Args:
        sheet (worksheet): The excel worksheet
        config (dict): A dict that includes the sheet configuration
        param (str): The param being pulled from the config
        index (str): This is used for instances where there is an additional config for getting a value
    Returns:
        str: A excel from an excel column
    """

    if index is None:
        sheet_val = str(sheet[config[param]].value)
    else:
        sheet_val = str(sheet[config[param][index]].value)

    # removing all spaces and lowering text
    val_check = "".join(sheet_val.lower().split())

    if val_check in ["na", "none", ""]:
        return None
    else:
        return sheet_val


def log_sheet_column_validation(df_log: DataFrame, config: Dict, log_sheet_schema_name: str) -> DataFrame:
    """
    log_sheet_column_validation Will take in a log dataframe and it will evaluate based on the config
    what columns are populated or are missing.

    Args:
        df_log (DataFrame): The log dataframe
        config (Dict): The config for the sensor log files
        log_sheet_schema_name (str): The log schema name

    Returns:
        DataFrame: The log sheet validation dataframe
    """

    df_val = pd.DataFrame(
        columns=[
            "column_name",
            "column_group",
            "status",
        ]
    )

    index = 0

    deployment_info_valid = config["validation_check"]["deployment_info"]
    calibration_check_valid = config["validation_check"]["calibration_check"]
    station_info_valid = config["validation_check"]["station_info"]

    validation_columns = deployment_info_valid + calibration_check_valid + station_info_valid

    for col in config[log_sheet_schema_name].keys():

        if col in validation_columns:

            df_val.loc[index, "column_name"] = col

            if col in deployment_info_valid:
                df_val.loc[index, "column_group"] = "deployment_info"
            elif col in calibration_check_valid:
                df_val.loc[index, "column_group"] = "calibration_check"
            elif col in station_info_valid:
                df_val.loc[index, "column_group"] = "station_info"

            if df_log.loc[0, col] is not None:
                df_val.loc[index, "status"] = "populated"
            else:
                df_val.loc[index, "status"] = "missing"

            index = index + 1

    return df_val
