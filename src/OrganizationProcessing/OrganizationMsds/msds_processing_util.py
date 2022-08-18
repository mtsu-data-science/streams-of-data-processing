import os
from pathlib import Path

import awswrangler as wr

from OrganizationProcessing.OrganizationMsds.minidotPreprocess import preprocessMinidotData, preprocessMinidotLogFile
from OrganizationProcessing.OrganizationMsds.solinstPreprocess import preprocessSolinstData, preprocessSolinstLogFile
from OrganizationProcessing.OrganizationMsds.ysiPreprocess import preprocessYsiData, preprocessYsiLogFile


def get_file_names(file_list):
    # this will get the name and pull out the name
    # regardless of how many extensions there are like name.snappy.parquet
    return [Path(x).name for x in file_list]


def return_unprocessed_files(source_list, staged_list):
    if staged_list != []:
        return list(set(source_list).difference(staged_list))
    else:
        return []


def get_full_file_name(file):
    file_list = wr.s3.list_objects(f"s3://{os.environ['infra']}-mtsu-msds-data-lake-source/*/{file}*")

    if len(file_list) == 1:
        return Path(file_list[0]).name
    else:
        print("Multiple files match this name")
        raise


def get_sensor_type(file_name):
    query = f"""
    with sensor_data as (
    select sensor_data_file_name, manufacturer_sensor
    from sensor_log_minidot_schema_v1
    union
    select sensor_data_file_name, manufacturer_sensor
    from sensor_log_minidot_schema_v2
    union
    select sensor_data_file_name, manufacturer_sensor
    from sensor_log_ysi_schema_v1
    union
    select sensor_data_file_name, manufacturer_sensor
    from sensor_log_ysi_schema_v2
    union
    select sensor_data_file_name, manufacturer_sensor
    from sensor_log_solinst_schema_v1
    )
    select manufacturer_sensor
    from sensor_data
    where sensor_data_file_name like '{file_name}'
    """

    df = wr.athena.read_sql_query(query, database=f"{os.environ['infra']}_source_multi_sensor_data_system")

    if df.shape[0] == 1:
        return df.loc[0, "manufacturer_sensor"]
    elif df.shape[0] > 1:
        raise Exception(f"Duplicate Log Sheets FOr: {file_name} | DF Shape: {df.shape}")
    elif df.shape[0] == 0:
        return "missing"


def process_data_file(sensor_type, file_name, config):
    try:
        if sensor_type == "minidot":
            result = preprocessMinidotData(event={"file_name": file_name}, config=config)
            print(result)
        if sensor_type == "solinst":
            result = preprocessSolinstData(event={"file_name": file_name}, config=config)
            print(result)
        if sensor_type == "ysi":
            result = preprocessYsiData(event={"file_name": file_name}, config=config)
            print(result)
    except Exception as e:
        print("=========ERROR===============")
        print(f"sensor_type: {sensor_type}")
        print(f"file_name: {file_name}")
        print(e)
        print("=========ERROR===============")


def process_log_file(file_name, config):
    try:
        if "YSI" in file_name and "DO NOT SUBMIT" not in file_name:
            result = preprocessYsiLogFile(event={"file_name": file_name}, config=config)
            print(result)
        if "PME" in file_name and "DO NOT SUBMIT" not in file_name:
            result = preprocessMinidotLogFile(event={"file_name": file_name}, config=config)
            print(result)
        if "Solinst" in file_name:
            result = preprocessSolinstLogFile(event={"file_name": file_name}, config=config)
            print(result)
    except Exception as e:
        print("=========ERROR===============")
        print(f"file_name: {file_name}")
        print(e)
        print("=========ERROR===============")
