import os
from pathlib import Path

import awswrangler as wr

from OrganizationProcessing.OrganizationMsds.minidotPreprocess import (
    postValidateMinidotDataFile,
    postValidateMinidotLogFile,
    preprocessMinidotData,
    preprocessMinidotLogFile,
    preValidateMinidotDataFile,
    preValidateMinidotLogFile,
)
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


def pre_validate_data_file(sensor, file_name, config):
    print(f"Pre-Validate Data {sensor} {file_name}")
    if sensor == "minidot":
        return preValidateMinidotDataFile(event={"file_name": file_name}, config=config)


def process_data_file(sensor, file_name, config):
    try:
        if sensor == "minidot":
            result = preprocessMinidotData(event={"file_name": file_name}, config=config)
            print(result)
        if sensor == "solinst":
            result = preprocessSolinstData(event={"file_name": file_name}, config=config)
            print(result)
        if sensor == "ysi":
            result = preprocessYsiData(event={"file_name": file_name}, config=config)
            print(result)
    except Exception as e:
        print("=========ERROR===============")
        print(f"sensor: {sensor}")
        print(f"file_name: {file_name}")
        print(e)
        print("=========ERROR===============")


def post_validate_data_file(sensor, file_name, config):
    print(f"Post-Validate Data {sensor} {file_name}")
    if sensor == "minidot":
        return postValidateMinidotDataFile(event={"file_name": file_name}, config=config)


def pre_validate_log_file(sensor, file_name, config):
    print(f"Pre-Validate Log {file_name}")
    if sensor == "minidot":
        return preValidateMinidotLogFile(event={"file_name": file_name}, config=config)


def process_log_file(sensor, file_name, config):
    try:
        if sensor == "ysi":
            result = preprocessYsiLogFile(event={"file_name": file_name}, config=config)
            print(result)
        if sensor == "minidot":
            result = preprocessMinidotLogFile(event={"file_name": file_name}, config=config)
            print(result)
        if sensor == "solinst":
            result = preprocessSolinstLogFile(event={"file_name": file_name}, config=config)
            print(result)
    except Exception as e:
        print("=========ERROR===============")
        print(f"file_name: {file_name}")
        print(e)
        print("=========ERROR===============")


def post_validate_log_file(sensor, file_name, config):
    print(f"Post-Validate Log {file_name}")
    if sensor == "minidot":
        return postValidateMinidotLogFile(event={"file_name": file_name}, config=config)
