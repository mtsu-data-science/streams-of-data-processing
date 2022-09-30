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
