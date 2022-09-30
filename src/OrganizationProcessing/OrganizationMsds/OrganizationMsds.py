import json
import os
import pprint
import time
from pathlib import Path

import awswrangler as wr
import numpy as np

from OrganizationProcessing.OrganizationMsds.msds_processing_util import (
    post_validate_data_file,
    post_validate_log_file,
    pre_validate_data_file,
    pre_validate_log_file,
    process_data_file,
    process_log_file,
)

pp = pprint.PrettyPrinter()


class OrganizationMsds:
    def __init__(self, sensor):
        # This is where all the files will be stored in S3
        self.s3_bucket_base = f"s3://{os.environ['infra']}-mtsu-msds-data-lake-source/test"
        self.s3_bucket_log = f"{self.s3_bucket_base}/log-sheet-files"
        self.s3_bucket_data = f"{self.s3_bucket_base}/data-files"

        # For ease of managing, all different manufacturer config files are json and
        # can be found in the pre-processing-config directory
        # Expectation would be this would be moved into the database and configurable
        # from the site
        with open(Path(__file__).parent / "../../../pre-processing-config/pre-processing-minidot.json") as f:
            self.minidot_config = json.load(f)

        with open(Path(__file__).parent / "../../../pre-processing-config/pre-processing-solinst.json") as f:
            self.solinst_config = json.load(f)

        with open(Path(__file__).parent / "../../../pre-processing-config/pre-processing-ysi.json") as f:
            self.ysi_config = json.load(f)

        # This is just a dict to make passing around configs easier
        self.config = {"ysi": self.ysi_config, "minidot": self.minidot_config, "solinst": self.solinst_config}

        self.process_file_status = {}
        self.current_file = ""

        # This makes it easy to know what manufacturer I am processing
        self.sensor = sensor

    def add_file_to_file_status(self, file_name):
        """
        add_file_to_file_status This method takes care of adding a file to the overall status dict of files being
        processed and sets all statuses to pending.

        Args:
            file_name (string): The filename to be added
        """
        self.current_file = file_name
        self.process_file_status[file_name] = {
            "s3_file_path": "pending",
            "upload_status": "pending",
            "processing_status": "pending",
        }

    def update_file_status(self, status, update):
        """
        update_file_status A simple method for managing the status of files as they are processed

        Args:
            status (string): The status to be updated
            update (string): The update to be made to the status
        """
        print(f"{self.current_file}: Setting: {status} to {update}")
        self.process_file_status[self.current_file][status] = update

    def upload_files_to_data_warehouse(self, files, file_type):
        """
        upload_files_to_data_warehouse _summary_

        Args:
            files (list): This is a list of files to be uploaded and processed into the data warehouse
            file_type (string): This tells whether it is a log-sheet file or a sensor data file
        """
        # First, upload files to s3
        for file in files:
            file_name = Path(file).name
            self.add_file_to_file_status(file_name)
            self.process_upload_file_to_s3(file, file_type)

        # Second, call method for processing either log-sheets or data files
        if file_type == "log-sheet":
            print("Processing log-sheet")
            self.manage_log_sheet_processing()
        elif file_type == "data":
            print("Processing sensor data")
            self.manage_sensor_data_processing()

        # will print out the
        pp.pprint(self.process_file_status)

    def process_upload_file_to_s3(self, local_file_path, file_type):
        if file_type == "log-sheet":
            s3_bucket = self.s3_bucket_log
        elif file_type == "data":
            s3_bucket = self.s3_bucket_data
        else:
            raise ValueError(f"Invalid file type: {file_type}")

        s3_file_path = f"{s3_bucket}/{self.current_file}"

        self.update_file_status("s3_file_path", s3_file_path)

        try:
            wr.s3.upload(local_file_path, s3_file_path)
            self.update_file_status("upload_status", "uploaded")
        except Exception as e:
            print(f"Error uploading {local_file_path} to {s3_file_path} - {e}")
            self.update_file_status("upload_status", f"failed - {e}")

    def manage_log_sheet_processing(self):
        """
        manage_log_sheet_processing method takes care of processing the uploaded log-sheet files by running
        the pre-validation, processing, and post-validation steps.
        """
        run_time = []
        source_log_sheet_files = []

        for file in self.process_file_status:
            if self.process_file_status[file]["upload_status"] == "uploaded":
                source_log_sheet_files.append(file)

        amount_of_files = len(source_log_sheet_files)
        pp.pprint(source_log_sheet_files)
        for file_name in source_log_sheet_files:
            st = time.time()
            try:
                self.current_file = file_name
                self.update_file_status("processing_status", "running pre-validation")
                pre_valid_status = pre_validate_log_file(self.sensor, file_name, self.config)
                print(f"Pre-validation status: {pre_valid_status}")
                if pre_valid_status == "pass":
                    self.update_file_status("processing_status", "file processing started")
                    process_log_file(self.sensor, file_name, self.config)
                    self.update_file_status("processing_status", "running post-validation")
                    post_valid_status = post_validate_log_file(self.sensor, file_name, self.config)
                    print(f"Post validation status: {post_valid_status}")
                    if post_valid_status == "pass":
                        self.update_file_status("processing_status", "file processing succeeded")
                    else:
                        self.update_file_status("processing_status", "file processing failed post-validation")
                else:
                    self.update_file_status("processing_status", "file processing failed pre-validation")
            except Exception as e:
                print(f"Error processing {file_name} - {e}")
                self.update_file_status("processing_status", f"failed - {e}")

            amount_of_files = amount_of_files - 1
            run_time.append(round((time.time() - st) / 60, 2))
            avg_time = round(np.average(run_time) * amount_of_files, 2)
            print(f"{amount_of_files} remaining...estimated minutes remaining {avg_time}")

    def manage_sensor_data_processing(self):
        """
        manage_sensor_data_processing method takes care of processing the uploaded log-sheet files by running
        the pre-validation, processing, and post-validation steps.
        """
        run_time = []
        source_data_files = []

        for file in self.process_file_status:
            if self.process_file_status[file]["upload_status"] == "uploaded":
                source_data_files.append(file)

        amount_of_files = len(source_data_files)
        pp.pprint(source_data_files)
        for file_name in source_data_files:
            st = time.time()
            try:
                self.current_file = file_name
                self.update_file_status("processing_status", "running pre-validation")
                self.update_file_status("processing_status", "processing")
                pre_valid_status = pre_validate_data_file(self.sensor, file_name, self.config)
                print(f"Pre-validation status: {pre_valid_status}")
                if pre_valid_status == "pass":
                    self.update_file_status("processing_status", "file processing started")
                    process_data_file(self.sensor, file_name, self.config)
                    self.update_file_status("processing_status", "running post-validation")
                    post_valid_status = post_validate_data_file(self.sensor, file_name, self.config)
                    print(f"Post-validation status: {post_valid_status}")
                    if post_valid_status == "pass":
                        self.update_file_status("processing_status", "file processing succeeded")
                    else:
                        self.update_file_status("processing_status", "file processing failed - post-validation")
                else:
                    self.update_file_status("processing_status", "file processing failed - pre-validation")
            except Exception as e:
                print(f"Error processing {file_name} - {e}")
                self.update_file_status("processing_status", f"failed - {e}")
            amount_of_files = amount_of_files - 1
            run_time.append(round((time.time() - st) / 60, 2))
            avg_time = round(np.average(run_time) * amount_of_files, 2)
            print(f"{amount_of_files} remaining...estimated minutes remaining {avg_time}")
