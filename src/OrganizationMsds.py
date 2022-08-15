
import os

class OrganizationMsds:
    def __init__(self):
        self.base_s3_bucket = f"s3://{os.environ['infra']}-mtsu-msds-data-lake-source/sensor-files/"
