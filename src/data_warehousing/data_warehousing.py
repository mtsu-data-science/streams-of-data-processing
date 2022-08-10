class data_warehousing:
    def __init__(self, file_name, s3_bucket):
        self.file_name = file_name
        self.s3_bucket = s3_bucket


class data_warehousing_epa(data_warehousing):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class data_warehousing_usgs(data_warehousing):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class data_warehousing_tbi(data_warehousing):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)
