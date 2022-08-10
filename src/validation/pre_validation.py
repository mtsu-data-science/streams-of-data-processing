class pre_validation:
    def __init__(self, file_name, s3_bucket):
        self.file_name = file_name
        self.s3_bucket = s3_bucket


class pre_validation_epa(pre_validation):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class pre_validation_usgs(pre_validation):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class pre_validation_tbi(pre_validation):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)
