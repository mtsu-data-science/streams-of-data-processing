class file_upload:
    def __init__(self, file_name, s3_bucket):
        self.file_name = file_name
        self.s3_bucket = s3_bucket


class file_upload_epa(file_upload):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class file_upload_usgs(file_upload):
    def print_file_name(self):
        print(self.file_name)


class file_upload_tbi(file_upload):
    def print_file_name(self):
        print(self.file_name)
