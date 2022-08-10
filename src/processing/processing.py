class processing:
    def __init__(self, file_name, s3_bucket):
        self.file_name = file_name
        self.s3_bucket = s3_bucket


class processing_epa(processing):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class processing_usgs(processing):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class processing_tbi(processing):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)
