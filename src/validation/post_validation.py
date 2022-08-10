class post_validation:
    def __init__(self, file_name, s3_bucket):
        self.file_name = file_name
        self.s3_bucket = s3_bucket


class post_validation_epa(post_validation):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class post_validation_usgs(post_validation):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)


class post_validation_tbi(post_validation):
    def print_file_name(self):
        print(self.file_name)

    def print_s3_bucket(self):
        print(self.s3_bucket)
