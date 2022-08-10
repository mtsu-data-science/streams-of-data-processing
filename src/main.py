from file_upload.file_upload import file_upload_epa, file_upload_tbi, file_upload_usgs


def main():
    org_config = "epa"

    if org_config == "epa":
        org_file_upload = file_upload_epa("epa_file.txt", "epa_bucket")

    if org_config == "usgs":
        org_file_upload = file_upload_usgs("usgs_file.txt", "usgs_bucket")

    if org_config == "tbi":
        org_file_upload = file_upload_tbi("tbi_file.txt", "tbi_bucket")

    org_file_upload.print_file_name()
    org_file_upload.print_s3_bucket()


if __name__ == "__main__":
    main()
