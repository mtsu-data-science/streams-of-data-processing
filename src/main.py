import argparse
import os

from database.setup_database import setup_database
from OrganizationProcessing.OrganizationMsds.OrganizationMsds import OrganizationMsds


def main():

    parser = argparse.ArgumentParser(description="Organization MSDS")
    parser.add_argument("--org", help="Organization", required=True)
    parser.add_argument("--filepath", help="Organization", required=True)
    parser.add_argument("--filetype", help="Organization", required=True)

    args = parser.parse_args()

    setup_database()

    print(os.getcwd())

    if args.org == "epa":
        org_object = OrganizationMsds()

    file_path_to_files = []

    for file in os.listdir(args.filepath):
        cur_file_path = f"{args.filepath}/{file}"
        if os.path.isfile(cur_file_path) and file != ".gitignore":
            file_path_to_files.append(os.path.abspath(cur_file_path))

    print(file_path_to_files)

    org_object.upload_files_to_data_warehouse(file_path_to_files, args.filetype)


if __name__ == "__main__":
    main()
