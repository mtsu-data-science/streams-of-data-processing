import click

from file_upload.file_upload import file_upload_epa, file_upload_tbi, file_upload_usgs


@click.command()
@click.option("--org", "-o", type=click.Choice(["epa", "usgs", "tbi"]), prompt="Organization Name")
def get_org(org):
    click.echo(f"Organization Selected: {org}")
    return org


def main():

    org = get_org(standalone_mode=False)

    if org == "epa":
        org_file_upload = file_upload_epa("epa_file.txt", "epa_bucket")

    if org == "usgs":
        org_file_upload = file_upload_usgs("usgs_file.txt", "usgs_bucket")

    if org == "tbi":
        org_file_upload = file_upload_tbi("tbi_file.txt", "tbi_bucket")

    org_file_upload.print_file_name()
    org_file_upload.print_s3_bucket()


if __name__ == "__main__":
    main()
