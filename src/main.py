import click
from database.setup_db import setup_database
from processing.processing_epa_ysi import processing_epa_ysi_log


@click.command()
@click.option("--org", "-o", type=click.Choice(["epa", "usgs", "tbi"]), prompt="Organization Name")
def get_org(org):
    click.echo(f"Organization Selected: {org}")
    return org


def main():

    org = get_org(standalone_mode=False)

    setup_database()

    if org == "epa":
        org_file_upload = processing_epa_ysi_log("epa_file.txt", "epa_bucket", "ysi")
        org_file_upload.print_sensor_type()


if __name__ == "__main__":
    main()
