import click
from statline_bq.utils import cbs_odata_to_gcs
from statline_bq.config import get_config
from pathlib import Path


@click.command()
# @click.argument("config", type=click.File("r"))
# @click.argument("dataset")
def upload_datasets():
    """
    This CLI uploads datasets from CBS to Google Cloud Platform.

    To run it, you must first have a GCP account, to which a GCS Project and a
    GCS Bucket are connected. Additionally, you must hold the proper IAM
    (permissions) settings enabled on this project.

    The GCP settings, along with a list of the datasets you wish to upload
    should be manually written into `config.toml`.

    For further information, see the documentaion "????"
    """

    config_path = Path("./config.toml")
    config = get_config(config_path)
    click.echo("The following datasets will be downloaded from CBS and uploaded into:")
    click.echo("")
    click.echo(f"Project: {config.gcp.project}")
    click.echo(f"Bucket:  {config.gcp.bucket}")
    click.echo("")
    for i, dataset in enumerate(config.datasets):
        click.echo(f"{i+1}. {dataset}")
    click.echo("")
    for dataset in config.datasets:
        cbs_odata_to_gcs(id=dataset, config=config)
