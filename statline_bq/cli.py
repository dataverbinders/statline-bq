import click
from statline_bq import commands
from pathlib import Path
import toml


@click.command()
# @click.argument("config", type=click.File("r"))
# @click.argument("dataset")
def upload_datasets():
    """
    This CLI uploads datasets from CBS to Google Cloud Platform.

    To run it, you must first have a GCP account, to which a GCS Project and a GCS Bucket
    are connected. Additionally, you must hold the proper IAM (permissions) settings enabled
    On this project.

    The GCP settings, along with a list of the datasets you wish to upload should be entered
    into `config.toml`.

    For further information, see the documentaion "????"
    """
    config_file = Path("./statline_bq/config.toml")
    with open(config_file) as conf:
        config = toml.load(conf)

    datasets = config["datasets"]["datasets"]
    # gcp = config["gcp"]

    click.echo(f"The following datasets will be uploaded:")
    for dataset in datasets:
        click.echo(f"{dataset}")
    for dataset in config["datasets"]["datasets"]:
        commands.cbs_odata_to_gcs(dataset)
        # commands.cbs_odata(dataset, )
    # click.echo(dataset)
