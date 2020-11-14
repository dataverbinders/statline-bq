import click
from statline_bq import commands
import toml


@click.command()
# @click.argument("config", type=click.File("r"))
@click.argument("dataset", type=str)
def upload_datasets(dataset):
    """
    This explains what this CLI is about
    """
    commands.cbsodatav4_to_gcs(dataset)
