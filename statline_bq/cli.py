import click
from statline_bq import commands
import toml


@click.command()
# @click.argument("config", type=click.File("r"))
@click.argument("dataset")
def upload_datasets(dataset):
    """
    This explains what this CLI is about
    """
    commands.cbs_odata_to_gcs(dataset)
    # click.echo(dataset)
