from pathlib import Path

import click

from statline_bq.utils import main
from statline_bq.config import get_config, get_datasets


@click.command()
@click.option(
    "--gcp-env",
    type=click.Choice(["dev", "test", "prod"], case_sensitive=False),
    default="dev",
    help='Which gcp configuration to use - can take either "dev", "test" or "prod".',
)
@click.option(
    "--force/--no-force",
    default=False,
    help="A flag that forces dataset processing even if the dataset's 'last_modified' metadata field is the same as the same dataset's metadata previously processesed.",
)
# @click.argument("config", type=click.File("r"))
# @click.argument("dataset")
def upload_datasets(gcp_env: str, force: bool):
    """
    This CLI uploads datasets from CBS to Google Cloud Platform.

    To run it, you must first have a GCP account, to which a GCS Project and a
    GCS Bucket are connected. Additionally, you must hold the proper IAM
    (permissions) settings enabled on this project.

    The GCP settings, should be manually written into "config.toml".

    The datasets sould be manually written into "datasets.toml".

    For further information, see the documentaion "????"
    """

    config_path = Path("./config.toml")
    datasets_path = Path("./datasets.toml")
    config = get_config(config_path)
    datasets = get_datasets(datasets_path)
    gcp_env = gcp_env.lower()
    config_envs = {
        "dev": config.gcp.dev,
        "test": config.gcp.test,
        "prod": config.gcp.prod,
    }
    gcp_project = config_envs.get(gcp_env)
    click.echo("The following datasets will be downloaded from CBS and uploaded into:")
    click.echo("")
    click.echo(f"Project: {gcp_project.project_id}")
    click.echo(f"Bucket:  {gcp_project.bucket}")
    click.echo("")
    for i, dataset in enumerate(datasets):
        click.echo(f"{i+1}. {dataset}")
    click.echo("")
    for id in datasets:
        main(id=id, config=config, gcp_env=gcp_env, force=force)
    click.echo("Finished processing datasets.")
