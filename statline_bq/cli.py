from pathlib import Path

from tomlkit.exceptions import NonExistentKey
import click

from statline_bq.utils import main
from statline_bq.config import get_config, get_datasets


@click.command()
@click.option(
    "--dataset_id",
    help="A valid CBS dataset id to be processed. If not provided, the ids will be taken from 'datasets.toml'",
)
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
def upload_datasets(dataset_id: str, gcp_env: str, force: bool):
    """
    This CLI uploads datasets from CBS to Google Cloud Platform.

    To run it, you must first have a GCP account, to which a GCS Project and a
    GCS Bucket are connected. Additionally, you must hold the proper IAM
    (permissions) settings enabled on this project.

    The GCP settings, should be manually written into "config.toml".

    To upload a single dataset, provide its dataset id as a parameter.
    
    To upload multiple datasets, their ids sould be manually written into
    "datasets.toml".

    For further information, see the documentaion "????"
    """  # TODO: provide link to documnetation

    config_path = Path("./config.toml")
    datasets_path = Path("./datasets.toml")
    config = get_config(config_path)
    if dataset_id:
        datasets = [dataset_id]
    else:
        try:
            datasets = get_datasets(datasets_path)
        except NonExistentKey:
            click.echo(
                "No dataset ids were provided. Please enter a dataset id, either using either the '--dataset_id` parameter or by editing `datasets.toml`"
            )
            return
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
