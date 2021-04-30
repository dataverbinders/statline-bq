from typing import Union
from pathlib import Path

from box import Box
import toml


def get_config(config_file: Union[Path, str]) -> Box:
    """Parses a toml file, and returns a frozen box object.

    Takes a path to a toml file, and parses it to instantiate and populate a
    a Frozen Box object. See README.MD for further details regarding the correct way
    to write the toml file, or see the existing config.toml.

    Parameters
    ----------
    config_file: Path or str
        The location of the config.toml file

    Returns
    -------
    config: Box
        A config object with relevant configuration information, including
        GCP and paths info.
    """
    config_file = Path(config_file)
    return Box(toml.load(config_file), frozen_box=True)


def get_datasets(datasets_file: Union[Path, str]) -> tuple:

    """Parses a toml file and returns dataset ids as a list.

    See README.MD for further details regarding the correct way
    to write the toml file, or see the existing datasets.toml.

    Parameters
    ----------
    datasets_file: Path or str
        The location of the datasets.toml file

    Returns
    -------
    tuple
        A tuple holding all dataset ids to be processed
    """
    datasets_file = Path(datasets_file)
    datasets = toml.load(datasets_file)
    if datasets:
        return tuple(
            datasets.get("ids")
        )  # TODO: make it more robust to changes in the file? i.e. if 'ids' was changed to something else?
    else:
        return None


if __name__ == "__main__":
    config_path = Path(__file__).parent / "config.toml"
    datasets_path = Path(__file__).parent / "datasets.toml"
    config = get_config(config_path)
    datasets = get_datasets(datasets_path)
    print(datasets)
