from dataclasses import dataclass
from serde import serialize, deserialize
from serde.toml import from_toml
from pathlib import Path
from typing import Union
from tomlkit import parse as parse_toml


@deserialize
@serialize
@dataclass(frozen=True)
class GcpProject:
    """A immutable Google Cloud Platform Project Data Class, holding
    information regarding an existing GCP Project.

    Attributes
    ----------
    project_id: str
        The project id
    bucket: str
        An existing bucket in the project, where all Storage blobs will be placed
    location: str
        The location of all Storage and BQ items
    """

    project_id: str
    bucket: str
    location: str


@deserialize
@serialize
@dataclass(frozen=True)
class Gcp:
    """An immutable Data Class for Google Cloud Platform, holding three
    GCPProject, one per development stage: 'dev', 'test' and 'prod'.

    Attributes
    ----------
    dev: GcpProject
        A GcpProject instance to be used for development.
    test: GcpProject
        A GcpProject instance to be used for testing.
    prod: GcpProject
        A GcpProject instance to be used for production.
    """

    dev: GcpProject
    test: GcpProject
    prod: GcpProject


@deserialize
@serialize
@dataclass(frozen=True)
class Paths:
    """An immutable Data Class holding information regarding local paths to be
    used during processing of datasets.

    When in this library, Paths.root is always called as follows:

    ```
    from pathlib import Path
    root = Path.home() / Path(Paths.root)
    ```

    And the rest of the folders are defined relative to it as follows:

    ```
    temp = root / Path(Paths.temp)
    ```

    Attributes
    ----------
    root: str
        The path leading to the local folder of 'statline-bq'
    temp: str
        A folder to usewhen writing to disk temporarly
    agb: str
        A folder to hold all agb related data
    vektis_open_data: str
        A folder to hold all vektis related data
    cbs: str
        A folder to hold all cbs related data
    bag: str
        A folder to hold all bag related data
    """

    root: str = None
    temp: str = None
    agb: str = None
    vektis_open_data: str = None
    cbs: str = None
    bag: str = None


@deserialize
@serialize
@dataclass(frozen=True)
class Config:
    """An immutable Data Class holding configuration details for the library,
    holding one instance of Gcp and one of Paths.

    Attributes
    ----------
    gcp: Gcp
        Information for Gcp to use
    paths: Paths
        Information for local paths
    """

    gcp: Gcp
    paths: Paths


def get_config(config_file: Union[Path, str]):
    """Parses a toml file, and returns a Config object.
    
    Takes a path to a toml file, and parses it to instantiate and populate a
    Config object. See README.MD for further details regarding the correct way
    to write the toml file, or see the existing config.toml.

    Parameters
    ----------
    config_file: Path or str
        The location of the config.toml file
    
    Returns
    -------
    config: Config
        A config object with relevant configuration information, including
        GCP and paths info.
    """
    config_file = Path(config_file)
    with open(config_file, "r") as f:
        config = from_toml(Config, f.read())
    return config


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
    config_file = Path(datasets_file)
    with open(config_file, "r") as f:
        doc = parse_toml(f.read())
    return tuple(
        doc["datasets"]["ids"]
    )  # TODO: make it more robust to changes in the file? i.e. if 'ids' was changed to something else?


if __name__ == "__main__":
    config_path = Path("./statline_bq/config.toml")
    datasets_path = Path("./statline_bq/datasets.toml")
    config = get_config(config_path)
    datasets = get_datasets(datasets_path)
    print(datasets)
