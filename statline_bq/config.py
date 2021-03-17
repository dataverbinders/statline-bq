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
class GcpProductionProjects:
    """An object holding all possible production GcpProjects.

    Attributes
    ----------
    cbs_dl: GcpProject
        A GcpProject instance for a CBS Datalake.
    external_dl: GcpProject
        A GcpProject instance for an external (=non CBS) Datalake.
    dwh: GcpProject
        A GcpProject instance to be for a datawarehouse.

    """

    cbs_dl: GcpProject
    external_dl: GcpProject
    dwh: GcpProject


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
    prod: GcpProductionProjects
        A GcpProductionProjects instance, holding various GcpProjects for production.
    """

    dev: GcpProject
    test: GcpProject
    prod: GcpProductionProjects


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
    mlz: str
        A folder to hold all mlz related data
    rivm: str
        A folder to hold all rivm related data
    """

    # root: str = None
    temp: str = None
    agb: str = None
    vektis_open_data: str = None
    cbs: str = None
    bag: str = None
    mlz: str = None
    rivm: str = None
    politie: str = None
    azw: str = None
    iv3: str = None
    mkb: str = None
    jm: str = None
    interreg: str = None


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
