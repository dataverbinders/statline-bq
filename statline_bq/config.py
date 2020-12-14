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
        A bucket withing the project, where all Storage blobs are placed
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
    """A data class holding information regarding local paths to be used during
    processing of datasets.

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
    gcp: Gcp
    paths: Paths


def get_config(config_file: Union[Path, str]):
    """Parse out a toml file, and returns a frozen config class contating the
    parsed config.toml file, leaving the Datasets information out:

    Args:
        - config_file: a Path (or string) to the config.toml file
    
    Returns:
        - config: a named tuple holding the following data from config.toml:
            - config.GCP: a Gcp class holding three instances of GcpProject class (dev, test and prod)
            - config.Paths: a  Paths class holding local paths to use during processing of data
    """
    config_file = Path(config_file)
    with open(config_file, "r") as f:
        config = from_toml(Config, f.read())
    return config


def get_datasets(datasets_file: Union[Path, str]) -> tuple:
    """Checks whether the field 'Datasets' is filled within the config file,
    and returns a tuple of with the datasets' strings if exists, or
    None if it does not.
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
