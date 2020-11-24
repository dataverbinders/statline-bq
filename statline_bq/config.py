from typing import List
from dataclasses import dataclass, field
from serde import serialize, deserialize
from serde.toml import from_toml
from pathlib import Path
from typing import Union


@deserialize
@serialize
@dataclass(frozen=True)
class GcpProject:
    project_id: str
    bucket: str
    location: str


@deserialize
@serialize
@dataclass(frozen=True)
class Gcp:
    dev: GcpProject
    test: GcpProject
    prod: GcpProject


@deserialize
@serialize
@dataclass(frozen=True)
class Paths:
    root: str
    temp: str


@deserialize
@serialize
@dataclass(frozen=True)
class Config:
    gcp: Gcp
    paths: Paths
    datasets: List[str] = field(default_factory=list, metadata={"serde_skip": True})


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


def get_datasets(config_file: Union[Path, str]):
    """Checks whether the field 'Datasets' is filled within the config file,
    and returns a tuple(/list???) of with the datasets' strings if exists, or
    None if it does not.
    """
    config_file = Path(config_file)


if __name__ == "__main__":
    config_path = Path("statline_bq/config.toml")
    config = get_config(config_path)
    print(config.paths)
