from typing import Union, Iterable, List
from os import remove, PathLike
from pathlib import Path
import requests
import json
from datetime import datetime
from tempfile import gettempdir
import logging
from box import Box

import ndjson
import pyarrow as pa
from pyarrow import json as pa_json
import pyarrow.parquet as pq

# from google.cloud import storage

from statline_bq.log import logdec

logger = logging.getLogger(__name__)


@logdec
def _check_gcp_env(gcp_env: str, options: List[str] = ["dev", "test", "prod"]) -> bool:
    """Check that gcp_env is one of the permitted options

    Parameters
    ----------
    gcp_env : str
        variable to check
    options : List[str], default=["dev", "test", "prod"]
        list of permittable options

    Returns
    -------
    bool
        True if gcp_env is one of options

    Raises
    ------
    ValueError
        If gcp_env is not one of options
    """
    if gcp_env not in options:
        raise ValueError(f"gcp_env must be one of {options}")
    else:
        return True


@logdec
def dict_to_json_file(
    id: str,
    dict: dict,
    dir: Union[Path, str],
    suffix: str,
    source: str = "cbs",
    odata_version: str = None,
) -> Path:
    """Writes a dict as a json file.

    Writes a dictionary into a json file and places that file in a directory
    alongside the rest of that dataset's tables (assuming it, and they exist).
    The file is named according to the same conventions used for the tables,
    and placed in the directory accordingly, namely:

        "{source}.{odata_version}.{id}_{suffix}.json"

    for example:

        "cbs.v3.83583NED_ColDescriptions.json"

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED"
    dict: dict
        The dictionary to be written as a json.
    dir: Path or str
        Path to directory where the file will be stored.
    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.
    odata_version: str
        The version of the OData for this dataset - should be "v3" or "v4".

    Returns
    -------
    json_file: Path
        A path to the output json file
    """

    json_file = Path(dir) / Path(f"{source}.{odata_version}.{id}_{suffix}.json")
    with open(json_file, "w+") as f:
        f.write(json.dumps(dict))
    return json_file


@logdec
def _create_dir(path: Path) -> Path:
    """Checks whether a path exists and is a directory, and creates it if not.

    Parameters
    ----------
    path: Path
        A path to the directory.

    Returns
    -------
    path: Path
        The same input path, to new or existing directory.
    """

    path = Path(path)
    if not (path.exists() and path.is_dir()):
        path.mkdir(parents=True)
    return path


@logdec
def convert_ndjsons_to_parquet(
    files: List[Path], file_name: str, out_dir: Union[Path, str], schema: pa.Schema
) -> Path:
    pq_file = Path(f"{out_dir}/{file_name}.parquet")
    if not schema:
        schema = pa_json.read_json(files[0]).schema
    with pq.ParquetWriter(pq_file, schema) as writer:
        parse_options = pa_json.ParseOptions(explicit_schema=schema)
        for f in files:
            logger.debug(f"Processing {f}")
            table = pa_json.read_json(f, parse_options=parse_options)
            writer.write_table(table)
            remove(f)
    return pq_file


@logdec
def _get_file_names(paths: Iterable[Union[str, PathLike]]) -> list:
    """Gets the filenames from an iterable of Path-like objects

    Parameters
    ----------
    paths: iterable of strings or path-like objects
        An iterable holding path-strings or path-like objects

    Returns
    -------
    file_names: list of str
        A list holding the extracted file names

    Example
    -------
    >>> from pathlib import Path

    >>> path1 = Path('some_folder/other_folder/some_file.txt')
    >>> path2 = 'some_folder/different_folder/another_file.png'
    >>> full_paths = [path1, path2]

    >>> file_names = get_file_names(full_paths)

    >>> for name in file_names:
            print(name)
    some_file.txt
    another_file.png
    """

    paths = [Path(path) for path in paths]
    file_names = [path.name for path in paths]
    return file_names


@logdec
def _create_named_dir(
    id: str, odata_version: str, source: str = "cbs", config: Box = None
) -> Path:
    """Creates a directory according to a specific structure.

    A convenience function, creatind a directory according to the following
    pattern, based on a config object and the rest of the parameters. Meant to
    create a directory for each dataset where its related tables are written
    into as parquet files.

    Directory pattern:
        
        "~/{config.paths.root}/{config.paths.temp}/{source}/{id}/{date_of_creation}/parquet"

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED"
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.
    config: Box
        Config object holding GCP and local paths

    Returns
    -------
    path_to_named_dir: Path
        path to created folder

    -------
    Example
    -------
    >>> from statline_bq.utils import create_named_dir
    >>> from statline_bq.config import get_config
    >>> id = "83583NED"
    >>> config = get_config("path/to/config.file")
    >>> print(config.paths.temp)
    temp
    >>> dir = create_named_dir(id=id, odata_version="v3")
    >>> dir
    PosixPath('/Users/{YOUR_USERNAME}/statline-bq/temp/cbs/v3/83583NED/20201214/parquet')
    """

    # Create "source" dir if does not exist
    temp = Path(gettempdir())
    source_dir = temp / Path(getattr(config.paths, locals()["source"]))

    # Create dataset dir for where final files (i.e. parqeut) will be stored (this is the folder to be eventually uploaded to GCS)
    # TODO: Consider taking the "parquet" string as parameter - it should not be embedded, as this function is general in nature (although currently used only once).
    path = source_dir / Path(
        f"{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}/parquet"
    )
    path_to_named_dir = _create_dir(path)
    return path_to_named_dir


@logdec
def _url_to_ndjson(target_url: str, ndjson_folder: Union[Path, str]):
    """Fetch json formatted data from a specific CBS table url and write each page as n ndjson file.

    Parameters
    ----------
    target_url : str
        The url to fetch from
    ndjson_folder : str or Path
        The folder to store all output files

    Returns
    -------
    list of dicts
        All entries in url, as a list of dicts

    Raises
    ------
    FileNotFoundError
        if no values exist in the url
    """

    logger.debug(f"load_from_url: url = {target_url}")
    r = requests.get(target_url).json()
    if r["value"]:
        # Write as ndjson
        filename = (
            f"page_{int(target_url.split('skip=')[-1])//10000}.ndjson"  # TODO: this is built for v3 - v4 datasets inappropriately names the files page_10, page_20, page_30, etc.
            if "skip" in target_url
            else "page_0.ndjson"
        )
        path = Path(ndjson_folder) / Path(filename)
        with open(path, "w+") as f:
            ndjson.dump(r["value"], f)
        return path
    else:
        return None


@logdec
def clean_python_name(s: str, extra_chars: str = ""):
    """Method to convert string to clean string for use
    in dataframe column names so that it complies to python 2.x object name standard:
           (letter|'_')(letter|digit|'_')
    Based on
    https://stackoverflow.com/questions/3303312/how-do-i-convert-a-string-to-a-valid-variable-name-in-python

    Parameters
    ----------
    s : str
        string to clean
    extra_chars : str, optional
        additional characrters to be replaced by an underscore

    Returns
    -------
    s: str
        clean string
    """
    import re

    # Remove leading characters until we find a letter or underscore, and remove trailing spaces
    s = re.sub("^[^a-zA-Z_]+", "", s.strip())

    # Replace invalid characters with underscores
    s = re.sub("[^0-9a-zA-Z_]" + extra_chars, "_", s)

    return s


# Created to backfill - not needed in normal operation
# @logdec
# def fix_data_properties(
#     id: str,
#     source: str = "cbs",
#     # third_party: bool = False,
#     config: Box = None,
#     gcp_env: str = "dev",
# ):
#     gcp = set_gcp(config=config, gcp_env=gcp_env, source=source)
#     odata_version = "v3"  # only relevant for v3
#     pq_dir = create_named_dir(
#         id=id, odata_version=odata_version, source=source, config=config
#     )
#     file_name = f"{source}.{odata_version}.{id}_DataProperties.parquet"
#     after_path = pq_dir / file_name
#     before_path = (
#         pq_dir / f"{source}.{odata_version}.{id}_DataProperties_BEFORE.parquet"
#     )
#     storage_client = storage.Client(project=gcp.project_id)
#     latest_folder = get_latest_folder(
#         gcs_folder=f"{source}/{odata_version}/{id}", gcp=gcp
#     )
#     bucket = storage_client.get_bucket(gcp.bucket)
#     download_blob = bucket.get_blob(latest_folder + "/" + file_name)
#     upload_blob = bucket.get_blob(latest_folder + "/" + file_name)
#     download_blob.download_to_filename(before_path)
#     table = pq.read_table(before_path)
#     new_column_names = [clean_python_name(name, ".") for name in table.column_names]
#     table = table.rename_columns(new_column_names)
#     pq.write_table(table, after_path)
#     upload_blob.upload_from_filename(after_path)
#     before_path.unlink(missing_ok=True)
#     after_path.unlink(missing_ok=True)
#     return new_column_names


# if __name__ == "__main__":
#     from statline_bq.config import get_config

#     config = get_config("./statline_bq/config.toml")
#     for id in [
#         "81008ned",
#         "81498ned",
#         "82000NED",
#         "82496NED",
#         "82949NED",
#         "83287NED",
#         "83553NED",
#         "83859NED",
#         "84378NED",
#         "84721NED",
#         "84929NED",
#         "70739ned",
#     ]:
#         new_column_names = fix_data_properties(
#             id=id, source="cbs", config=config, gcp_env="prod"
#         )
#         print(new_column_names)
