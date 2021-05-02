from typing import Union, Iterable, List
from os import remove, listdir, PathLike
from pathlib import Path
import requests
import json
import toml
from datetime import datetime
from shutil import rmtree
from tempfile import gettempdir
import xml.etree.ElementTree as ET
import logging
import logging.config

import ndjson
import dask.bag as db
import pyarrow as pa
from pyarrow import json as pa_json
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import exceptions
from google.oauth2.credentials import Credentials
from box import Box

LOGGING_CONF_FILE = Path(__file__).parent / "logging_conf.toml"
logging.config.dictConfig(toml.load(LOGGING_CONF_FILE))
logger = logging.getLogger(__name__)


def check_gcp_env(gcp_env: str, options: List[str] = ["dev", "test", "prod"]) -> bool:
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
        logging.error(f"Please set gcp_env to be one of {options}")
        raise ValueError(f"gcp_env must be one of {options}")
    else:
        return True


def check_v4(id: str, third_party: bool = False) -> str:
    """Checks whether a certain CBS table exists as OData Version "v4".

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED"

    third_party: bool, default=False
        Flag to indicate dataset is not originally from CBS. Set to true
        to use dataderden.cbs.nl as base url (not available in v4 yet).

    Returns
    -------
    odata_version: str
        "v4" if exists as odata v4, "v3" otherwise.
    """

    # Third party ("dataderden..cbs.nl") do not have v4 implemenetd
    if third_party:
        return "v3"

    base_url = {
        True: None,  # currently no IV3 links in ODATA V4,
        False: f"https://odata4.cbs.nl/CBS/{id}",
    }
    r = requests.get(base_url[third_party])
    if (
        r.status_code == 200
    ):  # TODO: Is this the best check to use? Maybe if not 404? Or something else?
        odata_version = "v4"
    else:
        odata_version = "v3"
    return odata_version


def get_metadata_cbs(id: str, third_party: bool, odata_version: str) -> dict:
    """Retrieves a dataset's metadata from cbs.

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED"

    third_party: bool
        Flag to indicate dataset is not originally from CBS. Set to true
        to use dataderden.cbs.nl as base url (not available in v4 yet).

    odata_version : str
        The version of the OData for this dataset - should be "v3" or "v4".

    Returns
    -------
    dict
        The dataset's metadata.

    Raises
    ------
    ValueError
        If odata_version is not "v3" or "v4"
    """
    catalog_urls = {
        ("v3", True): "https://dataderden.cbs.nl/ODataCatalog/Tables?$format=json",
        ("v3", False): "https://opendata.cbs.nl/ODataCatalog/Tables?$format=json",
        ("v4", False): f"https://odata4.cbs.nl/CBS/{id}/properties",
    }
    if odata_version == "v3":
        url = catalog_urls[(odata_version, third_party)]
        params = {}
        params["$filter"] = f"Identifier eq '{id}'"
        tables = requests.get(url, params).json()["value"]
        if tables:
            if len(tables) == 1:
                meta = tables[0]
            else:
                pass
                # TODO
                # This means more then 1 result came back for the same ID - which is unlikely, and suggests a bug in the code more than anything.
        else:
            raise KeyError(
                "Dataset ID not found. Please enter a valid ID, and ensure third_paty is set appropriately"
            )
    elif odata_version == "v4":
        if third_party:
            # TODO: HANDLE PROPERLY - is ValueError appropriate here?
            raise ValueError(
                "Third party datasets (IV3) using odata version v4 are not yet implemented in CBS."
            )
        meta = requests.get(catalog_urls[(odata_version, third_party)]).json()
    else:
        raise ValueError("odata version must be either 'v3' or 'v4'")
    return meta


def get_main_table_shape(metadata: dict) -> dict:
    """Reads into a CBS dataset metadata and returns the main table's shape as a dict.

    - For v3 odata, n_records and n_columns exist in the metadata
    - For v4 odata, n_observations exist in the metadata.

    This function returns a dict with all 3 keys, and sets non-existing values as None.

    Parameters
    ----------
    metadata : dict
        The dataset's metadata

    Returns
    -------
    dict
        The dataset's main table's shape
    """
    main_table_shape = {
        "n_records": metadata.get("RecordCount"),
        "n_columns": metadata.get("ColumnCount"),
        "n_observations": metadata.get("ObservationCount"),
    }
    return main_table_shape


def get_schema_cbs(metadata_url, odata_version) -> pa.Schema:
    """Returns a pyarrow.Schema for the main table of a cbs dataset given its base metadata url.

    Parameters
    ----------
    metadata_url : str
        A url containing the metadata of the dataset
    odata_version : str
        The version of the OData for this dataset - should be "v3" or "v4".

    Returns
    -------
    schema : pa.Schema
        A pyarrow Schema object for the main table of the dataset
    """
    # TODO complete full list
    # odata.types taken from: http://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#Picture 1:~:text=4.4%20Primitive%20Types,-Structured
    # pyarrow types taken from: https://arrow.apache.org/docs/python/api/datatypes.html
    odata_to_pa_hash = {
        "Edm.Binary": pa.binary(),
        "Edm.Boolean": pa.bool_(),
        "Edm.Byte": pa.int8(),
        # 'Edm.Date': pa.date32(), or pa.date64(), or something else?
        # 'Edm.DateTimeOffset': pa.timestamp() #Likely requires some wrangling to match
        # "Edm.Decimal": pa.decimal128(),  # TODO: Add precision and scale (see facets below)
        "Edm.Double": pa.float64(),
        # 'Edm.Duration': #TODO ??
        # 'Edm.Guid': #TODO ??
        "Edm.Int16": pa.int16(),
        "Edm.Int32": pa.int32(),
        "Edm.Int64": pa.int64(),
        "Edm.SByte": pa.int8(),
        "Edm.Single": pa.float32(),
        # 'Edm.Stream': #TODO ??
        "Edm.String": pa.string(),
        # 'Edm.TimeOfDay': #TODO ??
        # TODO: add geodata translation:
        # 'Edm.Geography'
        # 'Edm.GeographyPoint'
        # 'Edm.GeographyLineString'
        # 'Edm.GeographyPolygon'
        # 'Edm.GeographyMultiPoint'
        # 'Edm.GeographyMultiLineString'
        # 'Edm.GeographyMultiPolygon'
        # 'Edm.GeographyCollection'
        # 'Edm.Geometry'
        # 'Edm.GeometryPoint'
        # 'Edm.GeometryLineString'
        # 'Edm.GeometryPolygon'
        # 'Edm.GeometryMultiPoint'
        # 'Edm.GeometryMultiLineString'
        # 'Edm.GeometryMultiPolygon'
        # 'Edm.GeometryCollection'
    }
    r = requests.get(metadata_url)
    root = ET.fromstring(r.content)
    TData = root.find(".//*[@Name='TData']")
    schema = []
    for item in TData.iter():
        if "Type" in item.attrib.keys():
            # TODO: Add property facets for relevant types
            # see http://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#Picture 1:~:text=6.2%20Property%20Facets,-Property
            schema.append((item.attrib["Name"], item.attrib["Type"]))
    schema = [
        (field[0], odata_to_pa_hash.get(field[1], pa.string())) for field in schema
    ]
    schema = pa.schema(schema)
    return schema


def get_latest_folder(
    gcs_folder: str, gcp: Box, credentials: Credentials = None
) -> Union[str, None]:
    """Returns the latest subfolder from a "folder" in GCP[^folders].
    
    This function assumes the folders are named with `project-id/cbs/[v3|v4]/dataset-id/YYYYMMDD`,
    and that no further subfolders exist within a YYYYMMDD folder.
    
    For example, assuming the folder "cbs/v3/83583NED/" is populated with subfolders:

    - "cbs/v3/83583NED/20191225"
    - "cbs/v3/83583NED/20200102"
    - "cbs/v3/83583NED/20200108"

    the subfolder with the most recent date, "cbs/v3/83583NED/20200108" is returned.

    Parameters
    ----------
    gcs_folder : str
        The top level folder to traverse
    gcp : Box
        A Box object holding GCP Project parameters (project id, bucket)

    Returns
    -------
    folder : str or None
        The Google Storage folder with the latest date

    References
    ----------
    [^folders]: https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork
    """

    client = storage.Client(project=gcp.project_id, credentials=credentials)
    bucket = client.get_bucket(gcp.bucket)
    # Check if folder exists, return None otherwise
    if not len([blob.name for blob in bucket.list_blobs(prefix=gcs_folder)]):
        return None
    blobs = client.list_blobs(bucket, prefix=gcs_folder)
    dates = [blob.name.split("/")[-2] for blob in blobs]
    dates = set(dates)
    max_date = max(dates)
    folder = f"{gcs_folder}/{max_date}"
    return folder


def get_metadata_gcp(
    id: str, source: str, odata_version: str, gcp: Box, credentials: Credentials = None,
) -> Union[dict, None]:
    """Gets a dataset's metadata from GCP.

    This function assumes dataset's are uploaded to GCP using the following
    naming convention: `project-id/cbs/[v3|v4]/dataset-id/YYYYMMDD`, and that
    within these folders the dataset's metadata is a json file named
    'cbs.[v3|v4].{dataset_id}_Metadata.json'. For example:
    
        - 'cbs.v3.83583NED_Metadata.json'

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED".
    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    gcp : Box
        A Box object holding GCP Project parameters (project id, bucket)

    Returns
    -------
    meta : dict or None
        The metadata of the dataset if found. None otherwise.
    """

    client = storage.Client(project=gcp.project_id, credentials=credentials)
    bucket = client.get_bucket(gcp.bucket)
    gcs_folder = f"{source}/{odata_version}/{id}"
    gcs_folder = get_latest_folder(gcs_folder, gcp)
    blob = bucket.get_blob(f"{gcs_folder}/{source}.{odata_version}.{id}_Metadata.json")
    try:
        meta = json.loads(blob.download_as_string())
        return meta
    except AttributeError:
        logger.warning(
            f"No metadata was found in GCP {gcp.project_id} for {source}_{odata_version}_{id}"
        )
        return None


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


def get_gcp_modified(gcp_meta: dict, force: bool = False) -> Union[str, None]:
    if not force:
        try:
            gcp_modified = gcp_meta.get("Modified")
        except AttributeError:
            gcp_modified = None
    else:
        gcp_modified = None
    return gcp_modified


def skip_dataset(cbs_modified: str, gcp_modified: str, force: bool) -> bool:
    if (cbs_modified is None or cbs_modified == gcp_modified) and (not force):
        return True
    else:
        return False


def create_dir(path: Path) -> Path:
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

    try:
        path = Path(path)
        if not (path.exists() and path.is_dir()):
            path.mkdir(parents=True)
        return path
    except TypeError as error:
        logger.exception(f"Error trying to find {path}: {error!s}")
        return None


# def get_dataset_description(urls: dict, odata_version: str) -> str:
#     """Gets a CBS dataset description text.

#     Wrapper function to call the correct version function which in turn gets
#     the dataset description according to the odata version: "v3" or "v4".

#     Parameters
#     ----------
#     urls: dict
#         Dictionary holding urls of the dataset from CBS.
#         NOTE: urls["Properties"] (for v4) or urls["TableInfos"] (for v3)
#         must be present in order to access the dataset description.

#     odata_version: str
#         version of the odata for this dataset - must be either "v3" or "v4".

#     Returns
#     -------
#     description: str
#         The description of the dataset from CBS.

#     Raises
#     ------
#     ValueError
#         If odata_version is not "v3" or "v4"

#     Examples
#     --------
#     >>> from statline_bq.utils import get_dataset_description
#     >>> urls = {
#     ...         "TableInfos": "https://opendata.cbs.nl/ODataFeed/odata/83583NED/TableInfos",
#     ...         "UntypedDataSet": "https://opendata.cbs.nl/ODataFeed/odata/83583NED/UntypedDataSet"
#     ...         }
#     >>> odata_version = "v3"
#     >>> description_text = get_dataset_description(urls, odata_version=odata_version)
#     >>> description_text
#     #Text describing the dataset will print
#     """

#     if odata_version.lower() == "v4":
#         description = get_dataset_description_v4(urls["Properties"])
#     elif odata_version.lower() == "v3":
#         description = get_dataset_description_v3(urls["TableInfos"])
#     else:
#         raise ValueError("odata version must be either 'v3' or 'v4'")
#     return description


# def get_dataset_description_v3(url_table_infos: str) -> str:
#     """Gets the description of a v3 odata dataset from CBS provided in url_table_infos.

#     Usually wrapped in `get_dataset_description()`, and it is better practice
#     to call it wrapped to allow for both "v3" and "v4" functionality.

#     Parameters
#     ----------
#     url_table_infos: str
#         The url for a dataset's "TableInfos" table as string.

#     Returns
#     -------
#     description: str
#         A string with the dataset's description
#     """

#     # Get JSON format of data set.
#     url_table_infos = "?".join((url_table_infos, "$format=json"))

#     data_info = requests.get(url_table_infos).json()  # Is of type dict()

#     data_info_values = data_info["value"]  # Is of type list

#     # Get short description as text
#     description = data_info_values[0]["ShortDescription"]

#     return description


# def get_dataset_description_v4(url_table_properties: str) -> str:
#     """Gets table description of a table in CBS odata V4.

#     Usually wrapped in `get_dataset_description()`, and it is better practice
#     to call it wrapped to allow for both "v3" and "v4" functionality.

#     Parameters
#     ----------
#     url_table_properties: str
#         The url for a dataset's "Properties" table as string.

#     Returns
#     -------
#     description: str
#         A string with the dataset's description
#     """

#     r = requests.get(url_table_properties).json()
#     return r["Description"]


def get_column_descriptions(urls: dict, odata_version: str) -> dict:
    """Gets the column descriptions from CBS.

    Wrapper function to call the correct version function which in turn gets
    the dataset description according to the odata version: "v3" or "v4".

    Parameters
    ----------
    urls: dict
        Dictionary holding urls of the dataset from CBS.
        NOTE: urls["????????"] (for v4) or urls["DataProperties"] (for v3)
        must be present in order to access the dataset description.  #TODO: - Only implemented for V3. Implementation might differ for v4

    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".

    Returns
    -------
    dict
        dict holding all coloumn descriptions for the dataset's main table

    Raises
    ------
    ValueError
        If odata_version is not "v3" or "v4"
    """
    if odata_version.lower() == "v4":
        # Since odata v4 comes in a long format, this seems irrelevant #TODO: Verify
        # column_descriptions = get_column_decriptions_v4(urls["Properties"])
        return
    elif odata_version.lower() == "v3":
        column_descriptions = get_column_descriptions_v3(urls["DataProperties"])
    else:
        raise ValueError("odata version must be either 'v3' or 'v4'")
    return column_descriptions


def get_column_descriptions_v3(url_data_properties: str) -> dict:
    """Gets the column descriptions for the TypedDataSet of a CBS dataset V3

    Parameters
    ----------
    url_data_properties : str
        The url for a dataset's "DataProperties" table as string.

    Returns
    -------
    dict
        All of the "TypedDataSet" column descriptions.
    """
    # Construct url to get json format
    url_data_properties = "?".join((url_data_properties, "$format=json"))
    # Load data properties into a dict
    data_properties = requests.get(url_data_properties).json()["value"]
    # Create new dict with only descriptions
    col_desc = {item["Key"]: item["Description"] for item in data_properties}
    # If description exists, clean and truncate (BQ has 1024 chars limit)
    for k in col_desc:
        try:
            col_desc[k] = col_desc[k].replace("\n", "").replace("\r", "")
            if len(col_desc[k]) > 1023:
                col_desc[k] = col_desc[k][:1020] + "..."
        except:  # TODO: Handle better?
            pass
    return col_desc


def convert_ndjsons_to_parquet(
    files: List[Path], file_name: str, out_dir: Union[Path, str], schema: pa.Schema
) -> Path:
    pq_file = Path(f"{out_dir}/{file_name}.parquet")
    if not schema:
        schema = pa_json.read_json(files[0]).schema
    with pq.ParquetWriter(pq_file, schema) as writer:
        parse_options = pa_json.ParseOptions(explicit_schema=schema)
        for f in files:
            logger.info(f"Processing {f}")
            table = pa_json.read_json(f, parse_options=parse_options)
            writer.write_table(table)
            remove(f)
    return pq_file


def set_gcp(config: Box, gcp_env: str, source: str) -> Box:
    """Sets the desired GCP donciguration

    Parameters
    ----------
    config : Box
        configuration object
    gcp_env : str
        String representing the desired environment between ['dev', 'test', 'prod']
    source : str
        The source of the dataset.

    Returns
    -------
    Box
        A Box object holding GCP Project parameters (project id, bucket)
    """
    gcp_env = gcp_env.lower()
    source = source.lower() if source.lower() == "cbs" else "external"

    config_envs = {
        "dev": config.gcp.dev,
        "test": config.gcp.test,
        "prod": {
            "cbs": config.gcp.prod.cbs_dl,
            "external": config.gcp.prod.external_dl,
            "dwh": config.gcp.prod.dwh,
        },
    }
    return config_envs[gcp_env][source] if gcp_env == "prod" else config_envs[gcp_env]


def upload_to_gcs(
    dir: Path,
    source: str = "cbs",
    odata_version: str = None,
    id: str = None,
    gcp: Box = None,
    credentials: Credentials = None,
) -> str:  # TODO change the return value to some indication or id from Google?:
    """Uploads all files in a given directory to Google Cloud Storage.

    This function is meant to be used for uploading all tables of a certain dataset retrieved from
    the CBS API. It therefore uses the following naming structure as the GCS blobs:

        "{project_name}/{bucket_name}/{source}/{odata_version}/{id}/{YYYYMMDD}/{filename}"

    For example, dataset "82807NED", uploaded on Novemeber 11, 2020, to the "dataverbinders" project,
    using "dataverbinders" as a bucket, would create the following:

    - "dataverbinders/dataverbinders/cbs/v4/83765NED/20201104/cbs.82807NED_Observations.parquet"
    - "dataverbinders/dataverbinders/cbs/v4/83765NED/20201104/cbs.82807NED_PeriodenCodes.parquet"
    - etc..

    Parameters
    ----------
    dir: Path
        A Path object to a directory containing files to be uploaded
    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    id: str
        CBS Dataset id, i.e. "83583NED"
    config: Box
        Config object holding GCP and local paths
    gcp_env: str
        determines which GCP configuration to use from config.gcp


    Returns
    -------
    gcs_folder: str
        The folder (=blob) into which the tables have been uploaded # TODO -> Return success/ fail code?/job ID
    """
    # Initialize Google Storage Client and get bucket according to gcp_env
    # gcp = set_gcp(config=config, gcp_env=gcp_env, source=source)
    gcs = storage.Client(project=gcp.project_id, credentials=credentials)
    gcs_bucket = gcs.get_bucket(gcp.bucket)
    # Set blob
    gcs_folder = (
        f"{source}/{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}"
    )
    # Upload file
    for pfile in listdir(dir):
        gcs_blob = gcs_bucket.blob(gcs_folder + "/" + pfile)
        gcs_blob.upload_from_filename(
            dir / pfile
        )  # TODO: job currently returns None. Also how to handle if we get it?

    return gcs_folder  # TODO: return job id, if possible


def get_file_names(paths: Iterable[Union[str, PathLike]]) -> list:
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


def bq_update_main_table_col_descriptions(
    dataset_ref: str,
    descriptions: dict,
    gcp: Box = None,
    credentials: Credentials = None,
) -> bigquery.Table:
    """Updates column descriptions of main table for existing BQ dataset

    Parameters
    ----------
    dataset_ref : str
        dataset reference where main table exists
    descriptions : dict
        dictionary holding column descriptions
    gcp : Box,
        Box object holding GCP configurations
    gcp_env : str, default = "dev"
        determines which GCP configuration to use from gcp

    Returns
    -------
    bigquery.Table
        The updated table
    """

    # # Set GCP environmnet
    # gcp = set_gcp(config=config, gcp_env=gcp_env, source=source)

    # Construct a BigQuery client object.
    client = bigquery.Client(project=gcp.project_id, credentials=credentials)

    # Get all tables
    tables = client.list_tables(dataset_ref)

    # Set main_table as "TypedDataSet" reference  # This implementation allows flexibility in naming conventions, so long as "TypedDataset" is part of the table name(=id)
    options = ["TypedDataSet", "typeddataset", "TypedDataset"]
    for table in tables:
        if any(option in table.table_id for option in options):
            main_table_id = table.table_id
            break

    try:
        # write as standard SQL format
        main_table_id = dataset_ref.dataset_id + "." + main_table_id
        main_table = client.get_table(main_table_id)
    except UnboundLocalError as e:
        logger.exception(
            f"No table located with 'TypedDataset' in its name for dataset {dataset_ref}: {e}"
        )
        return None

    # Create SchemaField for column description
    new_schema = []
    for field in main_table.schema:
        schema_dict = field.to_api_repr()
        schema_dict["description"] = descriptions.get(schema_dict["name"])
        new_schema.append(bigquery.SchemaField.from_api_repr(schema_dict))

    main_table.schema = new_schema

    table = client.update_table(main_table, ["schema"])

    return table


def get_col_descs_from_gcs(
    id: str,
    source: str = "cbs",
    odata_version: str = None,
    gcp: Box = None,
    gcs_folder: str = None,
    credentials: Credentials = None,
) -> dict:
    """Gets previously uploaded dataset column descriptions from GCS.

    The description should exist in the following file, under the following structure:

        "{project}/{bucket}/{source}/{odata_version}/{id}/{YYYYMMDD}/{source}.{odata_version}.{id}_ColDescriptions.json"

    For example:

        "dataverbinders-dev/cbs/v4/83765NED/20201127/cbs.v4.83765NED_ColDescriptions.json"

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED".
    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    gcp: Box
        A Box object, holding GCP parameters
    gcs_folder : str
        The GCS folder holding the coloumn descriptions json file

    Returns
    -------
    dict
        Dictionary holding column descriptions
    """
    # gcp = set_gcp(config, gcp_env, source)
    client = storage.Client(project=gcp.project_id, credentials=credentials)
    bucket = client.get_bucket(gcp.bucket)
    blob = bucket.get_blob(
        f"{gcs_folder}/{source}.{odata_version}.{id}_ColDescriptions.json"
    )
    json_text = blob.download_as_string().decode("utf-8")
    col_desc = json.loads(json_text)
    return col_desc


def cbsodata_to_gbq(
    id: str,
    odata_version: str,
    third_party: str = False,
    source: str = "cbs",
    config: Box = None,
    gcp_env: str = None,
    force: bool = False,
    credentials: Credentials = None,
) -> set:  # TODO change return value
    """Loads a CBS dataset as a dataset in Google BigQuery.

    Retrieves a given dataset id from CBS, and converts it locally to Parquet. The
    Parquet files are uploaded to Google Cloud Storage, and a dataset is created
    in Google BigQuery, under which each permanenet tables are nested,linked to the
    Parquet files - each being a table of the dataset.

    Parameters
    ---------
    id: str
        CBS Dataset id, i.e. "83583NED"

    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".

    third_party: bool, default=False
        Flag to indicate dataset is not originally from CBS. Set to true
        to use dataderden.cbs.nl as base url (not available in v4 yet).

    source: str, default="cbs"
        The source of the dataset.

    config: Box
        Config object holding GCP and local paths

    gcp_env: str
        determines which GCP configuration to use from config.gcp
    
    force : bool, default = False
        If set to True, processes datasets, even if Modified dates are
        identical in source and target locations.

    Returns
    -------
    files_parquet: set of Paths
        A set with paths of local parquet files # TODO: replace with BQ job ids

    Example
    -------
    >>> from statline_bq.utils import check_v4, cbsodata_to_gbq
    >>> from statline_bq.config import get_config
    >>> id = "83583NED"
    >>> config = get_config("path/to/config.file")
    >>> print(f"Processing dataset {id}")
    >>> odata_version = check_v4(id=id)
    >>> cbsodata_to_gbq(
    ... id=id,
    ... odata_version=odata_version,
    ... config=config
    ... )
    >>> print(f"Completed dataset {id}")
    Processing dataset 83583NED
    # More messages from depending on internal process
    Completed dataset 83583NED

    Notes
    -----
    In **GCS**, the following "folders" and filenames structure is used:

        "{project_name}/{bucket_name}/{source}/{version}/{dataset_id}/{date_of_upload}/{source}.{version}.{dataset_id}_{table_name}.parquet"

    for example:

        "dataverbinders/dataverbinders/cbs/v3/84286NED/20201125/cbs.v3.84286NED_TypedDataSet.parquet"
    _________
    In **BQ**, the following structure and table names are used:

        "[project/]/{source}_{version}_{dataset_id}/{dataset_id}/{table_name}"

    for example:

        "[dataverbinders/]/cbs_v3_83765NED/83765NED_Observations"

    Odata version 3
    ---------------

    For given dataset id, the following tables are uploaded into GCS and linked in
    GBQ (taking `cbs` as default and `83583NED` as example):

    - "cbs.v3.83583NED_DataProperties" - Description of topics and dimensions contained in table
    - "cbs.v3.83583NED_{DimensionName}" - Separate dimension tables
    - "cbs.v3.83583NED_TypedDataSet" - The TypedDataset (***main table***)
    - "cbs.v3.83583NED_CategoryGroups" - Grouping of dimensions

    See *Handleiding CBS Open Data Services (v3)*[^odatav3] for details.

    Odata Version 4
    ---------------

    For a given dataset id, the following tables are ALWAYS uploaded into GCS
    and linked in GBQ (taking `cbs` as default and `83765NED` as example):

    - "cbs.v4.83765NED_Observations" - The actual values of the dataset (***main table***)
    - "cbs.v4.83765NED_MeasureCodes" - Describing the codes that appear in the Measure column of the Observations table.
    - "cbs.v4.83765NED_Dimensions" - Information regarding the dimensions

    Additionally, this function will upload all other tables related to the dataset, except for `Properties`.
        
    These may include:

    - "cbs.v4.83765NED_MeasureGroups" - Describing the hierarchy of the Measures

    And, for each Dimension listed in the `Dimensions` table (i.e. `{Dimension_1}`)
    
    - "cbs.v4.83765NED_{Dimension_1}Codes"
    - "cbs.v4.83765NED_{Dimension_1}Groups" (IF IT EXISTS)

    See *Informatie voor Ontwikelaars*[^odatav4] for details.

    [^odatav3]: https://www.cbs.nl/-/media/statline/documenten/handleiding-cbs-ewopendata-services.pdf
    [^odatav4]: https://dataportal.cbs.nl/info/ontwikkelaars
    """

    # Set gcp environment
    gcp = set_gcp(config, gcp_env, source)
    # Get all table-specific urls for the given dataset id
    urls = get_urls(id=id, odata_version=odata_version, third_party=third_party)
    # Get dataset metadata
    source_meta = get_metadata_cbs(
        id=id, third_party=third_party, odata_version=odata_version
    )
    gcp_meta = get_metadata_gcp(
        id=id,
        source=source,
        odata_version=odata_version,
        gcp=gcp,
        credentials=credentials,
    )

    ## Check if upload is needed
    # Get dataset modified date from source metadata
    cbs_modified = source_meta.get("Modified")
    # Get datatset modified date from GCP metadata (set to None if force is True)
    gcp_modified = get_gcp_modified(gcp_meta, force)
    # Skip all process if modified date is the same in GCP and source (and Force is set to False)
    if skip_dataset(cbs_modified, gcp_modified, force):
        # if (cbs_modified is None or cbs_modified == gcp_modified) and (not force):
        logger.info(cbs_modified)
        logger.info(
            f"Skipping dataset {id} because the same dataset exists on GCP, with the same 'Modified' date"
        )
        logger.info(f"Dataset {id} source last modified: {cbs_modified}")
        logger.info(f"Dataset {id} gcp last modified: {gcp_modified}")
        return None

    # Create directory to store parquest files locally
    pq_dir = create_named_dir(
        id=id, odata_version=odata_version, source=source, config=config
    )
    # Set main table shape to use for parallel fetching later
    main_table_shape = get_main_table_shape(source_meta)
    # Fetch each table from urls, convert to parquet and store locally
    files_parquet = tables_to_parquet(
        id=id,
        third_party=third_party,
        urls=urls,
        main_table_shape=main_table_shape,
        odata_version=odata_version,
        source=source,
        pq_dir=pq_dir,
    )
    # DataProperties table contains "." in field names which is not allowed in linked BQ tables
    data_properties_pq = next(
        (x for x in files_parquet if "DataProperties" in str(x)), None
    )
    if data_properties_pq:
        data_properties_table = pq.read_table(data_properties_pq)
        new_column_names = [
            name.replace(".", "_") for name in data_properties_table.column_names
        ]
        data_properties_table = data_properties_table.rename_columns(new_column_names)
        pq.write_table(data_properties_table, data_properties_pq)

    # Get columns' descriptions from CBS
    col_descriptions = get_column_descriptions(urls, odata_version=odata_version)
    # Write column descriptions to json file and store in dataset directory with parquet tables
    dict_to_json_file(
        id=id,
        dict=col_descriptions,
        dir=pq_dir,
        suffix="ColDescriptions",
        source=source,
        odata_version=odata_version,
    )
    # Write metadata to json file and store in dataset directory with parquet tables
    dict_to_json_file(
        id=id,
        dict=source_meta,
        dir=pq_dir,
        suffix="Metadata",
        source=source,
        odata_version=odata_version,
    )
    # Upload to GCS
    gcs_folder = upload_to_gcs(
        dir=pq_dir,
        source=source,
        odata_version=odata_version,
        id=id,
        gcp=gcp,
        credentials=credentials,
    )

    # Get file names for BQ dataset ids
    file_names = get_file_names(files_parquet)
    # Create table in GBQ
    dataset_ref = gcs_to_gbq(
        id=id,
        source=source,
        odata_version=odata_version,
        gcs_folder=gcs_folder,
        file_names=file_names,
        gcp=gcp,
        credentials=credentials,
    )
    # Add column description to main table
    desc_dict = get_col_descs_from_gcs(
        id=id,
        source=source,
        odata_version=odata_version,
        gcp=gcp,
        gcs_folder=gcs_folder,
        credentials=credentials,
    )

    # Add column descriptions to main table (only relevant for v3, as v4 is a "long format")
    if odata_version == "v3":
        bq_update_main_table_col_descriptions(
            dataset_ref=dataset_ref,
            descriptions=desc_dict,
            gcp=gcp,
            credentials=credentials,
        )

    # Remove all local files created for this process
    rmtree(pq_dir.parents[1])

    return files_parquet  # TODO: return bq job ids


def get_urls(
    id: str, odata_version: str, third_party: bool = False
) -> dict:  # TODO: Rename to get_dataset_urls (contrast with get_table_urls)
    """Returns a dict with urls of all dataset tables given a valid CBS dataset id.

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED"

    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".

    third_party: bool, default=False
        Flag to indicate dataset is not originally from CBS. Set to true
        to use dataderden.cbs.nl as base url (not available in v4 yet).

    Returns:
    urls: dict of str
        A dict containing all urls of a CBS dataset's tables

    Examples:
    >>> dataset_id = '83583NED'
    >>> urls = get_urls(id=dataset_id, odata_version="v3", third_party=False)
    >>> for name, url in urls.items():
    ...     print(f"{name}: {url}")
    TableInfos: https://opendata.cbs.nl/ODataFeed/odata/83583NED/TableInfos
    UntypedDataSet: https://opendata.cbs.nl/ODataFeed/odata/83583NED/UntypedDataSet
    TypedDataSet: https://opendata.cbs.nl/ODataFeed/odata/83583NED/TypedDataSet
    DataProperties: https://opendata.cbs.nl/ODataFeed/odata/83583NED/DataProperties
    CategoryGroups: https://opendata.cbs.nl/ODataFeed/odata/83583NED/CategoryGroups
    BedrijfstakkenBranchesSBI2008: https://opendata.cbs.nl/ODataFeed/odata/83583NED/BedrijfstakkenBranchesSBI2008
    Bedrijfsgrootte: https://opendata.cbs.nl/ODataFeed/odata/83583NED/Bedrijfsgrootte
    Perioden: https://opendata.cbs.nl/ODataFeed/odata/83583NED/Perioden 
    """

    if odata_version == "v4":
        base_url = {
            True: None,  # currently no IV3 links in ODATA V4,
            False: f"https://odata4.cbs.nl/CBS/{id}",
        }
        urls = {
            item["name"]: base_url[third_party] + "/" + item["url"]
            for item in requests.get(base_url[third_party]).json()["value"]
        }
    elif odata_version == "v3":
        base_url = {
            True: f"https://dataderden.cbs.nl/ODataFeed/odata/{id}?$format=json",
            False: f"https://opendata.cbs.nl/ODataFeed/odata/{id}?$format=json",
        }
        urls = {
            item["name"]: item["url"]
            for item in requests.get(base_url[third_party]).json()["value"]
        }
    else:
        raise ValueError("odata version must be either 'v3' or 'v4'")
    return urls


def create_named_dir(
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
    path_to_named_dir = create_dir(path)
    return path_to_named_dir


def generate_table_urls(base_url: str, n_records: int, odata_version: str) -> list:
    """Creates a list of urls for parallel fetching.

    Given a base url, this function creates a list of multiple urls, with query parameters
    added to the base url, each reading "$skip={i}" where i is a multiplication of 10,000
    (for v3) or 100,000 (for v4). The base url is meant to be the url for a CBS table, and
    so each generated url corresponds to the next 10,000(/100,000) rows of the table.

    Parameters
    ----------
    base_url : str
        The base url for the table.
    n_records : int
        The amount of rows(=records/observations) in the table.
    odata_version : str
        version of the odata for this dataset - must be either "v3" or "v4".

    Returns
    -------
    table_urls : list of str
        A list holding all urls needed to fetch full table data.
    """
    # Since v3 already has a parameter ("?$format=json"), the v3 and v4 connectors are different
    connector = {"v3": "&", "v4": "?"}
    cbs_limit = {"v3": 10000, "v4": 100000}
    trailing_zeros = {"v3": 4, "v4": 5}
    # Only the main table has more then 10000(/100000 for v4) rows, the other tables use None
    if n_records is not None:
        # Create url list with query parameters
        table_urls = [
            base_url
            + f"{connector[odata_version]}$skip={str(i+1)}"
            + ("0" * trailing_zeros[odata_version])
            for i in range(n_records // cbs_limit[odata_version])
        ]
        # Add base url to list
        table_urls.insert(0, base_url)
    else:
        table_urls = [base_url]
    return table_urls


def url_to_ndjson(target_url: str, ndjson_folder: Union[Path, str]):
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

    logger.info(
        f"load_from_url: url = {target_url}"
    )  # TODO - Bubble print statement to logging in general and in Prefect
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


def tables_to_parquet(
    id: str,
    third_party: bool,
    urls: dict,
    main_table_shape: dict,
    odata_version: str,
    source: str = "cbs",
    pq_dir: Union[Path, str] = None,
) -> set:
    """Downloads all tables related to a valid CBS dataset id, and stores them locally as Parquet files.

    Parameters
    ----------
    id : str
        CBS Dataset id, i.e. "83583NED"
    urls : dict
        Dictionary holding urls of all dataset tables from CBS
    odata_version : str
        version of the odata for this dataset - must be either "v3" or "v4".
    source : str, default='cbs
        The source of the dataset.
    pq_dir : Path or str
        The directory where the output Parquet files are stored.

    Returns
    -------
    files_parquet: set of Path
        A set containing Path objects of all output Parquet files
    """

    # Create placeholders for storage
    files_parquet = set()

    # Iterate over all tables related to dataset, except Metadata tables, that
    # are handled earlier ("Properties" from v4 and "TableInfos" from v3) and
    # UntypedDataset (from v3) which is redundant.

    for key, url in [
        (k, v)
        for k, v in urls.items()
        if k
        not in (
            "Properties",
            "TableInfos",
            "UntypedDataSet",
        )  # Redundant tables from v3 AND v4
    ]:

        # for v3 urls an appendix of "?format=json" is needed
        if odata_version == "v3":
            url = "?".join((url, "$format=json"))

        # Create table name to be used in GCS
        table_name = f"{source}.{odata_version}.{id}_{key}"

        # If processing main table, set table shape accordingly. Else set to None.
        if key in ("TypedDataSet", "Observations"):
            # if key in ("TypedDataSet"):
            table_shape = main_table_shape
            metadata_url = "/".join(url.split("/")[:-1]) + "/$metadata"
            # TODO: add support for v4 (getting 406 error on requests inside get_schema_cbs)
            if odata_version == "v3":
                schema = get_schema_cbs(metadata_url, odata_version)
        else:
            table_shape = {
                "n_records": None,
                "n_columns": None,
                "n_observations": None,
            }  # TODO: A better way to default on non-main tables?
            schema = None

        if odata_version == "v3":
            # Generate all table urls
            table_urls = generate_table_urls(
                url, table_shape["n_records"], odata_version
            )
        elif odata_version == "v4":
            table_urls = generate_table_urls(
                url, table_shape["n_observations"], odata_version
            )

        # create directories to store files
        pq_dir = Path(pq_dir)
        ndjson_dir = pq_dir.parent / Path(f"ndjson/{table_name}")
        create_dir(pq_dir)
        create_dir(ndjson_dir)

        # Fetch all urls and dump each as ndjson
        ndjsons_paths = (
            db.from_sequence(table_urls)
            .map(url_to_ndjson, ndjson_folder=ndjson_dir)
            .compute()
        )

        # Some urls (i.e. 84799NED_CategoryGroups) are actually empty. These will be computed to None, and skipped here
        if any(ndjsons_paths):
            # Convert to parquet
            logger.info(
                f"Starting convert_ndjson_to_parquet for table {table_name}"
            )  # TODO Convert to logging, add pq_dir to INFO
            pq_path = convert_ndjsons_to_parquet(
                files=ndjsons_paths,
                # urls=table_urls,
                # bag=table,
                file_name=table_name,
                out_dir=pq_dir,
                schema=schema
                # odata_version=odata_version,
            )
            logger.info(f"Finished convert_ndjson_to_parquet for table {table_name}")
        # Add path of file to set
        if pq_path:
            files_parquet.add(pq_path)

    return files_parquet


def create_bq_dataset(
    id: str,
    source: str = "cbs",
    odata_version: str = None,
    description: str = None,
    gcp: Box = None,
    credentials: Credentials = None,
) -> str:
    """Creates a dataset in Google Big Query. If dataset exists already exists, does nothing.

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED".
    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    description: str
        The description of the dataset
    gcp: Box
        A Box object, holding GCP parameters
    credentials : google.oauth2.credentials.Credentials
        A GCP Credentials object to identify as a service account

    Returns:
    dataset.dataset_id: str
        The id of the created BQ dataset
    """

    # Construct a BigQuery client object.
    client = bigquery.Client(project=gcp.project_id, credentials=credentials)

    # Set dataset_id to the ID of the dataset to create.
    dataset_id = f"{client.project}.{source}_{odata_version}_{id}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = gcp.location

    # Add description if provided
    dataset.description = description

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    try:
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        logger.info(f"Created dataset {client.project}.{dataset.dataset_id}")
    except exceptions.Conflict as e:
        logger.exception(
            f"Dataset {client.project}.{dataset.dataset_id} already exists: {e}"
        )
    finally:
        return dataset.dataset_id


def check_bq_dataset(
    id: str,
    source: str,
    odata_version: str,
    gcp: Box = None,
    credentials: Credentials = None,
) -> bool:
    """Check if dataset exists in BQ.

    Parameters:
    id : str
        The dataset id, i.e. '83583NED'
    source : str
        The source of the datset
    odata_version : str
        "v3" or "v4" indicating the version
    gcp : Box
        A Box object holding GCP parameters (i.e. project and bucket)
    credentials : google.oauth2.credentials.Credentials
        A GCP Credentials object to identify as a service account

    Returns:
        - True if exists, False if does not exists
    """
    client = bigquery.Client(project=gcp.project_id, credentials=credentials)

    dataset_id = f"{source}_{odata_version}_{id}"

    try:
        client.get_dataset(dataset_id)  # Make an API request.
        # logger.info(f"Dataset {dataset_id} already exists")
        return True
    except exceptions.NotFound as e:
        logger.exception(f"Dataset {dataset_id} is not found: {e}")
        return False


def delete_bq_dataset(
    id: str,
    source: str = "cbs",
    odata_version: str = None,
    gcp: Box = None,
    credentials: Credentials = None,
) -> None:
    """Delete an exisiting dataset from Google Big Query

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED".
    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    gcp: Box
        A Box Class object, holding GCP parameters
    credentials : google.oauth2.credentials.Credentials
        A GCP Credentials object to identify as a service account

    Returns
    -------
    None
    """

    # Construct a bq client
    client = bigquery.Client(project=gcp.project_id, credentials=credentials)

    # Set bq dataset id string
    dataset_id = f"{source}_{odata_version}_{id}"

    # Delete the dataset and its contents
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    return None


def gcs_to_gbq(
    id: str,
    source: str = "cbs",
    odata_version: str = None,
    gcs_folder: str = None,
    file_names: list = None,
    gcp: Box = None,
    credentials: Credentials = None,
) -> None:  # TODO Return job id
    """Creates a BQ dataset and links all relevant tables from GCS underneath.
    
    Creates a dataset (if does not exist) in Google Big Query, and underneath
    creates permanent tables linked to parquet file stored in Google Storage.
    If dataset exists, removes it and recreates it with most up to date uploaded files (?) # TODO: Is this the best logic?
    
    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED".
    source: str, default="cbs"
        The source of the dataset.
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    third_party : bool, default=False
        Flag to indicate dataset is not originally from CBS. Set to true to use dataderden.cbs.nl as base url (not available in v4 yet).
    config : Box
        Config object holding GCP and local paths.
    gcs_folder : str
        The GCS folder holding the description txt file.
    file_names : list
        A list holding all file names of tables to be linked.
    gcp_env: str
        determines which GCP configuration to use from config.gcp


    Returns
    -------
    # TODO Return job id
        [description]
    """

    # # Get all parquet files in gcs folder from GCS
    # storage_client = storage.Client(project=gcp.dev.project_id)

    # TODO: retrieve names from GCS? If yes, loop below should change to use these two lists
    # blob_uris = [
    #     blob.self_link
    #     for blob in storage_client.list_blobs(gcp.dev.bucket, prefix=gcs_folder)
    #     if not blob.name.endswith(".txt")
    # ]
    # blob_names = [
    #     blob.name
    #     for blob in storage_client.list_blobs(gcp.dev.bucket, prefix=gcs_folder)
    #     if not blob.name.endswith(".txt")
    # ]

    # Set GCP Environment
    # gcp = set_gcp(config=config, gcp_env=gcp_env, source=source)
    # Get metadata
    meta_gcp = get_metadata_gcp(
        id=id,
        source=source,
        odata_version=odata_version,
        gcp=gcp,
        credentials=credentials,
    )
    # Get dataset description
    description = None
    if meta_gcp:
        if odata_version == "v3":
            description = meta_gcp.get("ShortDescription")
        elif odata_version == "v4":
            description = meta_gcp.get("Description")
        else:
            raise ValueError("odata version must be either 'v3' or 'v4'")

    # Check if dataset exists and delete if it does TODO: maybe delete anyway (deleting uses not_found_ok to ignore error if does not exist)
    if check_bq_dataset(
        id=id,
        source=source,
        odata_version=odata_version,
        gcp=gcp,
        credentials=credentials,
    ):
        delete_bq_dataset(
            id=id,
            source=source,
            odata_version=odata_version,
            gcp=gcp,
            credentials=credentials,
        )

    # Create a dataset in BQ
    dataset_id = create_bq_dataset(
        id=id,
        source=source,
        odata_version=odata_version,
        description=description,
        gcp=gcp,
        credentials=credentials,
    )
    # if not existing:
    # Skip?
    # else:
    # Handle existing dataset - delete and recreate? Repopulate? TODO

    # Initialize client
    client = bigquery.Client(project=gcp.project_id, credentials=credentials)

    # Configure the external data source
    # dataset_id = f"{source}_{odata_version}_{id}"
    dataset_ref = bigquery.DatasetReference(gcp.project_id, dataset_id)

    # Loop over all files related to this dataset id  #TODO: refactor as function(s)
    for name in file_names:
        # Configure the external data source per table id
        table_id = str(name).split(".")[2]
        table = bigquery.Table(dataset_ref.table(table_id))

        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [
            f"https://storage.cloud.google.com/{gcp.bucket}/{gcs_folder}/{name}"
        ]
        table.external_data_configuration = external_config
        # table.description = description

        # Create a permanent table linked to the GCS file
        table = client.create_table(
            table, exists_ok=True
        )  # BUG: error raised, using exists_ok=True to avoid
    return dataset_ref  # TODO Return job id??


def main(
    id: str,
    source: str = "cbs",
    third_party: bool = False,
    config: Box = None,
    gcp_env: str = "dev",
    force: bool = False,
) -> Path:
    gcp_env = gcp_env.lower()
    if third_party and source == "cbs":
        raise ValueError(
            "A third-party dataset cannot have 'cbs' as source: please provide correct 'source' parameter"
        )
    if check_gcp_env(gcp_env):
        logger.info(f"Processing dataset {id}")
        # logger.info("TEST CHANGES")
        odata_version = check_v4(id=id, third_party=third_party)
        files_parquet = cbsodata_to_gbq(
            id=id,
            odata_version=odata_version,
            third_party=third_party,
            source=source,
            config=config,
            gcp_env=gcp_env,
            force=force,
        )
        logger.info(
            f"Completed dataset {id}"
        )  # TODO - add response from google if possible (some success/failure flag)
        if files_parquet:
            pq_file = Path(files_parquet.pop())
            local_folder = pq_file.parents[2]
        else:
            local_folder = None
        return local_folder


def fix_data_properties(
    id: str,
    source: str = "cbs",
    # third_party: bool = False,
    config: Box = None,
    gcp_env: str = "dev",
):
    gcp = set_gcp(config=config, gcp_env=gcp_env, source=source)
    odata_version = "v3"  # only relevant for v3
    pq_dir = create_named_dir(
        id=id, odata_version=odata_version, source=source, config=config
    )
    file_name = f"{source}.{odata_version}.{id}_DataProperties.parquet"
    after_path = pq_dir / file_name
    before_path = (
        pq_dir / f"{source}.{odata_version}.{id}_DataProperties_BEFORE.parquet"
    )
    storage_client = storage.Client(project=gcp.project_id)
    latest_folder = get_latest_folder(
        gcs_folder=f"{source}/{odata_version}/{id}", gcp=gcp
    )
    bucket = storage_client.get_bucket(gcp.bucket)
    download_blob = bucket.get_blob(latest_folder + "/" + file_name)
    upload_blob = bucket.get_blob(latest_folder + "/" + file_name)
    download_blob.download_to_filename(before_path)
    table = pq.read_table(before_path)
    new_column_names = [name.replace(".", "_") for name in table.column_names]
    table = table.rename_columns(new_column_names)
    pq.write_table(table, after_path)
    upload_blob.upload_from_filename(after_path)
    before_path.unlink(missing_ok=True)
    after_path.unlink(missing_ok=True)
    return new_column_names


if __name__ == "__main__":
    from statline_bq.config import get_config

    config = get_config("./statline_bq/config.toml")
    # # Test cbs core dataset, odata_version is v3
    # local_folder = main("83583NED", config=config, gcp_env="dev", force=True)
    # # Test skipping a dataset, odata_version is v3
    # local_folder = main("83583NED", config=config, gcp_env="dev", force=False)
    # Test cbs core dataset, odata_version is v3, contaiing empty url (CategoryGroups)
    # local_folder = main("84799NED", config=config, gcp_env="dev", force=True)
    # Test cbs core dataset, odata_version is v4
    # main("83765NED", config=config, gcp_env="dev", force=True)
    # Test external dataset, odata_version is v3
    # main(
    #     "45012NED",
    #     source="iv3",
    #     third_party=True,
    #     config=config,
    #     gcp_env="dev",
    #     force=True,
    # )
    # print(local_folder)
    for id in [
        "45001NED",
        # "45004NED",
        # "45005NED",
        # "45006NED",
        # "45007NED",
        # "45008NED",
        # "45031NED",
        # "45038NED",
        # "45042NED",
        # "45046NED",
        # "45050NED",
    ]:
        new_column_names = fix_data_properties(
            id=id, source="iv3", config=config, gcp_env="prod"
        )
        print(new_column_names)
