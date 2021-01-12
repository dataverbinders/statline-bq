import subprocess
from typing import Union, Iterable, List
from os import remove, listdir, rmdir, PathLike
from pathlib import Path
from glob import glob
import requests
import json
from datetime import datetime

import dask.bag as db
from pyarrow import json as pa_json
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import exceptions

from statline_bq.config import Config, Gcp, GcpProject


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


def get_metadata_cbs(urls: dict, odata_version: str) -> dict:
    """Retrieves a dataset's metadata from cbs.

    Parameters
    ----------
    urls : dict
        A list of a dataset's tables' urls
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

    if odata_version == "v3":
        target_url = "?".join((urls["TableInfos"], "$format=json"))
        meta = requests.get(target_url).json()["value"][0]
    elif odata_version == "v4":
        meta = requests.get(urls["Properties"]).json()
    else:
        raise ValueError("odata version must be either 'v3' or 'v4'")
    return meta


def get_latest_folder(gcs_folder: str, gcp: GcpProject) -> Union[str, None]:
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
    gcp : GcpProject
        A GcpProject class object holding GCP Project parameters (project id, bucket)

    Returns
    -------
    folder : str or None
        The Google Storage folder with the latest date

    References
    ----------
    [^folders]: https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork
    """

    client = storage.Client(project=gcp.project_id)
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
    id: str, source: str, odata_version: str, gcp: GcpProject
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
    gcp : GcpProject
        A GcpProject class object holding GCP Project parameters (project id, bucket)

    Returns
    -------
    meta : dict or None
        The metadata of the dataset if found. None otherwise.
    """

    client = storage.Client(project=gcp.project_id)
    bucket = client.get_bucket(gcp.bucket)
    gcs_folder = f"{source}/{odata_version}/{id}"
    gcs_folder = get_latest_folder(gcs_folder, gcp)
    blob = bucket.get_blob(f"{gcs_folder}/{source}.{odata_version}.{id}_Metadata.json")
    try:
        meta = json.loads(blob.download_as_string())
        return meta
    except AttributeError:
        # print("No Metadata exists in GCP - dataset will be processed")
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


def get_from_meta(meta: dict, key: str):
    """Wrapper function to dict.get()

    Parameters
    ----------
    meta : dict
        A dictionary holding a dataset's parameters
    key : str
        [description]

    Returns
    -------
    [type]
        [description]
    """
    return meta.get(key, None)


def get_gcp_modified(gcp_meta: dict, force: bool = False) -> Union[str, None]:
    if not force:
        try:
            gcp_modified = get_from_meta(meta=gcp_meta, key="Modified")
        except AttributeError:
            gcp_modified = None
    else:
        gcp_modified = None
    return gcp_modified


# def skip_dataset():
#     return False


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
        print(f"Error trying to find {path}: {error!s}")
        return None


def get_dataset_description(urls: dict, odata_version: str) -> str:
    """Gets a CBS dataset description text.
    
    Wrapper function to call the correct version function which in turn gets
    the dataset description according to the odata version: "v3" or "v4".

    Parameters
    ----------
    urls: dict
        Dictionary holding urls of the dataset from CBS.
        NOTE: urls["Properties"] (for v4) or urls["TableInfos"] (for v3)
        must be present in order to access the dataset description.

    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".

    Returns
    -------
    description: str
        The description of the dataset from CBS.

    Raises
    ------
    ValueError
        If odata_version is not "v3" or "v4"

    Examples
    --------
    >>> from statline_bq.utils import get_dataset_description
    >>> urls = {
    ...         "TableInfos": "https://opendata.cbs.nl/ODataFeed/odata/83583NED/TableInfos",  
    ...         "UntypedDataSet": "https://opendata.cbs.nl/ODataFeed/odata/83583NED/UntypedDataSet"
    ...         }
    >>> odata_version = "v3"
    >>> description_text = get_dataset_description(urls, odata_version=odata_version)
    >>> description_text
    #Text describing the dataset will print
    """

    if odata_version.lower() == "v4":
        description = get_dataset_description_v4(urls["Properties"])
    elif odata_version.lower() == "v3":
        description = get_dataset_description_v3(urls["TableInfos"])
    else:
        raise ValueError("odata version must be either 'v3' or 'v4'")
    return description


def get_dataset_description_v3(url_table_infos: str) -> str:
    """Gets the description of a v3 odata dataset from CBS provided in url_table_infos.

    Usually wrapped in `get_dataset_description()`, and it is better practice
    to call it wrapped to allow for both "v3" and "v4" functionality.

    Parameters
    ----------
    url_table_infos: str
        The url for a dataset's "TableInfos" table as string.

    Returns
    -------
    description: str
        A string with the dataset's description
    """

    # Get JSON format of data set.
    url_table_infos = "?".join((url_table_infos, "$format=json"))

    data_info = requests.get(url_table_infos).json()  # Is of type dict()

    data_info_values = data_info["value"]  # Is of type list

    # Get short description as text
    description = data_info_values[0]["ShortDescription"]

    return description


def get_dataset_description_v4(url_table_properties: str) -> str:
    """Gets table description of a table in CBS odata V4.

    Usually wrapped in `get_dataset_description()`, and it is better practice
    to call it wrapped to allow for both "v3" and "v4" functionality.

    Parameters
    ----------
    url_table_properties: str
        The url for a dataset's "Properties" table as string.

    Returns
    -------
    description: str
        A string with the dataset's description
    """

    r = requests.get(url_table_properties).json()
    return r["Description"]


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
        except:
            pass
    return col_desc


# Currently not implemented. Possibly not needed.
def get_odata_v4_curl(  # TODO -> CURL command does not process url with ?$skip=100000 ath the end - returns same data as first link
    target_url: str,
):  # TODO -> How to define Bag for type hinting? (https://docs.python.org/3/library/typing.html#newtype)
    """Retrieves a table from a specific url for CBS Odata v4.

    Parameters
    ----------
    url_table_properties: str
        The url of the desired table

    Returns
    -------
    data: Dask bag
        All data received from target url as json type, in a Dask bag
    """

    # First call target url and get json formatted response as dict
    temp_file = "odata.json"
    # r = requests.get(target_url).json()
    subprocess.run(["curl", "-fL", "-o", temp_file, target_url])
    # Parse json file as dict
    with open(temp_file) as f:
        r = json.load(f)
    # Create Dask bag from dict
    bag = db.from_sequence(r["value"])  # TODO -> define npartitions?

    # check if more data exists
    target_url = r.get("@odata.nextLink", None)

    # if more data exists continue to concat bag until complete
    while target_url:
        subprocess.run(["curl", "-fL", "-o", temp_file, target_url])
        # Parse json file as dict
        with open(temp_file) as f:
            r = json.load(f)
        temp_bag = db.from_sequence(r["value"])
        bag = db.concat([bag, temp_bag])

        target_url = r.get("@odata.nextLink", None)

    return bag


def get_odata(target_url: str, odata_version: str) -> db:
    """Gets a table from a valid CBS url and returns it as a Dask bag.

    A wrapper, calling the appropriate version function to get the table from
    a valid CBS url, leading to a table in OData format.

    Parameters
    ----------
    target_url : str
        A url to the table
    odata_version : str
        version of the odata for this dataset - must be either "v3" or "v4".

    Returns
    -------
    Dask Bag
        All data received from target url as json type, concatenated in a Dask Bag

    Raises
    ------
    ValueError
        If 'odata_version` is not one of {"v3", "v4"}
    """

    if odata_version == "v4":
        return get_odata_v4(target_url)
    elif odata_version == "v3":
        return get_odata_v3(target_url)
    else:
        raise ValueError("odata version must be either 'v3' or 'v4'")


def get_odata_v3(target_url: str) -> db:
    """Gets a table from a valid url for CBS dataset with Odata v3.

    This function uses standard requests.get() to retrieve data at target_url
    in json format, and concats it all into a Dask Bag to handle memory
    overflow if needed.

    Each request from CBS is limited to 10,000 rows, and if more data exists
    the key "odata.nextLink" exists in the response with the link to the next
    10,000 (or less) rows.

    Meant to be wrapped by `get_odata()`, and it is better practice to call it
    wrapped to allow for both "v3" and "v4" functionality.

    Parameters
    ----------
    target_url: str
        A valid url of a table from CBS

    Returns
    -------
    bag: Dask Bag
        All data received from target url as json type, concatenated as a Dask bag
    """

    print(f"Fetching from {target_url}")
    # First call target url and get json formatted response as dict
    r = requests.get(target_url).json()

    # Initialize bag as None
    bag = None

    # Create Dask bag from dict (check if not empty field)
    if r["value"]:
        bag = db.from_sequence(r["value"])  # TODO -> define npartitions?

    # check if more data exists
    target_url = r.get("odata.nextLink", None)

    # if more data exists continue to concat bag until complete
    while target_url:
        r = requests.get(target_url).json()
        if r["value"]:
            temp_bag = db.from_sequence(r["value"])
            bag = db.concat([bag, temp_bag])

        target_url = r.get("odata.nextLink", None)

    return bag


def get_odata_v4(target_url: str,) -> db:
    """Gets a table from a specific url for CBS Odata v4.
    This function uses standard requests.get() to retrieve data at target_url
    in json format, and concats it all into a Dask Bag to handle memory
    overflow if needed.

    Each request from CBS is limited to 10,000 rows, and if more data exists
    the key "@odata.nextLink" exists in the response with the link to the next
    10,000 (or less) rows.

    Meant to be wrapped by `get_odata()`, and it is better practice to call it
    wrapped to allow for both "v3" and "v4" functionality.

    Parameters
    ----------
    target_url: str
        A valid url of a table from CBS

    Returns
    -------
    bag: Dask Bag
        All data received from target url as json type, concatenated as a Dask bag
    """

    print(f"Fetching from {target_url}")
    # First call target url and get json formatted response as dict
    r = requests.get(target_url).json()
    # Create Dask bag from dict
    bag = db.from_sequence(r["value"])  # TODO -> define npartitions?

    # check if more data exists
    target_url = r.get("@odata.nextLink", None)

    # if more data exists continue to concat bag until complete
    while target_url:
        r = requests.get(target_url).json()
        temp_bag = db.from_sequence(r["value"])
        bag = db.concat([bag, temp_bag])

        target_url = r.get("@odata.nextLink", None)

    return bag


def convert_table_to_parquet(
    bag, file_name: str, out_dir: Union[str, Path]
) -> Path:  # (TODO -> IS THERE A FASTER/BETTER WAY??)
    """Converts a Dask Bag to Parquet files and stores them on disk.

    Converts a dask bag holding data from a CBS table to Parquet form
    and stores it on disk. The bag should be filled by dicts (can be nested)
    which can be serialized as json.

    The current implementation iterates over each bag partition and dumps
    it into a json file, then appends all file into a single json file. That
    json file is then read into a PyArrow table, and finally that table is
    written as a parquet file to disk.

    Parameters
    ----------
    bag: Dask Bag
        A Bag holding (possibly nested) dicts that can serialized as json
    file_name: str)
        The name of the file to store on disk
    out_dir: str or Path
        A path to the directory where the file is stored

    Returns
    -------
    pq_path: Path
        The path to the output parquet file
    """

    # create directories to store files
    out_dir = Path(out_dir)
    temp_json_dir = out_dir.parent / Path(f"json/{file_name}")
    create_dir(temp_json_dir)
    create_dir(out_dir)

    # File path to dump table as ndjson
    json_path = Path(f"{temp_json_dir}/{file_name}.json")
    # File path to create as parquet file
    pq_path = Path(f"{out_dir}/{file_name}.parquet")

    # Dump each bag partition to json file
    bag.map(json.dumps).to_textfiles(temp_json_dir / "*.json")
    # Get all json file names with path
    filenames = sorted(glob(str(temp_json_dir) + "/*.json"))
    # Append all jsons into a single file  ## Also possible to use Dask Delayed here https://stackoverflow.com/questions/39566809/writing-dask-partitions-into-single-file
    with open(json_path, "w+") as json_file:
        for fn in filenames:
            with open(fn) as f:
                json_file.write(f.read())
            remove(fn)

    # # Works without converting to ndjson - might be needed in a different implementation?
    # # Convert to ndjson format
    # with open(json_path, 'w+') as ndjson:
    #     for record in table:
    #         ndjson.write(json.dumps(record) + "\n")

    # Create PyArrow table from ndjson file
    pa_table = pa_json.read_json(json_path)

    # Store parquet table #TODO -> set proper data types in parquet file
    pq.write_table(pa_table, pq_path)

    # Remove temp ndjson file
    remove(json_path)
    # Remove temp folder if empty  #TODO -> inefficiently(?) creates and removes the folder each time the function is called
    if not listdir(temp_json_dir):
        rmdir(temp_json_dir)
    return pq_path


def set_gcp(config: Config, gcp_env: str) -> GcpProject:
    """Sets the desired GCP donciguration

    Parameters
    ----------
    config : Config
        `statline_bq.config.Config` object
    gcp_env : str
        String representing the deierd environment between ['dev', 'test', 'prod']

    Returns
    -------
    GcpProject
        [description]
    """
    gcp_env = gcp_env.lower()
    config_envs = {
        "dev": config.gcp.dev,
        "test": config.gcp.test,
        "prod": config.gcp.prod,
    }
    return config_envs[gcp_env]


def upload_to_gcs(
    dir: Path,
    source: str = "cbs",
    odata_version: str = None,
    id: str = None,
    config: Config = None,
    gcp_env: str = None,
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
    config: Config
        Config object holding GCP and local paths
    gcp_env: str
        determines which GCP configuration to use from config.gcp


    Returns
    -------
    gcs_folder: str
        The folder (=blob) into which the tables have been uploaded # TODO -> Return success/ fail code?/job ID
    """
    # Initialize Google Storage Client and get bucket according to gcp_env
    gcp = set_gcp(config=config, gcp_env=gcp_env)
    gcs = storage.Client(project=gcp.project_id)
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
    dataset_ref: str, descriptions: dict, config: Config = None, gcp_env: str = "dev"
) -> bigquery.Table:
    """Updates column descriptions of main table for existing BQ dataset

    Parameters
    ----------
    dataset_ref : str
        dataset reference where main table exists
    descriptions : dict
        dictionary holding column descriptions
    gcp : Gcp,
        Gcp object holding GCP configurations
    gcp_env : str, default = "dev"
        determines which GCP configuration to use from gcp

    Returns
    -------
    bigquery.Table
        The updated table
    """

    # Set GCP environmnet
    gcp = set_gcp(config=config, gcp_env=gcp_env)

    # Construct a BigQuery client object.
    client = bigquery.Client(project=gcp.project_id)

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
    except UnboundLocalError:
        print(
            f"No table located with 'TypedDataset' in its name for dataset {dataset_ref}"
        )
        return None

    # Create schema filed for column description
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
    config: Config = None,
    gcp_env: str = "dev",
    gcs_folder: str = None,
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
    gcp: Gcp
        A Gcp Class object, holding GCP parameters
    gcs_folder : str
        The GCS folder holding the coloumn descriptions json file

    Returns
    -------
    dict
        Dictionary holding column descriptions
    """
    gcp = set_gcp(config, gcp_env)
    client = storage.Client(project=gcp.project_id)
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
    config: Config = None,
    gcp_env: str = None,
    force: bool = False,
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
        The source of the dataset. Currently only "cbs" is relevant.

    config: Config object
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
    gcp = set_gcp(config, gcp_env)
    # Get all table-specific urls for the given dataset id
    urls = get_urls(id=id, odata_version=odata_version, third_party=third_party)
    # Get dataset metadata
    source_meta = get_metadata_cbs(urls=urls, odata_version=odata_version)
    gcp_meta = get_metadata_gcp(
        id=id, source=source, odata_version=odata_version, gcp=gcp
    )

    ## Check if upload is needed
    # Get dataset modified date from source metadata
    cbs_modified = get_from_meta(meta=source_meta, key="Modified")
    # Get datatset modified date from GCP metadata (set to None if force is True)
    gcp_modified = get_gcp_modified(gcp_meta, force)
    # Skip all process if modified date is the same in GCP and source (and Force is set to False)
    if skip_dataset(cbs_modified, gcp_modified, force):
        # if (cbs_modified is None or cbs_modified == gcp_modified) and (not force):
        print(cbs_modified)
        print(
            f"Skipping dataset {id} because the same dataset exists on GCP, with the same 'Modified' date"
        )
        print(f"Dataset {id} source last modified: {cbs_modified}")
        print(f"Dataset {id} gcp last modified: {gcp_modified}")
        return None

    # Create directory to store parquest files locally
    pq_dir = create_named_dir(
        id=id, odata_version=odata_version, source=source, config=config
    )
    # Fetch each table from urls, convert to parquet and store locally
    files_parquet = tables_to_parquet(
        id=id, urls=urls, odata_version=odata_version, source=source, pq_dir=pq_dir
    )
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
        config=config,
        gcp_env=gcp_env,
    )

    # Keep only file names
    file_names = get_file_names(files_parquet)
    # Create table in GBQ
    dataset_ref = gcs_to_gbq(
        id=id,
        source=source,
        odata_version=odata_version,
        config=config,
        gcs_folder=gcs_folder,
        file_names=file_names,
        gcp_env=gcp_env,
    )
    # Add column description to main table
    desc_dict = get_col_descs_from_gcs(
        id=id,
        source=source,
        odata_version=odata_version,
        config=config,
        gcp_env=gcp_env,
        gcs_folder=gcs_folder,
    )

    # Add column descriptions to main table (only relevant for v3, as v4 is a "long format")
    if odata_version == "v3":
        bq_update_main_table_col_descriptions(dataset_ref, desc_dict, config, gcp_env)

    return files_parquet  # TODO: return bq job ids


def get_urls(id: str, odata_version: str, third_party: bool = False) -> dict:
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
            for item in get_odata_v4(base_url[third_party])
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
    id: str, odata_version: str, source: str = "cbs", config: Config = None
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
    config: Config object
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

    # Get paths from config object
    root = Path(__file__).parent
    temp = root / Path(config.paths.temp)
    source_dir = temp / Path(getattr(config.paths, locals()["source"]))

    # Create placeholders for storage
    path = source_dir / Path(
        f"{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}/parquet"
    )
    path_to_named_dir = create_dir(path)
    return path_to_named_dir


def tables_to_parquet(
    id: str,
    urls: dict,
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
        The source of the dataset. Currently only "cbs" is relevant.
    pq_dir : Path or str
        The directory where the putput Parquet files are stored.

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

        # Get data from source
        table = get_odata(target_url=url, odata_version=odata_version)

        # Check if get_odata returned None (when link in CBS returns empty table, i.e. CategoryGroups in "84799NED" - seems only relevant for v3 only)
        if table is not None:

            # Convert to parquet
            pq_path = convert_table_to_parquet(table, table_name, pq_dir)

            # Add path of file to set
            files_parquet.add(pq_path)

    return files_parquet


def create_bq_dataset(
    id: str,
    source: str = "cbs",
    odata_version: str = None,
    description: str = None,
    gcp: Gcp = None,
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
    gcp: Gcp
        A Gcp Class object, holding GCP parameters

    Returns:
    dataset.dataset_id: str
        The id of the created BQ dataset
    """

    # Construct a BigQuery client object.
    client = bigquery.Client(project=gcp.project_id)

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
        print(f"Created dataset {client.project}.{dataset.dataset_id}")
    except exceptions.Conflict:
        print(f"Dataset {client.project}.{dataset.dataset_id} already exists")
    finally:
        return dataset.dataset_id


def check_bq_dataset(id: str, source: str, odata_version: str, gcp: Gcp = None) -> bool:
    """Check if dataset exists in BQ.

    Parameters:
        - id (str): the dataset id, i.e. '83583NED'
        - source (str): source to load data into
        - odata_version (str): "v3" or "v4" indicating the version
        - gcp (Gcp): a config object

    Returns:
        - True if exists, False if does not exists
    """
    client = bigquery.Client(project=gcp.project_id)

    dataset_id = f"{source}_{odata_version}_{id}"

    try:
        client.get_dataset(dataset_id)  # Make an API request.
        # print(f"Dataset {dataset_id} already exists")
        return True
    except exceptions.NotFound:
        # print(f"Dataset {dataset_id} is not found"
        return False


def delete_bq_dataset(
    id: str, source: str = "cbs", odata_version: str = None, gcp: Gcp = None
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
    gcp: Gcp
        A Gcp Class object, holding GCP parameters

    Returns
    -------
    None
    """

    # Construct a bq client
    client = bigquery.Client(project=gcp.project_id)

    # Set bq dataset id string
    dataset_id = f"{source}_{odata_version}_{id}"

    # Delete the dataset and its contents
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    return None


def gcs_to_gbq(
    id: str,
    source: str = "cbs",
    odata_version: str = None,
    third_party: bool = False,
    config: Config = None,
    gcs_folder: str = None,
    file_names: list = None,
    gcp_env: str = None,
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
        The source of the dataset. Currently only "cbs" is relevant.
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    third_party : bool, default=False
        Flag to indicate dataset is not originally from CBS. Set to true to use dataderden.cbs.nl as base url (not available in v4 yet).
    config : Config object
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
    gcp = set_gcp(config=config, gcp_env=gcp_env)
    # Get metadata
    meta_gcp = get_metadata_gcp(
        id=id, source=source, odata_version=odata_version, gcp=gcp
    )
    # Get dataset description
    description = None
    if meta_gcp:
        if odata_version == "v3":
            description = get_from_meta(meta_gcp, key="ShortDescription")
        elif odata_version == "v4":
            description = get_from_meta(meta_gcp, key="Description")
        else:
            raise ValueError("odata version must be either 'v3' or 'v4'")

    # Check if dataset exists and delete if it does TODO: maybe delete anyway (deleting uses not_found_ok to ignore error if does not exist)
    if check_bq_dataset(id=id, source=source, odata_version=odata_version, gcp=gcp):
        delete_bq_dataset(id=id, source=source, odata_version=odata_version, gcp=gcp)

    # Create a dataset in BQ
    dataset_id = create_bq_dataset(
        id=id,
        source=source,
        odata_version=odata_version,
        description=description,
        gcp=gcp,
    )
    # if not existing:
    # Skip?
    # else:
    # Handle existing dataset - delete and recreate? Repopulate? TODO

    # Initialize client
    client = bigquery.Client(project=gcp.project_id)

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
    config: Config = None,
    gcp_env: str = "dev",
    force: bool = False,
) -> None:
    gcp_env = gcp_env.lower()
    id = id.upper()
    if check_gcp_env(gcp_env):
        print(f"Processing dataset {id}")
        odata_version = check_v4(id=id, third_party=third_party)
        cbsodata_to_gbq(
            id=id,
            odata_version=odata_version,
            third_party=third_party,
            source=source,
            config=config,
            gcp_env=gcp_env,
            force=force,
        )
        print(
            f"Completed dataset {id}"
        )  # TODO - add response from google if possible (some success/failure flag)
        return None


if __name__ == "__main__":
    from statline_bq.config import get_config

    config = get_config("./statline_bq/config.toml")
    main("83583ned", config=config, gcp_env="dev", force=True)
    # main("83765NED", config=config, gcp_env="dev")
    # main(
    #     "40060NED",
    #     source="mlz",
    #     third_party=True,
    #     config=config,
    #     gcp_env="dev",
    #     force=False,
    # )

# from statline_bq.config import get_config

# config = get_config("./config.toml")

# description = get_description_v3(
#     "https://opendata.cbs.nl/ODataFeed/odata/{id}?$format=json"
# )
# print(description)

# gcs_to_gbq(
#     # id="835833NED",
#     source="cbs",
#     odata_version="v3",
#     gcp=config.gcp,
#     gcs_folder="cbs/v3/83583NED/20201126",
#     file_names=["cbs.v3.83583NED_Bedrijfsgrootte.parquet"],
# )
