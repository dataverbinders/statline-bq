import subprocess
from typing import Union
import os
from pathlib import Path
from glob import glob
import requests
import json
import dask.bag as db
from datetime import datetime
from pyarrow import json as pa_json
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud import bigquery
from statline_bq.config import Config, Gcp
from google.api_core import exceptions

from prefect import task


def create_dir(path: Path) -> Path:
    """Checks whether path exists and is directory, and creates it if not.

    Args:
        - path (Path): path to check

    Returns:
        - Path: new directory
    """
    try:
        path = Path(path)
        if not (path.exists() and path.is_dir()):
            path.mkdir(parents=True)
        return path
    except TypeError as error:
        print(f"Error trying to find {path}: {error!s}")
        return None


def get_dataset_description_v3(url_table_infos):
    """Getting the descriptions of columns from a data set given in url_table_infos.
    Args:
        - url_table_infos (str): url of TableInfos table as String.
    Return:
        - dict{'column_name':'description'}
    """
    # Get JSON format of data set.
    url_table_infos = "?".join((url_table_infos, "$format=json"))

    data_info = requests.get(url_table_infos).json()  # Is of type dict()

    data_info_values = data_info["value"]  # Is of type list

    # Get short description as text
    descritption = data_info_values[0]["ShortDescription"]

    # dict_description = {}

    # # Only dict's containing the key 'Key' has information about table columns.
    # for i in data_info_values:
    #     if i["Key"] != "":
    #         # Make description shorter, since BigQuery only allows 1024 characters
    #         if i["Description"] is not None and len(i["Description"]) > 1024:
    #             i["Description"] = i["Description"][:1021] + "..."

    #         dict_description[i["Key"]] = i["Description"]

    return descritption


def get_dataset_description_v4(url_table_properties: str) -> str:
    """Gets table description of a table in CBS odata V4.

    Args:
        - url_table_properties (str): url of the data set `Properties`

    Returns:
        - String: table_description
    """
    r = requests.get(url_table_properties).json()
    return r["Description"]


def get_odata_v4_curl(  # TODO -> CURL command does not process url with ?$skip=100000 ath the end - returns same data as first link
    target_url: str,
):  # TODO -> How to define Bag for type hinting? (https://docs.python.org/3/library/typing.html#newtype)
    """Gets a table from a specific url for CBS Odata v4.

    Args:
        - url_table_properties (str): url of the table

    Returns:
        - data (Dask bag): all data received from target url as json type, in a Dask bag
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
    if "@odata.nextLink" in r:
        target_url = r["@odata.nextLink"]
    else:
        target_url = None

    # if more data exists continue to concat bag until complete
    while target_url:
        subprocess.run(["curl", "-fL", "-o", temp_file, target_url])
        # Parse json file as dict
        with open(temp_file) as f:
            r = json.load(f)
        temp_bag = db.from_sequence(r["value"])
        bag = db.concat([bag, temp_bag])

        if "@odata.nextLink" in r:
            target_url = r["@odata.nextLink"]
        else:
            target_url = None

    return bag


def check_v4(id: str, third_party: bool = False) -> bool:
    """
    Check whether a certain CBS table exists as odata v4

    Args:
        - id (str): table ID like `83583NED`
        - third_party (boolean): 'odata4.cbs.nl' is used by default (False). Set to true for dataderden.cbs.nl (not available in v4 yet)        

    Returns:
        - v4 (bool): True if exists as odata v4, False otherwise
    """
    base_url = {
        True: None,  # currently no IV3 links in ODATA V4,
        False: f"https://odata4.cbs.nl/CBS/{id}",
    }
    r = requests.get(base_url[third_party])
    if (
        r.status_code == 200
    ):  # TODO: Is this the best check to use? Maybe if not 404? Or something else?
        v4 = True
    else:
        v4 = False
    return v4


@task
def get_odata_v3(
    target_url: str,
):  # TODO -> How to define Bag for type hinting? (https://docs.python.org/3/library/typing.html#newtype)
    # TODO -> Change requests to CURL command
    """Gets a table from a specific url for CBS Odata v3.

    Args:
        - target_url (str): url of the table

    Returns:
        - data (Dask bag): all data received from target url as json type, in a Dask bag
    """
    # First call target url and get json formatted response as dict
    r = requests.get(target_url).json()
    # Create Dask bag from dict (check if not empty field)
    if r["value"]:
        bag = db.from_sequence(r["value"])  # TODO -> define npartitions?

    # check if more data exists
    if "odata.nextLink" in r:
        target_url = r["odata.nextLink"]
    else:
        target_url = None

    # if more data exists continue to concat bag until complete
    while target_url:
        r = requests.get(target_url).json()
        if r["value"]:
            temp_bag = db.from_sequence(r["value"])
            bag = db.concat([bag, temp_bag])

        if "odata.nextLink" in r:
            target_url = r["odata.nextLink"]
        else:
            target_url = None

    if "bag" in locals():
        return bag
    else:
        return None


def get_odata_v4(
    target_url: str,
):  # TODO -> How to define Bag for type hinting? (https://docs.python.org/3/library/typing.html#newtype)
    # TODO -> Change requests to CURL command
    """Gets a table from a specific url for CBS Odata v4.

    Args:
        - target_url (str): url of the table

    Returns:
        - data (Dask bag): all data received from target url as json type, in a Dask bag
    """
    print(f"Fetching from {target_url}")
    # First call target url and get json formatted response as dict
    r = requests.get(target_url).json()
    # Create Dask bag from dict
    bag = db.from_sequence(r["value"])  # TODO -> define npartitions?

    # check if more data exists
    if "@odata.nextLink" in r:
        target_url = r["@odata.nextLink"]
    else:
        target_url = None

    # if more data exists continue to concat bag until complete
    while target_url:
        r = requests.get(target_url).json()
        temp_bag = db.from_sequence(r["value"])
        bag = db.concat([bag, temp_bag])

        if "@odata.nextLink" in r:
            target_url = r["@odata.nextLink"]
        else:
            target_url = None

    return bag


def convert_table_to_parquet(
    bag, file_name: str, out_dir: Union[str, Path]
) -> Path:  # (TODO -> IS THERE A FASTER/BETTER WAY??)
    """ Converts a table to a parquet form and stores it on disk

    Args:
        - bag: a Dask bag holding with nested dicts in json format  # TODO -> not sure this is a correct description
        - file_name: name of the file to store on disl
        - out_dir: path to directory to store file

    """
    # create directories to store files
    out_dir = Path(out_dir)
    temp_ndjson_dir = Path("./temp/ndjson")
    create_dir(temp_ndjson_dir)
    create_dir(out_dir)

    # File path to dump table as ndjson
    ndjson_path = Path(f"{temp_ndjson_dir}/{file_name}.ndjson")
    # File path to create as parquet file
    pq_path = Path(f"{out_dir}/{file_name}.parquet")

    # Dump each bag partition to json file
    bag.map(json.dumps).to_textfiles(temp_ndjson_dir / "*.json")
    # Get all json file names with path
    filenames = sorted(glob(str(temp_ndjson_dir) + "/*.json"))
    # Append all jsons into a single file  ## Also possible to use Dask Delayed here https://stackoverflow.com/questions/39566809/writing-dask-partitions-into-single-file
    with open(ndjson_path, "w+") as ndjson:
        for fn in filenames:
            with open(fn) as f:
                ndjson.write(f.read())
            os.remove(fn)

    # # Dump as ndjson format
    # with open(ndjson_path, 'w+') as ndjson:
    #     for record in table:
    #         ndjson.write(json.dumps(record) + "\n")

    # Create PyArrow table from ndjson file
    pa_table = pa_json.read_json(ndjson_path)

    # Store parquet table #TODO -> set proper data types in parquet file
    pq.write_table(pa_table, pq_path)

    # Remove temp ndjson file
    os.remove(ndjson_path)
    # Remove temp folder if empty  #TODO -> inefficiently(?) creates and removes the folder each time the function is called
    if not os.listdir(temp_ndjson_dir):
        os.rmdir(temp_ndjson_dir)
    return pq_path


def upload_to_gcs(dir: Path, source: str, odata_version: str, id: str, gcp: Gcp):
    """Uploads all files in a given directory to GCS, using the following structure:
    'project_name/bucket_name/source/odata_version/id/YYYYMMDD/filename'
    
    Meant to be used for uploading all tables for a certain dataset retrieved from CBS API,
    and hence to upload (for example) into:
        'dataverbinders/dataverbinders/cbs/v4/83765NED/
    The following datasetsfiles:
        - cbs.82807NED_Observations.parquet
        - cbs.82807NED_PeriodenCodes.parquet
        - etc...
    
    Args:
        - dir: A Path object to a directory containing files to be uploaded
        - source: source to load data into
        - odata_version: 'v4' or 'v3', stating the version of the original odata retrieved
        - id (str): table ID like `83583NED`

    Returns:
        - None  # TODO -> Return success/ fail code?
    """
    # Initialize Google Storage Client, get bucket, set blob
    gcs = storage.Client(
        project=gcp.dev.project_id
    )  # TODO -> handle dev, test and prod appropriatley
    gcs_bucket = gcs.get_bucket(gcp.dev.bucket)
    gcs_folder = (
        f"{source}/{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}"
    )
    # Upload file
    for pfile in os.listdir(dir):
        gcs_blob = gcs_bucket.blob(gcs_folder + "/" + pfile)
        gcs_blob.upload_from_filename(dir / pfile)

    return gcs_folder  # TODO: Also return suceess/failure??


def cbsodatav3_to_gbq(
    id: str, third_party: bool = False, source: str = "cbs", config: Config = None
):
    """Load CBS odata v3 into Google Cloud Storage as parquet files. Then, it creates a new permanenet
    table in Google Big Query, linked to the dataset.

    In GCS, the following "folders" and filenames' structure is used:

        - [project/]{bucket_name}/{source}/{version}/{dataset_id}/{date_of_upload}/{source}.{version}.{dataset_id}_{table_name}.parquet

        for example:

        - [dataverbinders/]dataverbinders/cbs/v3/84286NED/20201125/cbs.v3.84286NED_TypedDataSet.parquet
    
    In GBQ, the following structure and table names are used:

        - [project/]/{source}_{version}_{dataset_id}/{dataset_id}/{table_name}

        for example:

        - [dataverbinders/]/cbs_v3_83765NED/83765NED_Observations

    For given dataset id, following tables are uploaded into GCS and linked in GBQ (taking `cbs` as default and `83583NED` as example):
    - cbs.v3.83583NED_DataProperties: description of topics and dimensions contained in table
    - cbs.v3.83583NED_DimensionName: separate dimension tables
    - cbs.v3.83583NED_TypedDataSet: the TypedDataset
    - cbs.v3.83583NED_CategoryGroups: grouping of dimensions
    See Handleiding CBS Open Data Services (v3)[^odatav3] for details.

    Args:
        - id (str): table ID like `83583NED`
        - third_party (boolean): 'opendata.cbs.nl' is used by default (False). Set to true for dataderden.cbs.nl
        - source (str): source to load data into
        - config: config object

    Return:
        - List[google.cloud.bigquery.job.LoadJob]

    [^odatav3]: https://www.cbs.nl/-/media/statline/documenten/handleiding-cbs-opendata-services.pdf
    """
    odata_version = "v3"

    base_url = {
        True: f"https://dataderden.cbs.nl/ODataFeed/odata/{id}?$format=json",
        False: f"https://opendata.cbs.nl/ODataFeed/odata/{id}?$format=json",
    }
    urls = {
        item["name"]: item["url"]
        for item in requests.get(base_url[third_party]).json()["value"]
    }

    # Get paths from config object
    root = Path.home() / Path(config.paths.root)
    temp = root / Path(config.paths.temp)

    # Create placeholders for storage
    files_parquet = set()
    pq_dir = temp / Path(
        f"{source}/{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}/parquet"
    )
    create_dir(pq_dir)

    # TableInfos is redundant --> use https://opendata.cbs.nl/ODataCatalog/Tables?$format=json
    # UntypedDataSet is redundant --> use TypedDataSet
    for key, url in [
        (k, v) for k, v in urls.items() if k not in ("TableInfos", "UntypedDataSet")
    ]:
        url = "?".join((url, "$format=json"))

        # Create table name to be used in GCS
        table_name = f"{source}.{odata_version}.{id}_{key}"

        # Get data from source
        table = get_odata_v3(url)

        # Check if get_odata_v3 returned None (when link in CBS returns empty table, i.e. CategoryGroups in "84799NED")
        if table is not None:

            # Convert to parquet
            pq_path = convert_table_to_parquet(table, table_name, pq_dir)

            # Add path of file to set
            files_parquet.add(pq_path)

    # Get the description of the data set
    description_text = get_dataset_description_v3(urls["TableInfos"])

    description_file = pq_dir / Path(f"{source}.{odata_version}.{id}_Description.txt")
    with open(description_file, "w+") as f:
        f.write(description_text)

    # Upload to GCS
    gcs_folder = upload_to_gcs(pq_dir, source, odata_version, id, config.gcp)

    # Keep only names
    file_names = [
        Path(path.name) for path in files_parquet
    ]  # TODO: Does it matter we change a set to a list here?
    # Create table in GBQ
    gcs_to_gbq(
        id=id,
        source=source,
        odata_version=odata_version,
        third_party=third_party,
        gcp=config.gcp,
        gcs_folder=gcs_folder,
        file_names=file_names,
    )

    return files_parquet


def cbsodatav4_to_gbq(
    id: str, source: str = "cbs", third_party: bool = False, config: Config = None
):  # TODO -> Add Paths config objects
    """Load CBS odata v4 into Google Cloud Storage as parquet files. Then, it creates a new permanenet
    table in Google Big Query, linked to the dataset.

    In GCS, the following "folders" and filenames' structure is used:

        - [project/]{bucket_name}/{source}/{version}/{dataset_id}/{date_of_upload}/{source}.{version}.{dataset_id}_{table_name}.parquet

        for example:

        - [dataverbinders/]dataverbinders/cbs/v4/83765NED/20201125/cbs.v4.83765NED_Observationst.parquet
    
    In GBQ, the following structure and table names are used:

        - [project/]/{source}_{version}_{dataset_id}/{dataset_id}/{table_name}

        for example:

        - [dataverbinders/]/cbs_v4_83765NED/83765NED_Observations

    For a given dataset id, the following tables are ALWAYS uploaded into GCS and linked in GBQ
    (taking `cbs` as default and `83765NED` as example):
        - ``cbs.v4.83765NED_Observations``: The actual values of the dataset
        - ``cbs.v4.83765NED_MeasureCodes``: Describing the codes that appear in the Measure column of the Observations table. 
        - ``cbs.v4.83765NED_Dimensions``: Information over the dimensions

    Additionally, this function will upload all other tables in the dataset, except `Properties`.
        These may include:
            - ``cbs.v4.83765NED_MeasureGroups``: Describing the hierarchy of the Measures
        And, for each Dimensionlisted in the `Dimensions` table (i.e. `{Dimension_1}`)
            - ``cbs.v4.83765NED_{Dimension_1}Codes
            - ``cbs.v4.83765NED_{Dimension_1}Groups [IF IT EXISTS]

    See `Informatie voor ontwikelaars <https://dataportal.cbs.nl/info/ontwikkelaars>` for details.

    Args:
        - id (str): table ID like `83583NED`
        - third_party (boolean): 'opendata.cbs.nl' is used by default (False). Set to true for dataderden.cbs.nl
        - source (str): source to load data into
        - credentials: GCP credentials
        - GCP: config object
        - paths: config object for output directory

    Return:
        - Set: Paths to Parquet files
        - String: table_description
    """
    odata_version = "v4"

    base_url = {
        True: None,  # currently no IV3 links in ODATA V4,
        False: f"https://odata4.cbs.nl/CBS/{id}",
    }
    urls = {
        item["name"]: base_url[third_party] + "/" + item["url"]
        for item in get_odata_v4(base_url[third_party])
    }

    # Get paths from config object
    root = Path.home() / Path(config.paths.root)
    temp = root / Path(config.paths.temp)

    # Create placeholders for storage
    files_parquet = set()
    pq_dir = temp / Path(
        f"{source}/{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}/parquet"
    )
    create_dir(pq_dir)

    ## Download datasets from CBS and converting to Parquet

    # Iterate over all tables related to dataset, excepet Properties (TODO -> double check that it is redundandt)
    for key, url in [
        (k, v)
        for k, v in urls.items()
        if k not in ("Properties")
        # TEMP - FOR QUICKER TESTS OMIT OBSERVATIONS FROM PROCESSING
        # (k, v) for k, v in urls.items() if k not in ("Observations", "Properties")
    ]:

        # Create table name to be used in GCS
        table_name = f"{source}.{odata_version}.{id}_{key}"

        # Get data from source
        table = get_odata_v4(url)

        # Convert to parquet
        pq_path = convert_table_to_parquet(table, table_name, pq_dir)

        # Add path of file to set
        files_parquet.add(pq_path)

    # Get the description of the data set
    description_text = get_dataset_description_v4(urls["Properties"])

    description_file = pq_dir / Path(f"{source}.{odata_version}.{id}_Description.txt")
    with open(description_file, "w+") as f:
        f.write(description_text)

    # Upload to GCS
    gcs_folder = upload_to_gcs(pq_dir, source, odata_version, id, config.gcp)

    # Keep only names
    file_names = [
        Path(path.name) for path in files_parquet
    ]  # TODO: Does it matter we change a set to a list here?
    # Create table in GBQ
    gcs_to_gbq(
        id=id,
        source=source,
        odata_version=odata_version,
        third_party=third_party,
        gcp=config.gcp,
        gcs_folder=gcs_folder,
        file_names=file_names,
    )

    return files_parquet  # , data_set_description


def create_bq_dataset(
    id: str, source: str, odata_version: str, description: str = None, gcp: Gcp = None,
) -> str:
    """Creates a dataset in Google Big Query. If dataset exists already exists,
    does nothing.

    Args:
        - id (str): string to be used as the dataset id
        - source (str): source to load data into
        - odata_version (str): 'v3' or 'v4' indicating the version
        - description (str): description for the dataset
        - gcp (Gcp): config object

    Returns:
        - string with the dataset id
        - existing flag indicating whether the dataset already existed when trying to create it
    """
    # Construct a BigQuery client object.
    client = bigquery.Client(project=gcp.dev.project_id)

    # Set dataset_id to the ID of the dataset to create.
    dataset_id = f"{client.project}.{source}_{odata_version}_{id}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = gcp.dev.location

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

    Args:
        - id (str): the dataset id, i.e. '83583NED'
        - source (str): source to load data into
        - odata_version (str): 'v3' or 'v4' indicating the version
        - gcp (Gcp): a config object

    Returns:
        - True if exists, False if does not exists
    """
    client = bigquery.Client(project=gcp.dev.project_id)

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

        Args:
        - id (str): the dataset id, i.e. '83583NED'
        - source (str): source to load data into
        - odata_version (str): 'v3' or 'v4' indicating the version
        - gcp (Gcp): a config object

        Returns:
        - None
    """
    # Construct a bq client
    client = bigquery.Client(project=gcp.dev.project_id)

    # Set bq dataset id string
    dataset_id = f"{source}_{odata_version}_{id}"

    # Delete the dataset and its contents
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    return None


def get_description_from_gcs(
    id: str,
    source: str = "cbs",
    odata_version: str = None,
    gcp: Gcp = None,
    gcs_folder: str = None,
) -> str:
    """Gets previsouly uploaded dataset description from GCS. The description
    should exist in the following file, under the following structure:

        - {project}/{bucket}/{source}/{odata_version}/{id}/{YYYYMMDD}/{source}.{odata_version}.{id}_Description
        For example:
        - dataverbinders-dev/cbs/v4/83765NED/20201127/cbs.v4.83765NED_Description.txt

    Args:
        - id (str): table ID like `83583NED`
        - source (str): source to load data into
        - odata_version (str): 'v3' or 'v4' indicating the version
        - gcp: config object
        - gcs_folder (str): "folder" path in gcs
    """
    client = storage.Client(project=gcp.dev.project_id)
    bucket = client.get_bucket(gcp.dev.bucket)
    blob = bucket.get_blob(
        f"{gcs_folder}/{source}.{odata_version}.{id}_Description.txt"
    )
    return str(blob.download_as_string().decode("utf-8"))


def gcs_to_gbq(
    id: str,
    source: str = "cbs",
    odata_version: str = None,
    third_party: bool = False,
    gcp: Gcp = None,
    gcs_folder: str = None,
    file_names: list = None,
):
    """Creates a dataset (if does not exist) in Google Big Query, and underneath
    creates permanent tables linked to parquet file stored in Google Storage. If
    dataset exists, removes it and recreates it with most up to date uploaded files (?) # TODO: Is this the best logic?

    Args:
        - id (str): table ID like `83583NED`
        - source (str): source to load data into
        - odata_version (str): 'v3' or 'v4' indicating the version
        - third_party (boolean): 'opendata.cbs.nl' is used by default (False). Set to true for dataderden.cbs.nl
        - gcp (Gcp): config object
        - gcs_folder (str): "folder" path in gcs
        - file_names (list): list with file names uploaded to gcs TODO: change to get file names from gcs?

    Returns:
        - TODO
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

    # Get description text from txt file
    description = get_description_from_gcs(
        id=id,
        source=source,
        odata_version=odata_version,
        gcp=gcp,
        gcs_folder=gcs_folder,
    )

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
    client = bigquery.Client(project=gcp.dev.project_id)

    # Configure the external data source
    # dataset_id = f"{source}_{odata_version}_{id}"
    dataset_ref = bigquery.DatasetReference(gcp.dev.project_id, dataset_id)

    # Loop over all files related to this dataset id
    for name in file_names:
        # Configure the external data source per table id
        table_id = str(name).split(".")[2]
        table = bigquery.Table(dataset_ref.table(table_id))

        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [
            f"https://storage.cloud.google.com/{gcp.dev.bucket}/{gcs_folder}/{name}"  # TODO: Handle dev/test/prod?
        ]
        table.external_data_configuration = external_config
        # table.description = description

        # Create a permanent table linked to the GCS file
        table = client.create_table(table, exists_ok=True)  # BUG: error raised
    return None  # TODO: Return success/failure/info about table?


@task
def cbs_odata_to_gbq(
    id: str, source: str = "cbs", third_party: bool = False, config: Config = None,
):  # TODO -> Add Paths config object):

    print(f"Processing dataset {id}")
    # Check if v4
    if check_v4(id=id, third_party=third_party):
        cbsodatav4_to_gbq(id=id, source=source, third_party=third_party, config=config)
    else:
        cbsodatav3_to_gbq(id=id, source=source, third_party=third_party, config=config)
    print(
        f"Completed dataset {id}"
    )  # TODO - add response from google if possible (some success/failure flag)
    return None


if __name__ == "__main__":
    cbs_odata_to_gbq("83583NED")

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
