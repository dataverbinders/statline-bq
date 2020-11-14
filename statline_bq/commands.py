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


def get_table_description_v4(url_table_properties: str) -> str:
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
    if "@odata.nextLink" in r:
        target_url = r["@odata.nextLink"]
    else:
        target_url = None

    # if more data exists continue to concat bag until complete
    while target_url:
        r = requests.get(target_url).json()
        if r["value"]:
            temp_bag = db.from_sequence(r["value"])
            bag = db.concat([bag, temp_bag])

        if "@odata.nextLink" in r:
            target_url = r["@odata.nextLink"]
        else:
            target_url = None

    return bag


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


def upload_to_gcs(
    dir: Path, schema: str, odata_version: str, id: str
):  # TODO: Take GCP object from config file
    """Uploads all files in a given directory to GCS, using the following structure:
    'project_name/bucket_name/schema/odata_version/id/YYYYMMDD/filename'
    
    Meant to be used for uploading all tables for a certain dataset retrieved from CBS API,
    and hence to upload (for example) into:
        'dataverbinders/dataverbinders/cbs/v4/83765NED/
    The following datasetsfiles:
        - cbs.82807NED_Observations.parquet
        - cbs.82807NED_PeriodenCodes.parquet
        - etc...
    
    Args:
        - dir: A Path object to a directory containing files to be uploaded
        - schema: schema to load data into
        - odata_version: 'v4' or 'v3', stating the version of the original odata retrieved
        - id (str): table ID like `83583NED`

    Returns:
        - None  # TODO -> Return success/ fail code?
    """
    # Initialize Google Storage Client, get bucket, set blob
    # gcs = storage.Client(project=GCP.project)  #when used with GCP Class
    gcs = storage.Client(project="dataverbinders-dev")
    gcs_bucket = gcs.get_bucket("dataverbinders-dev_test")
    gcs_folder = (
        f"{schema}/{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}"
    )
    # Upload file
    for pfile in os.listdir(dir):
        gcs_blob = gcs_bucket.blob(gcs_folder + "/" + pfile)
        gcs_blob.upload_from_filename(dir / pfile)

    return None  # TODO: Return suceess/failure??


def cbsodatav3_to_gcs(id, third_party=False, schema="cbs", credentials=None, GCP=None):
    """Load CBS odata v3 into Google Cloud Storage as parquet files.
    For given dataset id, following tables are uploaded into schema (taking `cbs` as default and `83583NED` as example):
    - cbs.83583NED_DataProperties: description of topics and dimensions contained in table
    - cbs.83583NED_DimensionName: separate dimension tables
    - cbs.83583NED_TypedDataSet: the TypedDataset
    - cbs.83583NED_CategoryGroups: grouping of dimensions
    See Handleiding CBS Open Data Services (v3)[^odatav3] for details.

    Args:
        - id (str): table ID like `83583NED`
        - third_party (boolean): 'opendata.cbs.nl' is used by default (False). Set to true for dataderden.cbs.nl
        - schema (str): schema to load data into
        - credentials: GCP credentials
        - GCP: config object
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

    # Create placeholders for storage
    files_parquet = set()
    pq_dir = Path(
        f"./temp/{schema}/{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}/parquet"
    )
    create_dir(pq_dir)

    # TableInfos is redundant --> use https://opendata.cbs.nl/ODataCatalog/Tables?$format=json
    # UntypedDataSet is redundant --> use TypedDataSet
    for key, url in [
        (k, v) for k, v in urls.items() if k not in ("TableInfos", "UntypedDataSet")
    ]:
        url = "?".join((url, "$format=json"))

        # Create table name to be used in GCS
        table_name = (
            f"{schema}.{id}_{key}"  # Maybe remove schema? is stated via folder.
        )

        # Get data from source
        table = get_odata_v3(url)

        # Convert to parquet
        pq_path = convert_table_to_parquet(table, table_name, pq_dir)

        # Add path of file to set
        files_parquet.add(pq_path)

    upload_to_gcs(pq_dir, schema, odata_version, id)

    return upload_to_gcs


def cbsodatav4_to_gcs(
    id: str,
    schema: str = "cbs",
    third_party=False,  # TODO - add GCP and credentials to arguments
):  # TODO -> Add GCS and Paths config objects
    """Load CBS odata v4 into Google Cloud Storage as Parquet.

    For a given dataset id, the following tables are ALWAYS uploaded into GCS
    (taking `cbs` as default and `83583NED` as example):
        - ``cbs.83583NED_Observations``: The actual values of the dataset
        - ``cbs.83583NED_MeasureCodes``: Describing the codes that appear in the Measure column of the Observations table. 
        - ``cbs.83583NED_Dimensions``: Information over the dimensions

    Additionally, this function will upload all other tables in the dataset, except `Properties`.
        These may include:
            - ``cbs.83583NED_MeasureGroups``: Describing the hierarchy of the Measures
        And, for each Dimensionlisted in the `Dimensions` table (i.e. `{Dimension_1}`)
            - ``cbs.83583NED_{Dimension_1}Codes
            - ``cbs.83583NED_{Dimension_1}Groups [IF IT EXISTS]

    See `Informatie voor ontwikelaars <https://dataportal.cbs.nl/info/ontwikkelaars>` for details.

    Args:
        - id (str): table ID like `83583NED`
        - third_party (boolean): 'opendata.cbs.nl' is used by default (False). Set to true for dataderden.cbs.nl
        - schema (str): schema to load data into
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
    # # Get the description of the data set  # Currently not used - maybe move to a different place?
    # data_set_description = get_table_description_v4(urls["Properties"])

    # gcs_bucket = gcs.bucket(GCP.bucket)

    # Create placeholders for storage
    files_parquet = set()
    pq_dir = Path(
        f"./temp/{schema}/{odata_version}/{id}/{datetime.today().date().strftime('%Y%m%d')}/parquet"
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
        table_name = (
            f"{schema}.{id}_{key}"  # Maybe remove schema? is stated via folder.
        )

        # Get data from source
        table = get_odata_v4(url)

        # Convert to parquet
        pq_path = convert_table_to_parquet(table, table_name, pq_dir)

        # Add path of file to set
        files_parquet.add(pq_path)

    # Upload to GCS

    upload_to_gcs(pq_dir, schema, odata_version, id)

    return files_parquet  # , data_set_description


def cbs_odata_to_gcs(  # TODO: Implement **args and **kwargs
    id: str,
    schema: str = "cbs",
    third_party=False,  # TODO - add GCP and credentials to arguments
):  # TODO -> Add GCS and Paths config objects):

    # Check if v4
    if check_v4(id=id, third_party=third_party):
        cbsodatav4_to_gcs(id=id, schema=schema, third_party=third_party)
    else:
        cbsodatav3_to_gcs(id=id, schema=schema, third_party=third_party)


if __name__ == "__main__":
    cbs_odata_to_gcs("83583NED")
