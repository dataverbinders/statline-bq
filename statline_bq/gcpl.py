# Module containing functions that interact with the Google Cloud Platform
from typing import Union
import logging
from os import listdir
from pathlib import Path
import json
from datetime import datetime

from google.cloud import storage
from google.cloud import bigquery
from google.api_core import exceptions
from google.oauth2.credentials import Credentials
from box import Box

from statline_bq.log import logdec

logger = logging.getLogger(__name__)


@logdec
def _set_gcp(config: Box, gcp_env: str, source: str) -> Box:
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


@logdec
def _get_latest_folder(
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


@logdec
def _get_metadata_gcp(
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
    gcs_folder = _get_latest_folder(gcs_folder, gcp)
    blob = bucket.get_blob(f"{gcs_folder}/{source}.{odata_version}.{id}_Metadata.json")
    # If no such blob exists, the error will be caught by @logdec, and None would be returned instead
    meta = json.loads(blob.download_as_string())
    return meta


# @logdec
# def _get_gcp_modified(gcp_meta: dict, force: bool = False) -> Union[str, None]:
#     """Gets the "modified" field from a dict containing a dataset's metadata.

#     Parameters
#     ----------
#     gcp_meta : dict
#         A dataset's metadata
#     force : bool, optional
#         [description], by default False

#     Returns
#     -------
#     Union[str, None]
#         [description]
#     """
#     # TODO: can we remove `force` from here, as it is handled in skip_dataset? - run unit tests to verify
#     if not force:
#         try:
#             gcp_modified = gcp_meta.get("Modified")
#         except AttributeError:
#             gcp_modified = None
#     else:
#         gcp_modified = None
#     return gcp_modified


@logdec
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


@logdec
def _bq_update_main_table_col_descriptions(
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
    # write as standard SQL format
    main_table_id = dataset_ref.dataset_id + "." + main_table_id
    main_table = client.get_table(main_table_id)

    # Create SchemaField for column description
    new_schema = []
    for field in main_table.schema:
        schema_dict = field.to_api_repr()
        schema_dict["description"] = descriptions.get(schema_dict["name"])
        new_schema.append(bigquery.SchemaField.from_api_repr(schema_dict))

    main_table.schema = new_schema

    table = client.update_table(main_table, ["schema"])

    return table


@logdec
def _get_col_descs_from_gcs(
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


@logdec
def _create_bq_dataset(
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
    except exceptions.Conflict:
        pass
    finally:
        return dataset.dataset_id


@logdec
def _check_bq_dataset(
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
        client.get_dataset(dataset_id)
        return True
    except exceptions.BadRequest:
        return False


@logdec
def _delete_bq_dataset(
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


@logdec
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
    meta_gcp = _get_metadata_gcp(
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
    if _check_bq_dataset(
        id=id,
        source=source,
        odata_version=odata_version,
        gcp=gcp,
        credentials=credentials,
    ):
        _delete_bq_dataset(
            id=id,
            source=source,
            odata_version=odata_version,
            gcp=gcp,
            credentials=credentials,
        )

    # Create a dataset in BQ
    dataset_id = _create_bq_dataset(
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
