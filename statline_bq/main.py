from typing import Union, Tuple
from pathlib import Path
import logging
from shutil import rmtree

import pyarrow.parquet as pq
from box import Box
from google.oauth2.credentials import Credentials

from statline_bq.statline import (
    _check_v4,
    _get_urls,
    get_metadata_cbs,
    _get_main_table_shape,
    dataset_to_parquet,
    _get_column_descriptions,
)
from statline_bq.gcpl import (
    _get_metadata_gcp,
    _set_gcp,
    upload_to_gcs,
    gcs_to_gbq,
    _get_col_descs_from_gcs,
    _bq_update_main_table_col_descriptions,
)
from statline_bq.utils import (
    _check_gcp_env,
    _create_dir,
    _create_named_dir,
    dict_to_json_file,
    _get_file_names,
)
from statline_bq.log import logdec

logger = logging.getLogger(__name__)


@logdec
def _skip_dataset(
    id: str,
    source: str,
    third_party: bool,
    odata_version: str,
    gcp: Box,
    force: bool,
    credentials: Credentials,
) -> bool:
    """Checks whether a dataset should be skipped, given the "last modified" dates from the CBS version and the GCP version.

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED"
    source: str
        The source of the dataset.
    third_party: bool
        Flag to indicate dataset is not originally from CBS.
    odata_version: str
        version of the odata for this dataset - must be either "v3" or "v4".
    force : bool
        flag to signal forcing the processing of a dataset even if the dates match.
    credentials : Credentials
        Google oauth2 credentials

    Returns
    -------
    bool
        True if the dates are identical, False if not or if force=True
    """
    # Don't skip if force=True
    if force:
        return False
    source_meta = get_metadata_cbs(
        id=id, third_party=third_party, odata_version=odata_version
    )
    gcp_meta = _get_metadata_gcp(
        id=id,
        source=source,
        odata_version=odata_version,
        gcp=gcp,
        credentials=credentials,
    )
    cbs_modified = source_meta.get("Modified")
    gcp_modified = gcp_meta.get("Modified")
    # Don't skip if one of the dates is None
    if not (cbs_modified or gcp_modified):
        return False
    # Skip if the last modified data from CBS is the same as that for the GCP one
    elif cbs_modified == gcp_modified:
        return True
    else:
        return False


@logdec
def _cbsodata_to_local(
    id: str,
    odata_version: str,
    third_party: str = False,
    source: str = "cbs",
    config: Box = None,
    out_dir: Union[Path, str] = None,
) -> Tuple:  # TODO change return value
    """Downloads a CBS dataset and stores it locally as parquet (and json) files.

    Retrieves a given dataset from CBS, and converts it locally to Parquet. The
    Parquet files are stored locally. The dataset's metadata and the main table's
    column descriptions are stored as json files in the same folder with the parquet
    files.

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

    out_dir: Path or str
        Local folder to place the output files. If set to None, creates a folder
        within the temp directories of the OS based on the dataset source and id.

    Returns
    -------
    pq_dir : Path
        path to directory containing local converted files
    
    files_parquet : set of Paths
        A set with paths of local parquet files
    """
    # Get all table-specific urls for the given dataset id
    urls = _get_urls(id=id, odata_version=odata_version, third_party=third_party)
    # Get dataset metadata
    source_meta = get_metadata_cbs(
        id=id, third_party=third_party, odata_version=odata_version
    )
    # Create directory to store parquest files locally
    if out_dir:
        out_dir = Path(out_dir)
        pq_dir = _create_dir(out_dir / "parquet")
    else:
        pq_dir = _create_named_dir(
            id=id, odata_version=odata_version, source=source, config=config
        )
    # Set main table shape to use for parallel fetching later
    main_table_shape = _get_main_table_shape(source_meta)
    # Fetch each table from urls, convert to parquet and store locally
    files_parquet = dataset_to_parquet(
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
    if odata_version == "v3":
        col_descriptions = _get_column_descriptions(urls, odata_version=odata_version)
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
    return pq_dir, files_parquet


@logdec
def _cbsodata_to_gcs(
    id: str,
    odata_version: str,
    third_party: str = False,
    source: str = "cbs",
    config: Box = None,
    gcp: Box = None,
    credentials: Credentials = None,
) -> set:  # TODO change return value
    """Loads a CBS dataset as parquet files in Google Storage.

    Retrieves a given dataset id from CBS, and converts it locally to Parquet. The
    Parquet files are uploaded to Google Cloud Storage.

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

    credentials : Credentials, default = None
        Google oauth2 credentials

    Returns
    -------
    files_parquet: set of Paths
        A set with paths of local parquet files # TODO: replace with BQ job ids
    """

    # download data and store locally as parquet
    pq_dir, files_parquet = _cbsodata_to_local(
        id=id,
        odata_version=odata_version,
        third_party=third_party,
        source=source,
        config=config,
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

    # Remove all local files created for this process
    rmtree(pq_dir.parents[1])

    return files_parquet  # TODO: return gcs job ids


@logdec
def _cbsodata_to_gbq(
    id: str,
    odata_version: str,
    third_party: str = False,
    source: str = "cbs",
    config: Box = None,
    gcp: Box = None,
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
    """
    # download data and store locally as parquet
    pq_dir, files_parquet = _cbsodata_to_local(
        id=id,
        odata_version=odata_version,
        third_party=third_party,
        source=source,
        config=config,
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
    file_names = _get_file_names(files_parquet)
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
    # Add column descriptions to main table (only relevant for v3, as v4 is a "long format")
    if odata_version == "v3":
        desc_dict = _get_col_descs_from_gcs(
            id=id,
            source=source,
            odata_version=odata_version,
            gcp=gcp,
            gcs_folder=gcs_folder,
            credentials=credentials,
        )
        _bq_update_main_table_col_descriptions(
            dataset_ref=dataset_ref,
            descriptions=desc_dict,
            gcp=gcp,
            credentials=credentials,
        )

    # Remove all local files created for this process
    rmtree(pq_dir.parents[1])

    return files_parquet  # TODO: return bq job ids


@logdec
def main(
    id: str,
    source: str = "cbs",
    third_party: bool = False,
    config: Box = None,
    gcp_env: str = "dev",
    endpoint: str = "bq",
    local_dir: Union[str, Path] = None,
    force: bool = False,
    credentials: Credentials = None,
) -> Path:
    """Downloads a CBS dataset, converts it to parquet and stores it either locally or on GCP.

    Retrieves a given dataset id from CBS, and converts it to Parquet. The Parquet
    files are stored locally, or only uploaded to Google Cloud Storage, or uploaded
    to Google Cloud Storage and a dataset is created in Google BigQuery, under which
    each permanenet tables are nested,linked to the Parquet files - each being a
    table of the dataset.

    Parameters
    ---------
    id: str
        CBS Dataset id, i.e. "83583NED"

    source: str, default="cbs"
        The source of the dataset. i.e "mlz" or "cbs". If third_party=False, source
        must be "cbs". Otherwise, source must not be "cbs".

    third_party: bool, default=False
        Flag to indicate dataset is not originally from CBS. Set to true
        to use dataderden.cbs.nl as base url (not available in v4 yet).

    config: Box,
        Config object holding GCP and local paths

    gcp_env: str, default="dev"
        Determines which GCP configuration to use from config.gcp

    endpoint: str, default="bq"
        Determines the end result of the function.
        * 'local' stores the parquet files locally
        * 'gcs' uploads the parquet file to Google Cloud Storage
        * 'bq' uploads the parquet file to Google Cloud Storage and creates a linked dataset in BigQuery
    
    local_dir: str or Path, default=None
        If endpoint='local', determines the folder to store parquet files. If set to None,
        creates a folder within the temp directories of the OS based on the dataset source and id.
    
    force: bool, default=False
        If set to True, processes datasets, even if Modified dates are
        identical in source and target locations.

    credentials: Credentials, default=None
        Google oauth2 credentials, passed to google-cloud clients. If not passed,
        falls back to the default inferred from the environment.

    Returns
    -------
    files_parquet: set of Paths
        A set with paths of local parquet files # TODO: change and update

    Examples
    --------
    >>> # Storing files locally
    >>> from statline_bq.utils import check_v4, cbsodata_to_gbq
    >>> from statline_bq.config import get_config
    >>> id = "83583NED"
    >>> config = get_config("path/to/config.file")
    >>> out_dir = main(
    ...     id=id,
    ...     config=config,
    ...     gcp_env="dev",
    ...     endpoint="local",
    ...     local_dir = "./my_parquet_folder/",
    ...     force=False
    ... )
    >>> listdir(out_dir)
    ['cbs.v3.83583NED_DataProperties.parquet', 'cbs.v3.83583NED_BedrijfstakkenBranchesSBI2008.parquet', 'cbs.v3.83583NED_ColDescriptions.json', 'cbs.v3.83583NED_Bedrijfsgrootte.parquet', 'cbs.v3.83583NED_TypedDataSet.parquet', 'cbs.v3.83583NED_CategoryGroups.parquet', 'cbs.v3.83583NED_Perioden.parquet', 'cbs.v3.83583NED_Metadata.json']


    >>> # Creating a dataset in Google BigQuery
    >>> from statline_bq.utils import check_v4, cbsodata_to_gbq
    >>> from statline_bq.config import get_config
    >>> id = "83583NED"
    >>> config = get_config("path/to/config.file")
    >>> local_folder = main(
    ...     id=id,
    ...     config=config,
    ...     gcp_env="prod",
    ...     endpoint="bq",
    ...     force=False
    ... )
    

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
    if third_party and source == "cbs":
        raise ValueError(
            "A third-party dataset cannot have 'cbs' as source: please provide correct 'source' parameter"
        )
    odata_version = _check_v4(id=id, third_party=third_party)
    if endpoint not in ("local", "bq", "gcs"):
        raise ValueError("endpoint must be one of ['bq', 'gcs', 'local']")
    # no gcp interaction - only local
    elif endpoint == "local":
        _, files_parquet = _cbsodata_to_local(
            id=id,
            odata_version=odata_version,
            third_party=third_party,
            source=source,
            config=config,
            out_dir=local_dir,
        )
    # interaction with gcp
    else:
        gcp_env = gcp_env.lower()
        _check_gcp_env(gcp_env)
        gcp = _set_gcp(config, gcp_env, source)
        if _skip_dataset(
            id=id,
            source=source,
            third_party=third_party,
            odata_version=odata_version,
            gcp=gcp,
            force=force,
            credentials=credentials,
        ):
            logger.info(
                f"Skipping dataset {id} because the same dataset exists on GCP, with the same 'Modified' date"
            )
            return None
        if endpoint == "bq":
            files_parquet = _cbsodata_to_gbq(
                id=id,
                odata_version=odata_version,
                third_party=third_party,
                source=source,
                config=config,
                gcp=gcp,
                credentials=credentials,
            )  # TODO - add response from google if possible (some success/failure flag)
        elif endpoint == "gcs":
            files_parquet = _cbsodata_to_gcs(
                id=id,
                odata_version=odata_version,
                third_party=third_party,
                source=source,
                config=config,
                gcp=gcp,
                credentials=credentials,
            )
    return files_parquet


if __name__ == "__main__":
    from statline_bq.config import get_config

    config = get_config("./statline_bq/config.toml")
    # Test cbs core dataset, odata_version is v3
    # main("83583NED", config=config, gcp_env="dev", endpoint="bq", force=True)
    # Test cbs core dataset, odata_version is v3 - local only
    # main("83583NED", config=config, endpoint="local", local_dir="./temp/")
    # # Test skipping a dataset, odata_version is v3
    # local_folder = main("83583NED", config=config, gcp_env="dev", force=False)
    # Test cbs core dataset, odata_version is v3, contaiing empty url (CategoryGroups)
    # local_folder = main("84799NED", config=config, gcp_env="dev", force=True)
    # Test cbs core dataset, odata_version is v4
    main("83765NED", config=config, gcp_env="dev", force=True)
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
