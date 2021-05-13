from typing import Union
from pathlib import Path
import logging
from shutil import rmtree

import pyarrow.parquet as pq
from box import Box
from google.oauth2.credentials import Credentials

from statline_bq.statline import (
    check_v4,
    get_urls,
    get_metadata_cbs,
    get_main_table_shape,
    tables_to_parquet,
    get_column_descriptions,
)
from statline_bq.gcpl import (
    get_metadata_gcp,
    set_gcp,
    upload_to_gcs,
    gcs_to_gbq,
    get_col_descs_from_gcs,
    bq_update_main_table_col_descriptions,
)
from statline_bq.utils import (
    check_gcp_env,
    create_dir,
    create_named_dir,
    dict_to_json_file,
    get_file_names,
)
from statline_bq.log import logdec

logger = logging.getLogger(__name__)


@logdec
def skip_dataset(
    id: str,
    source: str,
    third_party: bool,
    odata_version: str,
    gcp: Box,
    force: bool,
    credentials: Credentials = None,
) -> bool:
    """Checks whether a dataset should be skipped, given the "last modified" dates from the CBS version and the GCP version.

    Parameters
    ----------
    cbs_modified : str
        "last modifed" string from the CBS metadata
    gcp_modified : str
        "last modifed" string from the GCP metadata
    force : bool
        flag to signal forcing the processing of a dataset even if the dates match.

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
    gcp_meta = get_metadata_gcp(
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
def cbsodata_to_local(
    id: str,
    odata_version: str,
    third_party: str = False,
    source: str = "cbs",
    config: Box = None,
    out_dir: Union[Path, str] = None,
) -> set:  # TODO change return value
    """Downloads a CBS dataset and stores it locally as parquet files.

    Retrieves a given dataset from CBS, and converts it locally to Parquet. The
    Parquet files are stored locally.

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
    # Get all table-specific urls for the given dataset id
    urls = get_urls(id=id, odata_version=odata_version, third_party=third_party)
    # Get dataset metadata
    source_meta = get_metadata_cbs(
        id=id, third_party=third_party, odata_version=odata_version
    )
    # Create directory to store parquest files locally
    if out_dir:
        pq_dir = create_dir(out_dir)
    else:
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
    return pq_dir, files_parquet


@logdec
def cbsodata_to_gcs(
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

    # download data and store locally as parquet
    pq_dir, files_parquet = cbsodata_to_local(
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
def cbsodata_to_gbq(
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
    # download data and store locally as parquet
    pq_dir, files_parquet = cbsodata_to_local(
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

    Retrieves a given dataset id from CBS, and converts it locally to Parquet. The
    Parquet files are uploaded to Google Cloud Storage, and a dataset is created
    in Google BigQuery, under which each permanenet tables are nested,linked to the
    Parquet files - each being a table of the dataset.

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
    gcp_env = gcp_env.lower()
    if third_party and source == "cbs":
        raise ValueError(
            "A third-party dataset cannot have 'cbs' as source: please provide correct 'source' parameter"
        )
    if check_gcp_env(gcp_env):
        odata_version = check_v4(id=id, third_party=third_party)
        # Set gcp environment
        gcp = set_gcp(config, gcp_env, source)
        # Check if upload is needed
        if (
            skip_dataset(
                id=id,
                source=source,
                third_party=third_party,
                odata_version=odata_version,
                gcp=gcp,
                force=force,
                credentials=credentials,
            )
            and endpoint != "local"
        ):
            logger.info(
                f"Skipping dataset {id} because the same dataset exists on GCP, with the same 'Modified' date"
            )
            return None
        if endpoint == "bq":
            files_parquet = cbsodata_to_gbq(
                id=id,
                odata_version=odata_version,
                third_party=third_party,
                source=source,
                config=config,
                gcp=gcp,
                credentials=credentials,
            )  # TODO - add response from google if possible (some success/failure flag)
        elif endpoint == "gcs":
            files_parquet = cbsodata_to_gcs(
                id=id,
                odata_version=odata_version,
                third_party=third_party,
                source=source,
                config=config,
                gcp=gcp,
                credentials=credentials,
            )
        elif endpoint == "local":
            _, files_parquet = cbsodata_to_local(
                id=id,
                odata_version=odata_version,
                third_party=third_party,
                source=source,
                config=config,
                out_dir=local_dir,
            )
        else:
            raise ValueError("endpoint must be one of ['bq', 'gcs', 'local']")
        if files_parquet:
            pq_file = Path(files_parquet.pop())
            local_folder = pq_file.parents[2]
        else:
            local_folder = None
        return local_folder


if __name__ == "__main__":
    from statline_bq.config import get_config

    config = get_config("./statline_bq/config.toml")
    # # Test cbs core dataset, odata_version is v3
    local_folder = main(
        "83583NED", config=config, gcp_env="dev", endpoint="local", force=False
    )
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
