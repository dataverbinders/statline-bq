# Module containing functions that interact with the statline API
from typing import Union
from pathlib import Path
import requests
import xml.etree.ElementTree as ET

import pyarrow as pa
import dask.bag as db

from statline_bq.utils import _create_dir, _url_to_ndjson, convert_ndjsons_to_parquet
from statline_bq.log import logdec


@logdec
def _check_v4(id: str, third_party: bool = False) -> str:
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


@logdec
def _get_urls(
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


@logdec
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


@logdec
def _get_main_table_shape(metadata: dict) -> dict:
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


@logdec
def _generate_table_urls(base_url: str, n_records: int, odata_version: str) -> list:
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


@logdec
def _get_schema_cbs(metadata_url, odata_version) -> pa.Schema:
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


@logdec
def _get_column_descriptions(urls: dict, odata_version: str) -> dict:
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
        column_descriptions = _get_column_descriptions_v3(urls["DataProperties"])
    else:
        raise ValueError("odata version must be either 'v3' or 'v4'")
    return column_descriptions


@logdec
def _get_column_descriptions_v3(url_data_properties: str) -> dict:
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
        except:  # TODO: Handle better!
            pass
    return col_desc


@logdec
def dataset_to_parquet(
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
                schema = _get_schema_cbs(metadata_url, odata_version)
        else:
            table_shape = {
                "n_records": None,
                "n_columns": None,
                "n_observations": None,
            }  # TODO: A better way to default on non-main tables?
            schema = None

        if odata_version == "v3":
            # Generate all table urls
            table_urls = _generate_table_urls(
                url, table_shape["n_records"], odata_version
            )
        elif odata_version == "v4":
            table_urls = _generate_table_urls(
                url, table_shape["n_observations"], odata_version
            )

        # create directories to store files
        pq_dir = Path(pq_dir)
        ndjson_dir = pq_dir.parent / Path(f"ndjson/{table_name}")
        _create_dir(pq_dir)
        _create_dir(ndjson_dir)

        # Fetch all urls and dump each as ndjson
        ndjsons_paths = (
            db.from_sequence(table_urls)
            .map(_url_to_ndjson, ndjson_folder=ndjson_dir)
            .compute()
        )

        # Some urls (i.e. 84799NED_CategoryGroups) are actually empty. These will be computed to None, and skipped here
        if any(ndjsons_paths):
            # Convert to parquet
            pq_path = convert_ndjsons_to_parquet(
                files=ndjsons_paths,
                # urls=table_urls,
                # bag=table,
                file_name=table_name,
                out_dir=pq_dir,
                schema=schema
                # odata_version=odata_version,
            )
        # Add path of file to set
        if pq_path:
            files_parquet.add(pq_path)

    return files_parquet
