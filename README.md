## statline-bq
`statline-bq` is an open source library built to upload datasets from the Dutch [CBS (Central Bureau of Statistics)](https://opendata.cbs.nl/statline/#/CBS/nl/) into [Google BigQuery](https://cloud.google.com/bigquery), where they could be accessed via SQL. While intended mainly to be a helper library within the [NL Open Data](https://github.com/dataverbinders/nl-open-data) project, it can also be used as a standalone CLI application. When provided with list of valid dataset IDs (i.e. "83583NED") it downloads the datasets, and uploads them to Google Big Query, where the datasets can be interacted with using SQL.

NOTE - you must have the [appropriate permissions](https://cloud.google.com/bigquery/docs/dataset-access-controls) on an existing [Google Cloud Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) prior to running statline-bq. 

### Motivation
In order to take advantage of open data, the ability to mix various datasets together must be available. As of now, in order to to that, a substantial knowledge of programming and data engineering must be available to any who wishes to do so. This library, and its parent project [NL Open Data](https://github.com/dataverbinders/nl-open-data), aim to make that task easier.

### Build status
[![Pypi Status](https://img.shields.io/pypi/v/statline-bq.svg)](https://pypi.python.org/pypi/statline-bq) [![Build Status](https://img.shields.io/travis/dkapitan/statline-bq.svg)](https://travis-ci.com/dkapitan/statline-bq) [![Docs Status](https://readthedocs.org/projects/statline-bq/badge/?version=latest)](https://dkapitan.github.io/statline-bq)

### Installation and setup

Using pip:
    `pip install statline_bq` -> **NOT IMPLEMENTED YET**

Using Poetry:
    Being a [Poetry](https://python-poetry.org/) managed package, installing via Poetry is also possible. Assuming Poetry is already installed:
    
1. Clone the repository
2. From your local clone's root folder, run `poetry install`

### Configuration

There are two elements that need to be configured prior to using the CLI. If using as an imported library the exact usage determines wheteher these are both needed (or just one, or none)

#### 1. GCP and Paths through `config.toml`

##### 1.1. GCP
The GCP project id, bucket, and location should be provided by editing `statline_bq/config.toml`, allowing up to 3 choices at runtime: `dev`, `test` and `prod`. `prod` is further divided into 3: `cbs_dl` serving as a data lake for the BASE CBS catalog, `external_dl` serving as a data lake for the THIRD-PARTY catalog and `dwh`, meant for any additional or combined resources. The selection within the three `prod` environments is automatically inferred at runtime, according to the `source` parameter.

Note that you must nest gcp projects details correctly for them to be interperted, as seen below. You must also have the proper IAM (permissions) on the GCP projects (more details below).

Correct nesting in config file:
```
[gcp]
    [gcp.dev]
    project_id = "my_dev_project_id"
    bucket = "my_dev_bucket"
    location = "EU"

    [gcp.test]
    project_id = "my_test_project_id"
    bucket = "my_test_bucket"
    location = "EU"

    [gcp.prod]
        [gcp.prod.cbs_dl]
        project_id = "my-cbs-dl"
        bucket = "my-cbs-dl_bucket"
        location = "EU"

        [gcp.prod.external_dl]
        project_id = "my-external-dl"
        bucket = "my-external-dl_bucket"
        location = "EU"

        [gcp.prod.dwh]
        project_id = "my-open-dwh"
        bucket = "my-open-dwh_bucket"
        location = "EU"
```
##### 1.2. Paths
Additionally, the local temp paths used by the library can be configured here. If a new `source` is used, it must be added both in `config.toml` under `[paths]` and in `config.py` to the `Paths` Class.

#### 2. Datasets through `datasets.toml`

Provide a list of all CBS dataset ids that are to be uploaded to GCP. i.e.:

`ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]`

This should be given by directly editing `statline_bq/datasets.toml`

### Usage

statline-bq can be used via a command line, or imported as a library.

#### CLI
Once the library is installed and configured, you can either used `poetry run`:

1. From your terminal, navigate to "my_path_to_library/statline-bq/statline_bq/"
2. run `poetry run statline-bq`
3. That's it!

Or spawn a shell:

1. From your terminal, navigate to "my_path_to_library/statline-bq/statline_bq/"
2. run `poetry shell`
3. run `statline-bq --help` for info

#### In a python script

Examples:
--------

- Running the whole process:

```
from statline_bq.utils import check_v4, cbsodata_to_gbq
from statline_bq.config import get_config

id = "83583NED"  # dataset id from CBS
config = get_config("./statline_bq/config.toml")  # string path to config.toml

odata_version = check_v4(id=id) # assigns 'v4' if a v4 version exists, 'v3' otherwise
cbsodata_to_gbq(
        id=id,
        odata_version=odata_version,
        config=config
    )
```

- Only uploading to GCS:
```
from statline_bq.utils import cbsodata_to_gbq
from statline_bq.config import get_config

id = ["83583NED"]  # dataset id from CBS
odata_version = "v3"  # odata version: 'v3' or 'v4'
config = get_config("./statline_bq/config.toml")  # string path to config.toml

upload_dir = "some_folder/folder_to_upload_from/" # path to directory containing filed to be uploaded

gcs_folder = upload_to_gcs(dir=upload_dir,
        odata_version=odata_version, id=id, config=config)

```



<!-- ## Screenshots
Include logo/demo screenshot etc. -->

<!-- ## Features
What makes your project stand out? -->

<!-- ## Code Example
Show what the library does as concisely as possible, developers should be able to figure out **how** your project solves their problem by looking at the code example. Make sure the API you are showing off is obvious, and that your code is short and concise.

## Installation
Provide step by step series of examples and explanations about how to get a development env running.

## API Reference
Depending on the size of the project, if it is small and simple enough the reference docs can be added to the README. For medium size to larger projects it is important to at least provide a link to where the API reference docs live.

## Tests
Describe and show how to run the tests with code examples.

## How to use?
If people like your project they’ll want to learn how they can use it. To do so include step by step guide to use your project.

## Contribute

Let people know how they can contribute into your project. A [contributing guideline](https://github.com/zulip/zulip-electron/blob/master/CONTRIBUTING.md) will be a big plus.

## Credits
Give proper credits. This could be a link to any repo which inspired you to build this project, any blogposts or links to people who contrbuted in this project. 

#### Anything else that seems useful

## License
A short snippet describing the license (MIT, Apache etc)

MIT © [Yourname]() -->
