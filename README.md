## statline-bq
An open source library to upload datasets from the Dutch CBS (Central Bureau of Statistics) into Google Cloud Platform. While its main use is as a helper library within the [NL Open Data](https://github.com/dataverbinders/nl-open-data) project, it can also be used as a standalone application, using its CLI. When given a list of valid dataset IDs (i.e. "83583NED") it downloads the datasets, and uploads them to Google Big Query, where the datasets can be interacted with using SQL.

NOTE - you must have the [appropriate permissions](https://cloud.google.com/bigquery/docs/dataset-access-controls) on an existing [Google Cloud Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) prior to running statline-bq. 

## Motivation
In order to take advantage of open data, the ability to mix various datasets together must be available. As of now, in order to to that, a substantial knowledge of programming and data engineering must be available to any who wishes to do so. This library, and its parent project [NL Open Data](https://github.com/dataverbinders/nl-open-data), aim to make that task easier.

## Build status
[![Pypi Status](https://img.shields.io/pypi/v/statline-bq.svg)](https://pypi.python.org/pypi/statline-bq) [![Build Status](https://img.shields.io/travis/dkapitan/statline-bq.svg)](https://travis-ci.com/dkapitan/statline-bq) [![Docs Status](https://readthedocs.org/projects/statline-bq/badge/?version=latest)](https://dkapitan.github.io/statline-bq)

## Installation and setup

Pip:
    `pip install statline_bq` -> **NOT IMPLEMENTED YET**

Poetry:
    Being a [Poetry](https://python-poetry.org/) managed package, installing via Poetry is also possible. Assuming Poetry is already installed:
    1. Clone the repository
    2. From your local clone's root folder, run `poetry install`

## Setup

There are two elements that need to bet setup prior to using the CLI:

### 1. GCP (and Paths) through config.toml

The GCP project id, bucket, and location are supplied here, allowing choices at runtime: `dev`, `test` and `prod`. Note that you must nest gcp projects setails correctly for them to be interperted, as seen below. At least one gcp project where the user holds the appropriate permissions for


```
[gcp]
    [gcp.prod]
    project_id = "my_dev_project_id"
    bucket = "my_dev_bucket"
    location = "EU"

    [gcp.test]
    project_id = "my_test_project_id"
    bucket = "my_test_bucket"
    location = "EU"

    [gcp.prod]
    project_id = "my_prod_project_id"
    bucket = "my_prod_bucket"
    location = "EU"
```

## Usage
Once the library is installed, two conifguration should be implemented:

1. 

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