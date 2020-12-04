from pathlib import Path
from typing import Union

from statline_bq.utils import cbs_odata_to_gbq as cbs_odata_to_gbq_util
from statline_bq.config import Config, Gcp

from prefect import task
from prefect.engine.signals import SKIP


@task
def curl_cmd(url: str, filepath: Union[str, Path], **kwargs) -> str:
    """Template for curl command to download file.
    Uses `curl -fL -o` that fails silently and follows redirects. 
    Example:
    ```
    from pathlib import Path
    from prefect import Parameter, Flow
    from prefect.tasks.shell import ShellTask
    curl_download = ShellTask(name='curl_download')
    with Flow('test') as flow:
        filepath = Parameter("filepath", required=True)
        curl_command = curl_cmd("https://some/url", filepath)
        curl_download = curl_download(command=curl_command)
    flow.run(parameters={'filepath': Path.home() / 'test.zip'})
    ```
    Args:
        - url (str): url to download
        - file (str): file for saving fecthed url
        - **kwargs: passed to Task constructor
    
    Returns:
        str: curl command
    
    Raises:
        - SKIP: if filepath exists
    """
    if Path(filepath).exists():
        raise SKIP(f"File {filepath} already exists.")
    return f"curl -fL -o {filepath} {url}"


@task
def cbs_odata_to_gbq(
    id: str, source: str = "cbs", third_party: bool = False, config: Config = None,
):
    """ A Prefect task wrapper for cbs_odata_to_gbq from utils.py
    """
    cbs_odata_to_gbq_util(id=id, source=source, third_party=third_party, config=config)
    return None


if __name__ == "__main__":
    try:
        cbs_odata_to_gbq("83583NED")
    except ValueError as error:
        print(error)
