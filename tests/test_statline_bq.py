import json
from pathlib import Path
import pytest
import requests

from statline_bq import __version__, config, utils, gcpl, statline, main


def test_version():
    assert __version__ == "0.1.0"


@pytest.fixture()
def config_file_toml(tmpdir):
    """Mock config.toml"""
    config_toml = """[gcp]
    [gcp.prod]
        project_id = "mock-project-dl"
        bucket = "mock-project-dl"
        location = "EU"

    [gcp.test]
    project_id = "mock-project-test"
    bucket = "mock-project-test"
    location = "EU"

    [gcp.dev]
    project_id = "mock-project-dev"
    bucket = "mock-project-dev"
    location = "EU"

    [paths]
    cbs = "cbs"
    """
    file = tmpdir.join("config.toml")
    with open(file, "w") as f:
        f.write(config_toml)
    return file


@pytest.fixture()
def datasets_toml(tmpdir):
    datasets_toml = (
        """ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]"""
    )
    file = tmpdir.join("datasets.toml")
    with open(file, "w") as f:
        f.write(datasets_toml)
    return file


class MockResponse:
    def __init__(self, id):
        self.id = id
        if self.id == "exists_in_v4":
            self.status_code = 200
        else:
            self.status_code = None


class TestConfig:
    def test_get_config(self, config_file_toml):
        config_box = config.get_config(config_file_toml)
        assert config_box.gcp.dev.project_id == "mock-project-dev"

    def test_get_datasets(self, datasets_toml):
        datasets = config.get_datasets(datasets_toml)
        assert datasets == ("83583NED", "83765NED", "84799NED", "84583NED", "84286NED")


class TestStatline:
    @pytest.mark.parametrize(
        "id, version", [("exists_in_v4", "v4"), ("not_exists_in_v4", "v3")]
    )
    def test_check_v4(self, monkeypatch, id, version):
        def mock_get_v4(*args, **kwargs):
            return MockResponse(id)

        monkeypatch.setattr(requests, "get", mock_get_v4)
        odata_version = statline._check_v4(id)
        assert odata_version == version

    def test_get_metadata_cbs(self):
        # TODO can't think of a logical way to test this
        pass

    @pytest.mark.parametrize(
        "metadata, shape",
        [
            (
                "metadata_v3.json",
                {"n_records": 304128, "n_columns": 10, "n_observations": None},
            ),
            (
                "metadata_v4.json",
                {"n_records": None, "n_columns": None, "n_observations": 2432},
            ),
        ],
    )
    def test_get_main_table_shape(self, metadata, shape):
        # test with downloaded data
        file_ = Path(__file__).parent / "data" / metadata
        with open(file_, "r") as f:
            meta = json.load(f)
        assert statline._get_main_table_shape(meta) == shape


class TestUtils:
    def test_check_gcp_env(self):
        assert utils._check_gcp_env("dev")
        with pytest.raises(ValueError):
            assert utils.check_gcp_env("foo")

