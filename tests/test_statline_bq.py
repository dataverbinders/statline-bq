import pytest

from statline_bq import __version__, config, utils


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
    datasets_toml = """ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]"""
    file = tmpdir.join("datasets.toml")
    with open(file, "w") as f:
        f.write(datasets_toml)
    return file


class TestConfig:
    def test_get_config(self, config_file_toml):
        config_box = config.get_config(config_file_toml)
        assert config_box.gcp.dev.project_id == "mock-project-dev"

    def test_get_datasets(self, datasets_toml):
        datasets = config.get_datasets(datasets_toml)
        assert datasets == ("83583NED", "83765NED", "84799NED", "84583NED", "84286NED")


class TestUtils:
    def test_check_gcp_env(self):
        assert utils.check_gcp_env("dev")
        with pytest.raises(ValueError):
            assert utils.check_gcp_env("foo")

