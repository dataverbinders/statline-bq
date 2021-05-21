from datetime import datetime
import json
from pathlib import Path
import pytest
import requests
import filecmp

from google.cloud import storage

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


class TestGcpl:
    def test_upload_to_gcs(self, tmpdir):
        file_names = ["test_1.txt", "test_2"]
        f1 = tmpdir.join(file_names[0])
        f2 = tmpdir.join(file_names[1])
        with open(f1, "w+") as f:
            f.write(
                """
            Some_text
            """
            )
        with open(f2, "w+") as f:
            f.write(
                """
            different text
            in several
            rows
            """
            )
        CONFIG = config.get_config("statline_bq/config.toml")
        gcp = CONFIG.gcp.test
        gcs_folder = gcpl.upload_to_gcs(
            tmpdir, source="test", odata_version="0", id="NO_ID", gcp=gcp
        )
        client = storage.Client(project=gcp.project_id)
        blob_names = [
            blob.name.split("/")[-1]
            for blob in client.list_blobs(gcp.bucket, prefix=gcs_folder)
        ]
        assert file_names == blob_names


class TestUtils:
    def test_check_gcp_env(self):
        assert utils._check_gcp_env("dev")
        with pytest.raises(ValueError):
            assert utils._check_gcp_env("foo")


class TestMain:
    @pytest.mark.parametrize(
        "ID, SOURCE, THIRD_PARTY, ODATA_VERSION, FORCE",
        [
            ("83583NED", "cbs", False, "v3", True),
            ("84799NED", "cbs", False, "v3", True),
            ("83765NED", "cbs", False, "v4", True),
            ("45012NED", "iv3", True, "v3", True),
        ],
    )
    def test_main(self, ID, SOURCE, THIRD_PARTY, ODATA_VERSION, FORCE, tmp_path):
        CONFIG = config.get_config("statline_bq/config.toml")
        GCS_FOLDER = f"{SOURCE}/{ODATA_VERSION}/{ID}/{datetime.today().date().strftime('%Y%m%d')}"
        odata_version = statline._check_v4(ID, THIRD_PARTY)
        assert (
            odata_version == ODATA_VERSION
        ), "The given version does not match the actual version. This might happen if a v3 dataset was updated to v4."
        new_meta = statline.get_metadata_cbs(
            id=ID, third_party=THIRD_PARTY, odata_version=ODATA_VERSION
        )
        test_metadata_file = (
            Path(__file__).parent
            / "data"
            / ID
            / f"{SOURCE}.{ODATA_VERSION}.{ID}_Metadata.json"
        )
        with open(test_metadata_file, "r",) as f:
            test_meta = json.load(f)
        new_modified = new_meta.get("Modified")
        test_modified = test_meta.get("Modified")
        assert (
            new_modified == test_modified
        ), "The 'Modified' metadata fields do not match. This might happen if a dataset was updated."
        main.main(
            id=ID,
            source=SOURCE,
            third_party=THIRD_PARTY,
            config=CONFIG,
            gcp_env="test",
            endpoint="gcs",
            force=FORCE,
        )
        client = storage.Client(project=CONFIG.gcp.test.project_id)
        blobs = client.list_blobs(CONFIG.gcp.test.bucket, prefix=GCS_FOLDER)
        assertion_paths = {}
        for blob in blobs:
            file_name = blob.name.split("/")[-1]
            download_file = str(tmp_path) + "/" + file_name
            blob.download_to_filename(download_file)
            truth_file = Path(__file__).parent / "data" / ID / file_name
            assertion_paths[download_file] = truth_file
        # Assert at least one blob exists
        assert assertion_paths
        # Assert each file against source
        for gfile, truth in assertion_paths.items():
            assert filecmp.cmp(gfile, truth)
