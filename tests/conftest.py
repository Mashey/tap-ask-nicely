import pytest
import boto3
from moto import mock_s3
from tap_ask_nicely.client import AskNicelyClient
from tap_ask_nicely.storage import StorageHandler

import os


@pytest.fixture
def config(local_file_path):
    return {
        "subdomain": os.getenv("SUBDOMAIN"),
        "api_key": os.getenv("API_KEY"),
        "start_date": "2020-01-01",
        "file_path": local_file_path,
    }


@pytest.fixture
def client(config):
    return AskNicelyClient(config)


@pytest.fixture
def state():
    return {}


@pytest.fixture
def aws_credentials():
    return {
        "aws_access_key_id": "testing_key",
        "aws_secret_access_key": "testing_secret",
    }


@pytest.fixture
def setup_aws_credentials(aws_credentials):
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = aws_credentials["aws_access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_credentials["aws_secret_access_key"]


@pytest.fixture
def s3(setup_aws_credentials):
    with mock_s3():
        session = boto3.Session(region_name="us-east-1")
        yield session.resource("s3")


@pytest.fixture
def raw_file_data():
    return [1, 2, 3, 5]


@pytest.fixture
def file_path():
    return "test-file-path.json"


@pytest.fixture
def local_file_path(tmpdir, file_path):
    return tmpdir.join(file_path)
