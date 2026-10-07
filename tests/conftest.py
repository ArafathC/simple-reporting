import os
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x")


def _table(ddb, name):
    return ddb.create_table(
        TableName=name, BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                              {"AttributeName": "sk", "AttributeType": "S"}])


@pytest.fixture
def aws():
    with mock_aws():
        ddb = boto3.resource("dynamodb")
        yield {"speed": _table(ddb, "speed"), "batch": _table(ddb, "batch"),
               "s3": boto3.client("s3"), "kinesis": boto3.client("kinesis")}
