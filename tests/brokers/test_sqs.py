"""Unit tests for navigator_eventbus.brokers.sqs (TASK-1817, FEAT-316)."""
import inspect

from navigator_eventbus.brokers.sqs import SQSConnection


def test_sqs_connection_creds_navconfig(monkeypatch):
    """AWS creds resolved via navconfig env, not navigator.conf."""
    monkeypatch.setenv("AWS_KEY", "AKIATEST")
    monkeypatch.setenv("AWS_SECRET", "s3cret")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    conn = SQSConnection()
    assert conn._credentials["aws_access_key_id"] == "AKIATEST"
    assert conn._credentials["aws_secret_access_key"] == "s3cret"
    assert conn._credentials["region_name"] == "us-east-1"


def test_sqs_connection_explicit_credentials():
    creds = {"aws_access_key_id": "explicit", "aws_secret_access_key": "x", "region_name": "eu-west-1"}
    conn = SQSConnection(credentials=creds)
    assert conn._credentials == creds


def test_sqs_no_navigator_imports():
    import navigator_eventbus.brokers.sqs.connection as m

    src = inspect.getsource(m)
    assert "from navigator." not in src and "import navigator." not in src
