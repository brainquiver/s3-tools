"""Bucket creation: the tool uses a bucket of this account, and stops on a name of another.

The S3 code in the error body tells the cases apart. R2 and B2 both answer 409, with
different meanings.
"""
import http.client

import pytest

import push
from fake_service import Reply, error


@pytest.mark.parametrize("code", ["BucketAlreadyOwnedByYou", "BucketConflict"])
def test_a_bucket_this_account_owns_is_used(transport, credentials, code):
    # B2 answers BucketAlreadyOwnedByYou for a bucket of this account. R2 answers BucketConflict,
    # and R2 names are unique within one account, so on R2 that code can mean only this account.
    transport.on("PUT", "/datasets", error(409, code))
    push.make_bucket("datasets")


def test_a_name_that_another_account_owns_stops_the_run(transport, credentials):
    # B2 bucket names are global across every B2 account, so the name can belong to a stranger.
    transport.on("PUT", "/datasets", error(409, "BucketAlreadyExists"))
    with pytest.raises(RuntimeError, match="another account"):
        push.make_bucket("datasets")


def test_an_unknown_conflict_stops_the_run(transport, credentials):
    transport.on("PUT", "/datasets", error(409, "OperationAborted"))
    with pytest.raises(RuntimeError, match="OperationAborted"):
        push.make_bucket("datasets")


def test_a_new_bucket_is_reported(transport, credentials, capsys):
    transport.on("PUT", "/datasets", Reply(200))
    push.make_bucket("datasets")
    assert "bucket datasets created" in capsys.readouterr().out


def test_a_dropped_connection_is_tried_again(transport, credentials, monkeypatch):
    monkeypatch.setattr(push.time, "sleep", lambda seconds: None)
    seen = []

    def drop_once(call):
        if not seen:
            seen.append(call)
            raise http.client.RemoteDisconnected("Remote end closed connection without response")
        return error(409, "BucketAlreadyOwnedByYou")

    transport.on("PUT", "/datasets", drop_once)
    push.make_bucket("datasets")
    assert len(transport.find("PUT", "/datasets")) == 2
