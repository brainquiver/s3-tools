"""Multipart upload: the tool sends a large file in parts, and a failure aborts the upload.

The tests shrink the threshold and the part size to a few bytes. A ten-byte file then takes
the same path as a six-gigabyte file.
"""
import hashlib
import sys
import urllib.error
import xml.etree.ElementTree as ET

import pytest

import push
from fake_service import NS, Reply, error, listing

KEY = "/b/big.bin"
INITIATED = (f'<InitiateMultipartUploadResult xmlns="{NS}"><Bucket>b</Bucket>'
             "<Key>big.bin</Key><UploadId>up-1</UploadId></InitiateMultipartUploadResult>").encode()
COMPLETED = (f'<CompleteMultipartUploadResult xmlns="{NS}"><Key>big.bin</Key>'
             "</CompleteMultipartUploadResult>").encode()


def etag_for(call):
    return Reply(200, headers={"ETag": f'"e{call.query["partNumber"]}"'})


@pytest.fixture
def big(transport, credentials, tmp_path, monkeypatch):
    monkeypatch.setattr(push, "MULTIPART_THRESHOLD", 8, raising=False)
    monkeypatch.setattr(push, "PART_SIZE", 4, raising=False)
    transport.on("PUT", "/b", error(409, "BucketAlreadyOwnedByYou"))
    transport.on("GET", "/b", listing([]), query={"list-type"})
    f = tmp_path / "big.bin"
    f.write_bytes(b"0123456789")
    return f


def run(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["push.py", *args])
    push.main()


def test_a_large_file_goes_up_in_ordered_parts(transport, big, monkeypatch):
    transport.on("POST", KEY, Reply(200, INITIATED), query={"uploads"})
    transport.on("PUT", KEY, etag_for, query={"partNumber", "uploadId"})
    transport.on("POST", KEY, Reply(200, COMPLETED), query={"uploadId"})
    run(monkeypatch, "b", str(big))

    started = transport.find("POST", KEY, "uploads")
    assert len(started) == 1
    assert started[0].headers["x-amz-meta-sha256"] == hashlib.sha256(b"0123456789").hexdigest()

    # The parts go up over several connections at once, so they arrive in any order.
    parts = sorted(transport.find("PUT", KEY, "partNumber"), key=lambda p: int(p.query["partNumber"]))
    assert [p.body for p in parts] == [b"0123", b"4567", b"89"]
    assert [p.query["partNumber"] for p in parts] == ["1", "2", "3"]
    for p in parts:
        assert p.headers["x-amz-content-sha256"] == hashlib.sha256(p.body).hexdigest()

    done = [c for c in transport.find("POST", KEY, "uploadId")]
    assert len(done) == 1
    root = ET.fromstring(done[0].body)
    listed = [(p.findtext("PartNumber"), p.findtext("ETag")) for p in root.iter("Part")]
    assert listed == [("1", '"e1"'), ("2", '"e2"'), ("3", '"e3"')]
    assert [c for c in transport.find("PUT", KEY) if not c.query] == []


def test_a_part_that_keeps_failing_aborts_the_upload(transport, big, monkeypatch):
    def part(call):
        return error(500, "InternalError") if call.query["partNumber"] == "2" else etag_for(call)

    transport.on("POST", KEY, Reply(200, INITIATED), query={"uploads"})
    transport.on("PUT", KEY, part, query={"partNumber", "uploadId"})
    transport.on("DELETE", KEY, Reply(204), query={"uploadId"})
    with pytest.raises(urllib.error.HTTPError):
        run(monkeypatch, "b", str(big))
    aborted = transport.find("DELETE", KEY, "uploadId")
    assert [c.query["uploadId"] for c in aborted] == ["up-1"]
    assert transport.find("POST", KEY, "uploadId") == []


def test_an_error_inside_a_200_completion_aborts_the_upload(transport, big, monkeypatch):
    # S3 can answer CompleteMultipartUpload with 200 and an Error document in the body. A
    # success status alone therefore does not prove that the object exists.
    transport.on("POST", KEY, Reply(200, INITIATED), query={"uploads"})
    transport.on("PUT", KEY, etag_for, query={"partNumber", "uploadId"})
    transport.on("POST", KEY, Reply(200, b"<Error><Code>InternalError</Code></Error>"),
                 query={"uploadId"})
    transport.on("DELETE", KEY, Reply(204), query={"uploadId"})
    with pytest.raises(SystemExit, match="InternalError"):
        run(monkeypatch, "b", str(big))
    assert len(transport.find("DELETE", KEY, "uploadId")) == 1


def test_a_file_at_the_threshold_goes_up_in_one_put(transport, big, monkeypatch, tmp_path):
    small = tmp_path / "small.bin"
    small.write_bytes(b"01234567")
    transport.on("PUT", "/b/small.bin", Reply(200))
    run(monkeypatch, "b", str(small))
    assert len(transport.find("PUT", "/b/small.bin")) == 1
    assert transport.find("POST", "/b/small.bin", "uploads") == []
