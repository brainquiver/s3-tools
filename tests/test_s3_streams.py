"""Downloads in ranges over several connections, and uploads in parts over several connections.

The tests shrink the sizes to a few bytes. A ten-byte object then takes the same path as a
checkpoint of several gigabytes.
"""
import hashlib
import re
import sys

import pytest

import push
import s3
from fake_service import NS, Reply, error, listing

BODY = b"0123456789"
DIGEST = hashlib.sha256(BODY).hexdigest()
KEY = "/b/ckpt.bin"


@pytest.fixture
def small_ranges(monkeypatch):
    monkeypatch.setattr(s3, "STREAM_MIN", 4)
    monkeypatch.setattr(s3, "CHUNK", 3)
    monkeypatch.setattr(s3, "STREAMS", 3)
    monkeypatch.setattr(s3.time, "sleep", lambda seconds: None)


def ranged(body):
    """Answer a GET with the bytes of its Range header, as S3 does with 206."""
    def reply(call):
        a, b = map(int, re.fullmatch(r"bytes=(\d+)-(\d+)", call.headers["range"]).groups())
        return Reply(206, body[a:b + 1])
    return reply


def head_reply(digest=DIGEST):
    return Reply(200, headers={"Content-Length": str(len(BODY)), "x-amz-meta-sha256": digest})


def test_a_large_object_comes_down_in_ranges(transport, credentials, small_ranges, tmp_path):
    transport.on("HEAD", KEY, head_reply())
    transport.on("GET", KEY, ranged(BODY))
    out = tmp_path / "ckpt.bin"
    assert s3.download("b", "ckpt.bin", out) == len(BODY)
    assert out.read_bytes() == BODY
    asked = sorted(c.headers["range"] for c in transport.find("GET", KEY))
    assert asked == ["bytes=0-2", "bytes=3-5", "bytes=6-8", "bytes=9-9"]
    assert not (tmp_path / "ckpt.bin.part").exists()


def test_a_range_whose_body_ends_early_comes_down_again(transport, credentials, small_ranges, tmp_path):
    seen = set()

    def cut_once(call):
        if call.headers["range"] == "bytes=3-5" and "cut" not in seen:
            seen.add("cut")
            return Reply(206, BODY[3:4])
        return ranged(BODY)(call)

    transport.on("HEAD", KEY, head_reply())
    transport.on("GET", KEY, cut_once)
    out = tmp_path / "ckpt.bin"
    s3.download("b", "ckpt.bin", out)
    assert out.read_bytes() == BODY
    assert [c.headers["range"] for c in transport.find("GET", KEY)].count("bytes=3-5") == 2


def test_a_file_that_misses_the_stored_hash_is_not_kept(transport, credentials, small_ranges, tmp_path):
    transport.on("HEAD", KEY, head_reply(digest="0" * 64))
    transport.on("GET", KEY, ranged(BODY))
    out = tmp_path / "ckpt.bin"
    with pytest.raises(SystemExit, match="does not match the stored SHA-256"):
        s3.download("b", "ckpt.bin", out)
    assert not out.exists() and not (tmp_path / "ckpt.bin.part").exists()


def test_a_small_object_comes_down_over_one_connection(transport, credentials, small_ranges, tmp_path):
    transport.on("GET", "/b/s.bin", ranged(b"abc"))
    out = tmp_path / "s.bin"
    assert s3.download("b", "s.bin", out, size=3) == 3
    assert out.read_bytes() == b"abc"
    assert [c.headers["range"] for c in transport.find("GET", "/b/s.bin")] == ["bytes=0-2"]
    assert not transport.find("HEAD", "/b/s.bin")


def test_a_file_over_the_parallel_threshold_goes_up_in_parts(transport, credentials, tmp_path, monkeypatch):
    monkeypatch.setattr(push, "MULTIPART_THRESHOLD", 10 ** 9)
    monkeypatch.setattr(push, "PARALLEL_THRESHOLD", 8)
    monkeypatch.setattr(push, "PART_SIZE", 4)
    initiated = (f'<InitiateMultipartUploadResult xmlns="{NS}"><UploadId>up-1</UploadId>'
                 "</InitiateMultipartUploadResult>").encode()
    transport.on("PUT", "/b", error(409, "BucketAlreadyOwnedByYou"))
    transport.on("GET", "/b", listing([]), query={"list-type"})
    transport.on("POST", KEY, Reply(200, initiated), query={"uploads"})
    transport.on("PUT", KEY, lambda c: Reply(200, headers={"ETag": f'"e{c.query["partNumber"]}"'}),
                 query={"partNumber", "uploadId"})
    transport.on("POST", KEY, Reply(200, b"<CompleteMultipartUploadResult/>"), query={"uploadId"})
    f = tmp_path / "ckpt.bin"
    f.write_bytes(BODY)
    monkeypatch.setattr(sys, "argv", ["push.py", "b", str(f), "--streams", "3"])
    push.main()
    parts = sorted(transport.find("PUT", KEY, "partNumber"), key=lambda p: int(p.query["partNumber"]))
    assert [p.body for p in parts] == [b"0123", b"4567", b"89"]
    done = transport.find("POST", KEY, "uploadId")[0].body.decode()
    assert done.index("<PartNumber>1</PartNumber>") < done.index("<PartNumber>2</PartNumber>") < done.index("<PartNumber>3</PartNumber>")


def test_a_prefix_fetches_its_small_objects_several_at_a_time(transport, credentials, small_ranges, tmp_path):
    names = [f"set/part-{i}.bin" for i in range(5)]
    transport.on("GET", "/b", listing([(n, 2) for n in names]), query={"list-type"})
    for n in names:
        transport.on("GET", f"/b/{n}", ranged(n[-5:-4].encode() * 2))
    s3.get("b", "set/", "", tmp_path, streams=3)
    assert [(tmp_path / n).read_bytes() for n in names] == [n[-5:-4].encode() * 2 for n in names]
