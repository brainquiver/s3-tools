"""Upload to an S3-compatible store, skip what is already there and retry what breaks.

On a slow or saturated uplink, a large PUT breaks often enough that a long
transfer needs several tries, and a restart must send only what is still
absent.

The tool lists the bucket first. An edit can keep the byte count, so the tool
skips a key only when the size matches and the stored SHA-256 matches the file.
Each upload stores that hash as x-amz-meta-sha256. An object from an older
version of the tool has no hash. That object keeps the size check alone, and
the run counts it.

The tool sends a file over MULTIPART_THRESHOLD as a multipart upload, and with
--streams above 1 a file over PARALLEL_THRESHOLD too, its parts over that many
connections at once: one connection is often far slower than the link. A failure
after the start aborts that upload, so no unfinished upload remains.

The tool tries a failed request again, and each pause is longer than the one
before. A PUT is atomic, so a broken one leaves the bucket as it was before the
PUT.

A dataset directory goes to the bucket as its card and its curated tree, under
the name of the directory. The caller chooses the folder in the bucket that
holds the tree.
"""
import argparse, concurrent.futures, hashlib, http.client, json, os, pathlib, sys, time
import urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from s3 import DEFAULT_ENV, head, load_env, sign, objects  # noqa: E402

TRIES = 6
HASH_HEADER = "x-amz-meta-sha256"

# CONTRACT: B2 accepts at most 5 GB in one upload. The threshold takes that as 5,000,000,000
# bytes, the smaller of the two meanings, so a single PUT stays under the limit in either unit.
MULTIPART_THRESHOLD = 5_000_000_000

# NOTE: B2 recommends parts of 100 MB and allows 10,000 of them, so one object reaches about
# 1 TB. Every part except the last has this size.
PART_SIZE = 100 * 1024 * 1024

# With STREAMS above 1, a file over PARALLEL_THRESHOLD goes up in parts over STREAMS connections
STREAMS = 4
PARALLEL_THRESHOLD = 2 * PART_SIZE

# CONTRACT: the S3 code in the error body decides the case. R2 and B2 both answer 409, with
# different meanings. B2 bucket names are global, so B2 answers BucketAlreadyOwnedByYou for a
# bucket of this account and BucketAlreadyExists for a name of another account. R2 names are
# unique within one account, and R2 answers BucketConflict, which can therefore mean only this
# account.
OWN_BUCKET = {"BucketAlreadyOwnedByYou", "BucketConflict"}


def is_junk(name):
    """True for the metadata files that macOS writes beside real files."""
    return name == ".DS_Store" or name.startswith("._")


def keyed(prefix, name):
    return f"{prefix}/{name}" if prefix else name


def object_path(bucket, key):
    return f"/{bucket}/{urllib.parse.quote(key, safe='/')}"


def file_hash(path):
    """Return the SHA-256 of a file as hex. It reads in blocks, so a large file stays on disk."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 22), b""):
            h.update(block)
    return h.hexdigest()


def xml_text(body, name):
    """Return the text of the first element called name, in any namespace, or None."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] == name:
            return element.text
    return None


def with_retries(action, what):
    """Run action until it succeeds, TRIES times at most. Each pause is 5 seconds longer.

    The function tries again only after a fault of the network or of the service. A
    RuntimeError reports a reply that a second try cannot change, so the function raises it
    at once.
    """
    for attempt in range(1, TRIES + 1):
        try:
            return action()
        except (OSError, http.client.HTTPException) as e:
            if attempt == TRIES:
                raise
            pause = 5 * attempt
            print(f"    {type(e).__name__} on try {attempt} of {what}, next try in {pause}s",
                  flush=True)
            time.sleep(pause)


def put(bucket, key, path, digest=None):
    """Upload one file, in parts when it is larger than MULTIPART_THRESHOLD.

    The upload carries the SHA-256 of the file as x-amz-meta-sha256. A later run can then tell
    an edited file from an unchanged file of the same size. Return the HTTP status.
    """
    size = path.stat().st_size
    digest = digest or file_hash(path)
    if size > MULTIPART_THRESHOLD or (STREAMS > 1 and size > PARALLEL_THRESHOLD):
        return put_in_parts(bucket, key, path, digest)

    def attempt():
        # The signature carries a timestamp, so each try signs again.
        url, headers = sign("PUT", object_path(bucket, key), digest, length=size,
                            extra_headers={HASH_HEADER: digest})
        headers["content-type"] = "application/octet-stream"
        with open(path, "rb") as body:
            r = urllib.request.urlopen(
                urllib.request.Request(url, data=body, method="PUT", headers=headers),
                timeout=1800)
        return r.status

    return with_retries(attempt, key)


def put_in_parts(bucket, key, path, digest):
    """Upload one large file as a multipart upload, and abort it when any step fails.

    CAUTION: an upload that starts and never completes leaves its parts in the bucket. Every
    failure after the start therefore aborts the upload before it raises, and an interrupt
    does the same.
    """
    where = object_path(bucket, key)
    upload_id = with_retries(lambda: start_upload(where, digest), f"{key} start")
    size = path.stat().st_size

    def send_part(number):
        # Each worker reads its own part, so at most STREAMS parts are in memory at once.
        with open(path, "rb") as f:
            f.seek((number - 1) * PART_SIZE)
            chunk = f.read(PART_SIZE)
        etag = with_retries(lambda: put_part(where, upload_id, number, chunk),
                            f"{key} part {number}")
        print(f"    {key} part {number}  {len(chunk)/1e6:8.1f} MB", flush=True)
        return number, etag

    try:
        count = max(1, -(-size // PART_SIZE))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, STREAMS)) as pool:
            parts = sorted(pool.map(send_part, range(1, count + 1)))
        with_retries(lambda: finish_upload(where, upload_id, parts), f"{key} completion")
    except BaseException:
        abort_upload(where, upload_id)
        raise
    return 200


def start_upload(where, digest):
    """Start a multipart upload that carries the hash of the whole file, and return its id."""
    empty = hashlib.sha256(b"").hexdigest()
    url, headers = sign("POST", where, empty, query={"uploads": ""}, length=0,
                        extra_headers={HASH_HEADER: digest})
    headers["content-type"] = "application/octet-stream"
    r = urllib.request.urlopen(
        urllib.request.Request(url, data=b"", method="POST", headers=headers), timeout=120)
    upload_id = xml_text(r.read(), "UploadId")
    if not upload_id:
        raise RuntimeError(f"the start of the upload of {where} returned no UploadId")
    return upload_id


def put_part(where, upload_id, number, chunk):
    """Send one part and return its ETag, which the completion must list."""
    url, headers = sign("PUT", where, hashlib.sha256(chunk).hexdigest(),
                        query={"partNumber": str(number), "uploadId": upload_id},
                        length=len(chunk))
    r = urllib.request.urlopen(
        urllib.request.Request(url, data=chunk, method="PUT", headers=headers), timeout=1800)
    etag = r.headers.get("ETag")
    if not etag:
        raise RuntimeError(f"part {number} of {where} returned no ETag")
    return etag


def finish_upload(where, upload_id, parts):
    """Complete the upload from its parts in order, and raise when the service reports an error.

    CAUTION: S3 can answer this call with 200 and an Error document in the body. A success
    status alone therefore does not prove that the object exists.
    """
    rows = "".join(f"<Part><PartNumber>{n}</PartNumber><ETag>{escape(e)}</ETag></Part>"
                   for n, e in parts)
    body = f"<CompleteMultipartUpload>{rows}</CompleteMultipartUpload>".encode()
    url, headers = sign("POST", where, hashlib.sha256(body).hexdigest(),
                        query={"uploadId": upload_id}, length=len(body))
    headers["content-type"] = "application/xml"
    r = urllib.request.urlopen(
        urllib.request.Request(url, data=body, method="POST", headers=headers), timeout=600)
    reply = r.read()
    try:
        tag = ET.fromstring(reply).tag.rsplit("}", 1)[-1]
    except ET.ParseError:
        raise RuntimeError(f"the completion of {where} returned a reply that is not XML") from None
    if tag == "Error":
        raise RuntimeError(f"the completion of {where} failed: {xml_text(reply, 'Code')}")


def abort_upload(where, upload_id):
    """Abort an unfinished upload, so that its parts do not remain. It prints any failure."""
    empty = hashlib.sha256(b"").hexdigest()
    url, headers = sign("DELETE", where, empty, query={"uploadId": upload_id})
    try:
        urllib.request.urlopen(
            urllib.request.Request(url, method="DELETE", headers=headers), timeout=120)
        print(f"    aborted the unfinished upload of {where}", flush=True)
    except Exception as e:
        print(f"    the abort of the upload {upload_id} of {where} failed: {e}", flush=True)


def make_bucket(bucket):
    """Create the bucket with a PUT on its name, or use it when this account owns it already.

    Raise RuntimeError when another account holds the name, or when the service refuses the
    PUT with any code that OWN_BUCKET does not hold.
    """
    empty = hashlib.sha256(b"").hexdigest()
    url, headers = sign("PUT", "/" + bucket, empty, length=0)
    try:
        urllib.request.urlopen(
            urllib.request.Request(url, data=b"", method="PUT", headers=headers),
            timeout=120)
        print(f"  bucket {bucket} created", flush=True)
    except urllib.error.HTTPError as e:
        code = xml_text(e.read(), "Code")
        if code in OWN_BUCKET:
            return
        if code == "BucketAlreadyExists":
            raise RuntimeError(f"another account holds the bucket name {bucket}. B2 bucket "
                               f"names are global, so this bucket needs another name") from None
        raise RuntimeError(f"the creation of bucket {bucket} failed: HTTP {e.code}, "
                           f"{code or 'with no S3 code'}") from None


def send(bucket, pairs):
    """Upload each (key, path) pair that the bucket does not already hold unchanged."""
    make_bucket(bucket)
    root = os.path.commonprefix([k for k, _ in pairs]).rpartition("/")[0]
    prefix = root + "/" if root else ""
    have = {k: s for k, s in objects(bucket, prefix)}
    sent = skipped = size_only = 0
    for key, path in pairs:
        size = path.stat().st_size
        digest = None
        if have.get(key) == size:
            meta = head(bucket, key)
            stored = meta.get(HASH_HEADER) if meta is not None else None
            if meta is not None and stored is None:
                size_only += 1
                skipped += 1
                continue
            if stored is not None:
                digest = file_hash(path)
                if stored == digest:
                    skipped += 1
                    continue
        status = put(bucket, key, path, digest)
        sent += 1
        print(f"  {key}  {size/1e6:8.1f} MB  HTTP {status}", flush=True)
    note = f", {size_only} matched on size only" if size_only else ""
    print(f"  {bucket}/{prefix}: {sent} sent, {skipped} already there{note}", flush=True)


def send_shard_set(bucket, prefix, directory):
    """Send a shard set, check it against its own manifest, and send the manifest last.

    The bytes on disk must be the bytes that the manifest describes. The tool reads
    and hashes each shard once, and a shard with a different BLAKE2b digest stops
    the run before the upload. A set that is wrong on disk is wrong in the bucket,
    where it is much harder to find.

    The manifest goes last. An interrupted run then leaves a set without its
    manifest, so a reader cannot take the set for a complete one. A manifest sent
    first would describe shards that are still on their way.
    """
    directory = pathlib.Path(directory)
    manifest_path = directory / "manifest.json"
    if not manifest_path.is_file():
        sys.exit(f"{manifest_path} is not there, so this is not a shard set")
    manifest = json.loads(manifest_path.read_text())
    entries = manifest.get("shards")
    if not entries:
        sys.exit(f"{manifest_path} does not list any shards")

    files = []
    for entry in entries:
        path = directory / entry["name"]
        if not path.is_file():
            sys.exit(f"{path} is named by the manifest and is not there")
        size = path.stat().st_size
        if size != entry["compressed_bytes"]:
            sys.exit(f"{entry['name']} is {size} bytes and the manifest says "
                     f"{entry['compressed_bytes']}")
        digest = hashlib.blake2b(digest_size=32)
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(1 << 22), b""):
                digest.update(block)
        if digest.hexdigest() != entry["blake2b"]:
            sys.exit(f"{entry['name']} does not match its manifest digest")
        files.append(path)
    print(f"  {len(files)} shards checked against the manifest", flush=True)

    send(bucket, [(keyed(prefix, f.name), f) for f in files])
    send(bucket, [(keyed(prefix, manifest_path.name), manifest_path)])


def dataset_files(directory, inner, whole=False):
    """Return the (key, path) pairs of one dataset: its card, then each file under curated/.

    Every key starts with the name of the dataset directory. inner names the folder in the
    bucket that holds the curated tree, and None puts that tree straight under the dataset.
    The config directories inside curated/ stay in both cases. Two configs can hold shards with
    the same file name. whole sends every file of the directory at its own path, for an archive
    that keeps the samples, the scripts and the stats beside the curated tree.
    """
    directory = pathlib.Path(directory)
    if whole:
        return tree_files(directory)
    if inner is not None and (not inner or "/" in inner or inner in (".", "..")):
        raise RuntimeError(f"--inner {inner!r} must be one folder name")
    slug = directory.name
    card = directory / f"{slug}_card.md"
    if not card.is_file():
        raise RuntimeError(f"{card} is not there, and the tool sends a dataset only with its card")
    curated = directory / "curated"
    if not curated.is_dir():
        raise RuntimeError(f"{curated} is not there")
    base = f"{slug}/{inner}" if inner else slug
    pairs = [(f"{slug}/{card.name}", card)]
    for path in sorted(curated.rglob("*")):
        rel = path.relative_to(curated)
        if path.is_file() and not any(is_junk(part) for part in rel.parts):
            pairs.append((f"{base}/{rel.as_posix()}", path))
    if len(pairs) == 1:
        raise RuntimeError(f"{curated} does not hold any files")
    return pairs


def tree_files(directory):
    """Return the (key, path) pairs of every file in a dataset directory, at its own path."""
    slug = directory.name
    if not (directory / f"{slug}_card.md").is_file():
        raise RuntimeError(f"{directory / f'{slug}_card.md'} is not there, "
                           "and the tool sends a dataset only with its card")
    pairs = []
    for path in sorted(directory.rglob("*")):
        rel = path.relative_to(directory)
        if path.is_file() and not path.is_symlink() and not any(is_junk(part) for part in rel.parts):
            pairs.append((f"{slug}/{rel.as_posix()}", path))
    return pairs


def collect_datasets(directories, inner, whole=False):
    """Return the pairs of several datasets, and refuse two datasets with one directory name."""
    seen: dict[str, pathlib.Path] = {}
    pairs: list[tuple[str, pathlib.Path]] = []
    for directory in map(pathlib.Path, directories):
        if directory.name in seen:
            raise RuntimeError(f"{seen[directory.name]} and {directory} are both called "
                               f"{directory.name}, so their keys collide")
        seen[directory.name] = directory
        pairs.extend(dataset_files(directory, inner, whole))
    return pairs


def main(argv=None):
    global STREAMS
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("bucket")
    parser.add_argument("files", nargs="*", type=pathlib.Path)
    parser.add_argument("--prefix", default="",
                        help="a key prefix, so the objects land under one name")
    parser.add_argument("--shard-set", default=None,
                        help="a directory that holds shards and a manifest.json")
    parser.add_argument("--dataset", action="append", type=pathlib.Path, default=[],
                        help="a dataset directory, sent as NAME/ with its NAME_card.md and its "
                             "curated tree. Give it once for each dataset")
    layout = parser.add_mutually_exclusive_group()
    layout.add_argument("--inner", metavar="NAME",
                        help="with --dataset, the folder in the bucket that holds the tree")
    layout.add_argument("--flat", action="store_true",
                        help="with --dataset, the tree sits directly under NAME/")
    layout.add_argument("--whole", action="store_true",
                        help="with --dataset, every file of the directory goes under NAME/ "
                             "at its own path")
    parser.add_argument("--streams", type=int, default=STREAMS,
                        help=f"connections at once for the parts of a file over "
                             f"{PARALLEL_THRESHOLD >> 20} MiB (1 for one, parts only over 5 GB)")
    parser.add_argument("--env", default=DEFAULT_ENV)
    a = parser.parse_args(argv)
    STREAMS = max(1, a.streams)
    if sum((bool(a.files), bool(a.shard_set), bool(a.dataset))) != 1:
        parser.error("name files, one --shard-set or --dataset directories, and only one of them")
    if a.dataset and a.inner is None and not a.flat and not a.whole:
        parser.error("a dataset needs --inner NAME, --flat or --whole, to say what goes where")
    if not a.dataset and (a.inner is not None or a.flat or a.whole):
        parser.error("--inner, --flat and --whole apply only with --dataset")
    load_env(a.env)
    for want in ("S3_ENDPOINT", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"):
        if not os.environ.get(want):
            sys.exit(f"{want} is not set, so the .env did not load")
    try:
        if a.shard_set:
            return send_shard_set(a.bucket, a.prefix, a.shard_set)
        if a.dataset:
            pairs = collect_datasets(a.dataset, None if a.flat else a.inner, a.whole)
            return send(a.bucket, [(keyed(a.prefix, k), p) for k, p in pairs])
        missing = [f for f in a.files if not f.is_file()]
        if missing:
            sys.exit(f"not a file: {missing[0]}")
        send(a.bucket, [(keyed(a.prefix, f.name), f) for f in a.files])
    except RuntimeError as e:
        sys.exit(str(e))


if __name__ == "__main__":
    main()
