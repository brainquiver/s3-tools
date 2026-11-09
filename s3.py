"""List, download and delete objects in an S3-compatible store, with SigV4 signed by hand.

    python3 s3.py buckets
    python3 s3.py ls BUCKET [--prefix P]
    python3 s3.py get BUCKET --prefix P --out DIR
    python3 s3.py get BUCKET --key K --out PATH
    python3 s3.py rm BUCKET --prefix P --yes

LIST needs a canonical query string, so the signer here takes query parameters.
An upload signs an empty one. The rest of the signature is the same.

The signature names the region in S3_REGION, or auto when that variable is absent.
R2 accepts auto, and Backblaze B2 needs the region of its endpoint. With one .env for
each service, the same tools reach either service.

A delete needs --yes, because a prefix that matches more than intended is the
one mistake that this tool cannot undo.

A get writes the key under --out, and a prefix keeps its own folder structure. A
prefix fetches its small objects --streams at a time.
It streams each object to disk, because an object can be a checkpoint of several
gigabytes. It skips a file that is already there at the same size, so a second
run fetches only the files that are still absent.

An object of STREAM_MIN bytes or more comes down as ranges of CHUNK bytes over
--streams connections at once, each range written at its place in the file. One
connection is often far slower than the link, and a connection that breaks then
costs one range, which the tool fetches again. When the object carries its
SHA-256 (x-amz-meta-sha256), the tool checks the file against it.
"""
import argparse, concurrent.futures, datetime, hashlib, hmac, http.client, os, pathlib, sys, time
import urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET

NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

# The credentials file, relative to the working directory. Git ignores it, and
# the tools keep its values out of the output, the logs and the manifests.
DEFAULT_ENV = ".env"


def load_env(path):
    """Read the .env, and keep every value out of the output.

    CONTRACT: an absent file is acceptable when the environment already holds the
    credentials. A machine can set them in its environment without a .env file,
    and an error here would stop the tool on that machine. The caller checks the
    three variables afterwards, so an environment without them still fails, and
    the message names the absent variable.
    """
    if not pathlib.Path(path).exists():
        return
    for line in pathlib.Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def sign(method, path, payload_hash, query=None, length=None, extra_headers=None):
    """Return the URL and the headers of a request with a SigV4 signature.

    The signature covers extra_headers with the rest, so a proxy cannot drop or change one
    unseen. The stored hash of an upload travels this way, as x-amz-meta-sha256.
    """
    ep = os.environ["S3_ENDPOINT"].rstrip("/")
    ak, sk = os.environ["S3_ACCESS_KEY_ID"], os.environ["S3_SECRET_ACCESS_KEY"]
    # NOTE: R2 accepts the region auto for every request. B2 and AWS reject a signature unless
    # it names the region of the endpoint, as eu-central-003 in s3.eu-central-003.backblazeb2.com.
    region = os.environ.get("S3_REGION") or "auto"
    host = ep.split("//", 1)[1]
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, ds = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz}
    headers.update({k.lower(): v for k, v in (extra_headers or {}).items()})
    names = ";".join(sorted(headers))
    canon_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
    # The canonical query string is sorted by key and each part is encoded.
    q = "&".join(f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(str(v), safe='')}"
                 for k, v in sorted((query or {}).items()))
    canon = f"{method}\n{path}\n{q}\n{canon_headers}\n{names}\n{payload_hash}"
    scope = f"{ds}/{region}/s3/aws4_request"
    sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(canon.encode()).hexdigest()}"
    step = lambda k, m: hmac.new(k, m.encode(), hashlib.sha256).digest()
    key = step(step(step(step(("AWS4" + sk).encode(), ds), region), "s3"), "aws4_request")
    sig = hmac.new(key, sts.encode(), hashlib.sha256).hexdigest()
    out = dict(headers)
    out["Authorization"] = (f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, "
                            f"SignedHeaders={names}, Signature={sig}")
    if length is not None:
        out["Content-Length"] = str(length)
    return ep + path + (f"?{q}" if q else ""), out


def call(method, path, query=None, expect=(200, 204)):
    empty = hashlib.sha256(b"").hexdigest()
    url, headers = sign(method, path, empty, query=query)
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, method=method, headers=headers), timeout=300)
        return r.read()
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} failed: {e.code} {e.read().decode(errors='replace')[:300]}")


# Objects of STREAM_MIN bytes or more come down in ranges of CHUNK bytes over STREAMS connections
STREAMS = 8
STREAM_MIN = 64 * 1024 * 1024
CHUNK = 32 * 1024 * 1024
TRIES = 6
RANGE_TIMEOUT = 60
HASH_HEADER = "x-amz-meta-sha256"


def with_retries(action, what):
    """Run action until it succeeds, TRIES times at most. Each pause is 5 seconds longer.

    It tries again after a fault of the network, a short body or a 5xx or 429 reply. Any other
    HTTP error is final, so it stops the tool at once.
    """
    for attempt in range(1, TRIES + 1):
        try:
            return action()
        except urllib.error.HTTPError as e:
            if e.code < 500 and e.code != 429:
                sys.exit(f"{what} failed: {e.code} {e.read().decode(errors='replace')[:300]}")
            err = e
        except (OSError, http.client.HTTPException) as e:
            err = e
        if attempt == TRIES:
            sys.exit(f"{what} failed after {TRIES} tries: {err}")
        print(f"    {type(err).__name__} on try {attempt} of {what}, next try in {5 * attempt}s",
              flush=True)
        time.sleep(5 * attempt)


def buckets():
    root = ET.fromstring(call("GET", "/"))
    rows = [(b.findtext(NS + "Name"), b.findtext(NS + "CreationDate", "")[:10])
            for b in root.iter(NS + "Bucket")]
    if not rows:
        print("this account does not have any buckets")
        return
    w = max(len(n) for n, _ in rows)
    for name, made in rows:
        print(f"  {name.ljust(w)}  created {made}")


def objects(bucket, prefix=""):
    """Every key under the prefix, read through each continuation token to the end."""
    out, token = [], None
    while True:
        q = {"list-type": "2", "max-keys": "1000"}
        if prefix:
            q["prefix"] = prefix
        if token:
            q["continuation-token"] = token
        root = ET.fromstring(call("GET", f"/{bucket}", query=q))
        for c in root.iter(NS + "Contents"):
            out.append((c.findtext(NS + "Key"), int(c.findtext(NS + "Size", "0"))))
        if root.findtext(NS + "IsTruncated") == "true":
            token = root.findtext(NS + "NextContinuationToken")
        else:
            return out


def head(bucket, key):
    """Return the response headers of one object, or None when the bucket lacks the key.

    The user metadata of an object, such as x-amz-meta-sha256, arrives only here. A listing
    does not carry it.
    """
    empty = hashlib.sha256(b"").hexdigest()
    url, headers = sign("HEAD", f"/{bucket}/{urllib.parse.quote(key, safe='/')}", empty)
    try:
        with urllib.request.urlopen(
                urllib.request.Request(url, method="HEAD", headers=headers), timeout=300) as r:
            return r.headers
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def fetch(path, fd, start, end, what):
    """Write bytes start to end (both included) of an object at their place in the open file.

    A body that ends early raises, so the caller fetches the range again.
    """
    def attempt():
        empty = hashlib.sha256(b"").hexdigest()
        # The signature carries a timestamp, so each try signs again.
        url, headers = sign("GET", path, empty)
        headers["Range"] = f"bytes={start}-{end}"
        pos = start
        # A connection that stalls raises after RANGE_TIMEOUT seconds without a byte, and the
        # range comes down again, in place of waiting out a long timeout
        with urllib.request.urlopen(
                urllib.request.Request(url, method="GET", headers=headers), timeout=RANGE_TIMEOUT) as r:
            for chunk in iter(lambda: r.read(1 << 20), b""):
                os.pwrite(fd, chunk, pos)
                pos += len(chunk)
        if pos != end + 1:
            raise OSError(f"the body ended after {pos - start} of {end + 1 - start} bytes")
    with_retries(attempt, what)


def download(bucket, key, out, size=None, streams=None):
    """Fetch one object to a path, and return the number of bytes written.

    CONTRACT: stream it. call() reads a whole response into memory, which suits a
    listing, and an object can be a checkpoint of several gigabytes. An object of
    STREAM_MIN bytes or more comes down as ranges over several connections, and the
    file is checked against the object's stored SHA-256 when it has one.
    """
    streams = STREAMS if streams is None else streams
    path = f"/{bucket}/{urllib.parse.quote(key, safe='/')}"
    meta = head(bucket, key) if size is None or (streams > 1 and size >= STREAM_MIN) else None
    if size is None:
        if meta is None:
            sys.exit(f"{key} is not in {bucket}")
        size = int(meta.get("Content-Length", "0"))
    out.parent.mkdir(parents=True, exist_ok=True)
    part = out.with_suffix(out.suffix + ".part")
    fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.ftruncate(fd, size)
        ranges = [(a, min(a + CHUNK, size) - 1) for a in range(0, size, CHUNK)] or [(0, -1)]
        if streams > 1 and size >= STREAM_MIN:
            with concurrent.futures.ThreadPoolExecutor(max_workers=streams) as pool:
                jobs = [pool.submit(fetch, path, fd, a, b, f"GET {key} bytes {a}-{b}") for a, b in ranges]
                for j in concurrent.futures.as_completed(jobs):
                    j.result()
        elif size:
            fetch(path, fd, 0, size - 1, f"GET {key}")
    except BaseException:
        os.close(fd)
        part.unlink(missing_ok=True)
        raise
    os.close(fd)
    stored = meta.get(HASH_HEADER) if meta is not None else None
    if stored:
        h = hashlib.sha256()
        with open(part, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 22), b""):
                h.update(chunk)
        if h.hexdigest() != stored:
            part.unlink(missing_ok=True)
            sys.exit(f"GET {key}: the file does not match the stored SHA-256")
    # CAUTION: rename only when the body is complete. An interrupted get then leaves
    # a .part file, and the size check accepts only a complete file.
    part.replace(out)
    return size


def get(bucket, prefix, key, out, streams=None):
    """Fetch one key, or every key under a prefix, under out."""
    if key:
        target = out / key.rsplit("/", 1)[-1] if out.is_dir() else out
        if target.exists() and target.stat().st_size:
            print(f"  {target} already {target.stat().st_size:,} bytes, skipped")
            return
        n = download(bucket, key, target, streams=streams)
        print(f"  {key} -> {target}  {n:,} bytes")
        return
    found = objects(bucket, prefix)
    if not found:
        sys.exit(f"nothing under {prefix!r} in {bucket}")
    todo = [(k, size) for k, size in found
            if not ((out / k).exists() and (out / k).stat().st_size == size)]
    skipped = len(found) - len(todo)
    streams = STREAMS if streams is None else streams
    # The small objects several at a time, each over one connection; the large ones one
    # after another, each over its own ranges
    small = [(k, s) for k, s in todo if s < STREAM_MIN]
    large = [(k, s) for k, s in todo if s >= STREAM_MIN]
    total = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, streams)) as pool:
        total += sum(pool.map(lambda ks: download(bucket, ks[0], out / ks[0], ks[1], 1), small))
    for k, size in large:
        total += download(bucket, k, out / k, size, streams)
    print(f"  {len(todo)} fetched, {skipped} already there, {total/1e6:.1f} MB into {out}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["buckets", "ls", "get", "rm"])
    p.add_argument("bucket", nargs="?")
    p.add_argument("--prefix", default="")
    p.add_argument("--key", default="", help="one object, in place of a prefix")
    p.add_argument("--out", type=pathlib.Path, help="where a get writes")
    p.add_argument("--yes", action="store_true")
    p.add_argument("--streams", type=int, default=STREAMS,
                   help=f"connections at once for an object of {STREAM_MIN >> 20} MiB or more (1 for one)")
    p.add_argument("--env", default=DEFAULT_ENV)
    a = p.parse_args()
    load_env(a.env)
    for want in ("S3_ENDPOINT", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"):
        if not os.environ.get(want):
            sys.exit(f"{want} is not set, so the .env did not load")

    if a.action == "buckets":
        return buckets()
    if not a.bucket:
        sys.exit("that action needs a bucket")
    if a.action == "get":
        if not a.out:
            sys.exit("a get needs --out, a directory for a prefix or a path for a key")
        return get(a.bucket, a.prefix, a.key, a.out, max(1, a.streams))
    found = objects(a.bucket, a.prefix)
    total = sum(s for _, s in found)
    if a.action == "ls":
        for k, s in found:
            print(f"  {s/1e6:10.1f} MB  {k}")
        print(f"  {len(found)} objects, {total/1e9:.2f} GB")
        return
    if not found:
        print("0 objects match, so there is nothing to delete")
        return
    print(f"{len(found)} objects, {total/1e9:.2f} GB, under {a.bucket}/{a.prefix or '(all)'}")
    if not a.yes:
        sys.exit("a delete needs --yes")
    for i, (k, _) in enumerate(found, 1):
        call("DELETE", f"/{a.bucket}/{urllib.parse.quote(k, safe='/')}")
        print(f"  deleted {i}/{len(found)}  {k}", flush=True)


if __name__ == "__main__":
    main()
