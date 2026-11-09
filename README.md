---
type: Repository Guide
title: S3 Tools
description: Dependency-free Python tools to list, upload, download and delete objects in S3-compatible stores, such as Cloudflare R2 and Backblaze B2.
status: stable
tags: [data, s3, r2, b2, storage]
generated:
  by: claude-code/opus-5.5
  at: 2026-09-29T18:00:00Z
supervised:
  by: human:ciprian-florin_ifrim
  at: 2026-09-29T18:00:00Z
edited:
  by: claude-code/opus-5.5
  at: 2026-10-20T19:55:58Z
---

# S3 Tools

Two Python tools use S3-compatible object stores, such as Cloudflare R2 and Backblaze B2, and they make their SigV4 signatures by hand. They need only the standard library, so they run on any machine with Python 3.10 or later.

An upload streams its body from disk, and the signature uses the digest from a first pass. A 500 MB shard therefore stays on disk for the whole upload, and a file over 5 GB goes to the bucket in parts.

| What | Where |
|---|---|
| List buckets and objects, download, delete | `s3.py` |
| Upload files, shard sets and datasets | `push.py` |
| Unit suite behind a fake S3 service | `tests/` |

**Each command can run again safely, because an upload and a download both skip what is already complete.**

## 1. Build and Run

    python3 s3.py buckets
    python3 s3.py ls BUCKET [--prefix P]
    python3 s3.py get BUCKET --prefix P --out DIR
    python3 s3.py get BUCKET --key K --out PATH
    python3 s3.py rm BUCKET --prefix P --yes
    python3 push.py BUCKET FILE... [--prefix P]
    python3 push.py BUCKET --shard-set DIR [--prefix P]
    python3 push.py BUCKET --dataset DIR [--dataset DIR...] --inner NAME [--prefix P]
    python3 push.py BUCKET --dataset DIR [--dataset DIR...] --flat [--prefix P]
    python3 push.py BUCKET --dataset DIR [--dataset DIR...] --whole [--prefix P]
    python3 -m pytest tests                  # the unit suite, with no network and no credential

The tools run from the source, without a build or a package install, and the unit suite needs pytest.

### 1.1 Credentials

| Variable | Value |
|---|---|
| `S3_ENDPOINT` | the account endpoint |
| `S3_ACCESS_KEY_ID` | the access key |
| `S3_SECRET_ACCESS_KEY` | the secret key |
| `S3_REGION` | the region of the endpoint, and `auto` when it is absent |

Both tools read these variables from the file that `--env FILE` names, which is `.env` in the working directory by default. If the file is absent, they read the variables from the environment. A variable that is still absent stops the tool, and the message names it.

R2 accepts the region `auto`, but B2 rejects a signature unless it names the region in its endpoint, as `eu-central-003` in `s3.eu-central-003.backblazeb2.com`. With one file for each service, such as `r2.env` and `b2.env`, the same tools reach either one, and git ignores every `*.env` file.

## 2. Directory Tree

    s3.py      signatures, lists, heads, downloads and deletes
    push.py    uploads, retries, multipart uploads, shard sets and datasets
    tests/     the unit suite, with one file for each concern

## 3. Concepts

**A prefix matches the start of a key.** Both services store keys in one flat list, so `ls --prefix general-32k` matches every key that starts with those characters, and `rm --prefix` deletes each of them.

**A PUT is atomic.** A broken upload leaves the bucket as it was before the PUT, so a retry is safe without a cleanup step.

**A stored hash shows that an object is current.** A size match alone cannot tell an edited file from the original, so each upload stores the SHA-256 of its file in `x-amz-meta-sha256`.

## 4. Rules

**A delete needs `--yes`.** The tool prints the count and the size first, because a prefix that matches too much is the one mistake that it cannot undo.

**Never pass a credential as an argument, and never log one.** An argument reaches the process list and the shell history.

**Create each B2 bucket with the lifecycle rule "Keep only the last version".** B2 keeps every overwritten or deleted version of an object, and each one bills as storage until a rule removes it.

**Give each B2 bucket a name that is free across all of B2.** B2 bucket names are global, so `push.py` stops on a name that another account holds.

## 5. Uploads

| Behavior | Reason |
|---|---|
| `push.py` lists the bucket first, and skips a key whose size and stored hash both match the file | A restart must send only what is still absent, and an edit can keep the byte count |
| An object without a stored hash keeps the size check, and the run counts it | An older version of the tool stored no hash, and a new upload of each such object costs a full transfer |
| `push.py` tries a failed request again up to 6 times, and each pause is 5 seconds longer than the last | A short network fault must not stop a long upload |
| Each new try signs the request again | The signature carries a timestamp, so an old signature fails |
| A file over 5 GB goes to the bucket as a multipart upload, in parts of 100 MiB | B2 accepts at most 5 GB in one upload |
| With `--streams` above 1 (4 by default), a file over 200 MiB goes up in parts too, that many parts at once | One connection is often far slower than the link |
| A failure after the start of a multipart upload aborts it | An unfinished upload keeps its parts in the bucket |
| `push.py` creates the bucket when it does not exist | An upload to a new bucket then needs one command |

## 6. Shard Sets

`--shard-set DIR` sends a directory that holds shards and a `manifest.json`. The tool checks every shard before it sends anything.

| Step | Check |
|---|---|
| 1 | Each shard that the manifest names must exist, at the size in `compressed_bytes` |
| 2 | Each shard must match its BLAKE2b digest in the manifest |
| 3 | The shards go first |
| 4 | The manifest goes last |

**Send the manifest after its shards.** An interrupted run then leaves shards without a manifest, and a reader cannot take the set for a complete one.

## 7. Datasets

`--dataset DIR` sends one dataset: its card `DATASET_card.md` and every file under `curated/`, where `DATASET` is the name of the directory. Other directories, such as the samples, stay on disk, and so do `.DS_Store` and `._*` files. `--whole` sends every directory instead, for an archive that keeps the samples, the scripts and the stats beside the curated tree.

Every key starts with `DATASET/`, and the card always sits at `DATASET/DATASET_card.md`. `--inner NAME`, `--flat` or `--whole` sets what goes where, and one of the three is mandatory:

| Option | Key of one shard |
|---|---|
| `--inner curated` | `DATASET/curated/CONFIG/train-00000-of-00001.parquet` |
| `--flat` | `DATASET/CONFIG/train-00000-of-00001.parquet` |
| `--whole` | `DATASET/curated/CONFIG/train-00000-of-00001.parquet`, and every other file at its own path, such as `DATASET/samples/raw/run.jsonl` |

The config directories stay in every layout: two configs often hold shards with the same file name. With `--inner curated`, the bucket holds the same layout as the directory on disk.

| Behavior | Reason |
|---|---|
| A dataset without its card stops the run | A copy without the card cannot say what its data holds |
| Two datasets with the same directory name stop the run | Their keys collide |
| `--prefix P` goes above the dataset, as `P/DATASET/` | One bucket can then hold several archives |

## 8. Downloads

| Behavior | Reason |
|---|---|
| `get` writes each key under `--out` | A prefix keeps its own shape on disk |
| Each object streams to a `.part` file, and the file takes its name when it is complete | An interrupted download leaves a `.part` file, and only a complete file takes the final name |
| An object of 64 MiB or more comes down in ranges of 32 MiB over `--streams` connections at once (8 by default), each range written at its place in the file | One connection is often far slower than the link |
| A range whose connection breaks or whose body ends early comes down again, up to 6 times, and each pause is 5 seconds longer | A broken connection then costs one range in place of the whole object |
| A file whose object carries a stored hash is checked against it before it takes its name | A range written at the wrong place would otherwise go unseen |
| A file that already exists at the same size is skipped | An interrupted run continues with the next object, and a complete file is not fetched again |
| A prefix fetches its objects under 64 MiB `--streams` at a time, each over one connection | A set of many small files would otherwise wait on one request after another |

## 9. Outputs

| Command | Output |
|---|---|
| `ls` | one line for each object with its size, then the count and the total |
| `get` | one line for each object, then the counts of fetched and skipped objects and the total size |
| `push.py` | one line for each object with its size and the HTTP status, one line for each part of a large file, then the counts of sent, skipped and size-only objects |
| `rm` | the count and the size, then one line for each deleted object |

## 10. Limitations

| Limitation | Reason |
|---|---|
| An interrupted run fetches that object again from the start | The tool does not keep the ranges of an interrupted run, and within a run only a broken range comes down again |
| Buckets are deleted by hand | `push.py` creates a bucket when it needs one, and a bucket deletion is rare |
| On B2, `ls` lists only the current versions, and `rm` hides each object until the lifecycle rule deletes it | B2 versions every object, and the rule deletes a hidden version one day later |
