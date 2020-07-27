[![Tests](https://github.com/lostatc/acid-store/workflows/Tests/badge.svg)](https://github.com/lostatc/acid-store/actions?query=workflow%3ATests)
[![codecov](https://codecov.io/gh/lostatc/acid-store/branch/main/graph/badge.svg)](https://codecov.io/gh/lostatc/acid-store)
[![crates.io](https://img.shields.io/crates/v/acid-store)](https://crates.io/crates/acid-store)
[![docs.rs](https://docs.rs/acid-store/badge.svg)](https://docs.rs/acid-store)

# acid-store

`acid-store` is a library for secure, deduplicated, transactional, and verifiable data storage.

This library provides high-level abstractions for data storage over a number of storage backends.
The goal is to decouple how you access your data from where you store it. You can access your data
as an object store, a virtual file system, a persistent collection, or a content-addressable
storage, regardless of where the data is stored. Out of the box, this library supports the local
file system, SQLite, Redis, Amazon S3, SFTP, and many cloud providers as storage backends. Storage
backends are easy to implement, and this library builds on top of them to provide features like
encryption, compression, deduplication, locking, and atomic transactions.

For details and examples, see the [documentation](https://docs.rs/acid-store).

⚠️ This project is still immature and needs more testing. Testers are always appreciated, but please
remember to back up your data! Also keep in mind that this code has not been audited for security.
All the usual disclaimers apply.

## Features
- Optional encryption of all data and metadata using XChaCha20-Poly1305 and Argon2, powered by
[libsodium](https://download.libsodium.org/doc/)
- Optional compression using DEFLATE, LZMA, or LZ4
- Optional content-based deduplication using the ZPAQ chunking algorithm
- Integrity checking of data and metadata using checksums and (if encryption is enabled) AEAD
- Transactional operations providing atomicity, consistency, isolation, and durability (ACID)
- Copy-on-write semantics
- New storage backends are easy to implement

### Abstractions

This library provides the following abstractions for data storage.

- An object store which maps keys to seekable binary blobs
- A virtual file system which supports file metadata, special files, and importing and exporting
files to the local OS file system
- A persistent, heterogeneous, map-like collection
- An object store with support for content versioning
- A content-addressable storage which allows for accessing data by its cryptographic hash

### Backends

This library provides the following storage backends out of the box.

- Local file system directory
- SQLite
- Redis
- Amazon S3
- SFTP
- Cloud storage via [rclone](https://rclone.org/)
- In-Memory

## Benchmarks

You can run the benchmarks yourself by running `cargo bench --all-features`.

### Specs

Spec | Value
--- | ---
Processor | Ryzen 5 1600x
Memory | 16 GB (2133MHz)
Disk | Crucial MX500 SATA SSD
OS | Ubuntu 19.10

### Results

Encryption | Chunking | Read | Write
--- | --- | --- | ---
None | Fixed | 1555 MiB/s | 290 MiB/s
XChaCha20-Poly1305 | Fixed | 630 MiB/s | 210 MiB/s
None | ZPAQ | 1600 MiB/s | 185 MiB/s
XChaCha20-Poly1305 | ZPAQ | 710 MiB/s | 125 MiB/s

## Contributing

Some tests are not run in GitHub Actions because they rely on outside resources. These tests must be
run locally and be configured with environment variables. Below is a table of what those environment
variables are and what Cargo features they are associated with. The variables only need to be set if
their corresponding Cargo features are enabled when running the test suite.

| Variable | Description | Feature |
| --- | --- | --- |
| `REDIS_URL` | The `redis://` URL of the Redis server to test against. | `store-redis` |
| `S3_BUCKET` | The name of the S3 bucket to test against. | `store-s3` |
| `S3_ACCESS_KEY` | The access key for accessing the S3 bucket. | `store-s3` |
| `S3_SECRET_KEY` | The secret key for accessing the S3 bucket. | `store-s3` |
| `RCLONE_REMOTE` | The `<remote>:<path>` string for the rclone remote to test against. | `store-rclone` |
| `SFTP_SERVER` | The URL of the SFTP server to test against. | `store-sftp` |
| `SFTP_PATH` | The path to use on the SFTP server. | `store-sftp` |
| `SFTP_USERNAME` | The username to access the SFTP server. | `store-sftp` |
| `SFTP_PASSWORD` | The password to access the SFTP server. | `store-sftp` |
