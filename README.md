[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/lostatc/acid-store/Tests/main?label=tests&logo=github&style=for-the-badge)](https://github.com/lostatc/acid-store/actions?query=workflow%3ATests)
[![Codecov](https://img.shields.io/codecov/c/github/lostatc/acid-store?logo=codecov&style=for-the-badge)](https://app.codecov.io/gh/lostatc/acid-store)
[![Crates.io](https://img.shields.io/crates/v/acid-store?logo=rust&style=for-the-badge)](https://crates.io/crates/acid-store)
[![docs.rs](https://img.shields.io/docsrs/acid-store?logo=docsdotrs&style=for-the-badge)](https://docs.rs/acid-store)

# acid-store

`acid-store` is a library for secure, deduplicated, transactional, and
verifiable data storage.

This library provides high-level abstractions for data storage over a number of
storage backends. The goal is to decouple how you access your data from where
you store it. You can access your data as an object store, a virtual file
system, a persistent collection, or a content-addressable storage, regardless of
where the data is stored. Out of the box, this library supports the local file
system, SQLite, Redis, Amazon S3, SFTP, and many cloud providers as storage
backends. Storage backends are easy to implement, and this library builds on top
of them to provide features like encryption, compression, deduplication,
locking, and atomic transactions.

For details and examples, see the [documentation](https://docs.rs/acid-store).

⚠️ This project is still experimental; it experiences frequent breaking API
changes and hasn't been tested thoroughly. This project is not ready for use in
production environments. Please remember to back up your data if you choose to
use this library. Also keep in mind that this code has not been audited for
security.

## Features

- Optional encryption of all data and metadata using XChaCha20-Poly1305 and
  Argon2, powered by [libsodium](https://download.libsodium.org/doc/)
- Optional compression using LZ4
- Optional content-based deduplication using the ZPAQ chunking algorithm
- Supports packing data into fixed-size blocks to avoid metadata leakage
- Integrity checking of data and metadata using checksums and (if encryption is
  enabled) AEAD
- Transactional operations providing atomicity, consistency, isolation, and
  durability (ACID)
- Two-phase locking protects against concurrent access from multiple clients
- Copy-on-write semantics
- New storage backends are easy to implement

### Abstractions

This library provides the following abstractions for data storage.

- An object store which maps keys to seekable binary blobs
- A virtual file system which supports file metadata, special files, sparse
  files, hard links, importing and exporting files to the local OS file system,
  and being mounted via FUSE
- A persistent, heterogeneous, map-like collection
- An object store with support for content versioning
- A content-addressable storage which allows for accessing data by its
  cryptographic hash

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

The following results show read and write speeds for an in-memory repository
with various configurations. An in-memory repository is used to make benchmark
results more consistent between runs and between machines. You can run the
benchmarks yourself by running `cargo bench --all-features`.

### Specs

| Spec      | Value           |
| --------- | --------------- |
| Processor | Ryzen 5 1600x   |
| Memory    | 32 GB (3200MHz) |
| OS        | Linux 5.11      |

### Results

| Chunking | Packing | Encryption         | Compression | Read       | Write      |
| -------- | ------- | ------------------ | ----------- | ---------- | ---------- |
| Fixed    | None    | None               | None        | 6090 MiB/s | 1920 MiB/s |
| ZPAQ     | None    | None               | None        | 2670 MiB/s | 520 MiB/s  |
| Fixed    | Fixed   | XChaCha20-Poly1305 | None        | 870 MiB/s  | 610 MiB/s  |
| ZPAQ     | Fixed   | XChaCha20-Poly1305 | None        | 840 MiB/s  | 300 MiB/s  |

## Copyright

Copyright 2019-2022 Wren Powell

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
