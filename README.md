[![Tests Workflow Status (main)](https://img.shields.io/github/actions/workflow/status/lostatc/acid-store/test.yaml?branch=main&label=Tests&style=for-the-badge&logo=github)](https://github.com/lostatc/acid-store/actions/workflows/test.yaml)
[![Codecov](https://img.shields.io/codecov/c/github/lostatc/acid-store?logo=codecov&style=for-the-badge)](https://app.codecov.io/gh/lostatc/acid-store)
[![Crates.io](https://img.shields.io/crates/v/acid-store?logo=rust&style=for-the-badge)](https://crates.io/crates/acid-store)
[![docs.rs](https://img.shields.io/docsrs/acid-store?logo=docsdotrs&style=for-the-badge)](https://docs.rs/acid-store)

⚠ **UNMAINTAINED**

*I am no longer maintaining this project.*

# acid-store

acid-store is a Rust library for secure, deduplicated, and transactional data
storage.

This library provides abstractions for data storage over a number of storage
backends. You can turn any storage backend into an encrypted and deduplicated
object store, persistent collection, or virtual file system (which can be
mounted via FUSE).

Out of the box, this library supports the local file system, SQLite, Redis,
Amazon S3, SFTP, and many cloud providers as storage backends. Storage backends
are easy to implement, and this library builds on top of them to provide
encryption, compression, content-based deduplication, locking, and atomic
transactions.

For details and examples, see the [documentation](https://docs.rs/acid-store).

⚠ This project is experimental! ⚠

This project experiences frequent breaking API changes and hasn't seen
significant real-world usage. This project is not ready for use in production
environments. Also keep in mind that this code has not been audited for
security.

## Features

- Optional encryption of all data and metadata using XChaCha20-Poly1305 and
  Argon2, via [libsodium](https://download.libsodium.org/doc/)
- Optional compression using LZ4
- Optional content-based deduplication
- Supports packing data into fixed-size blocks to avoid metadata leakage when
  using encryption
- Integrity checking of data and metadata using checksums and (if encryption is
  enabled) AEAD
- Locking protects against concurrent access from multiple clients
- Copy-on-write semantics
- New storage backends are easy to implement

### Abstractions

This library provides the following abstractions for data storage.

- An object store which maps keys to seekable binary blobs
- A virtual file system which can be mounted via FUSE and supports file
  metadata, special files, sparse files, hard links, and importing and exporting
  files to the local file system
- A persistent, heterogeneous, map-like collection

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

The following results show read and write speeds using an in-memory storage
backend. You can run the benchmarks yourself by running `cargo bench --features
'encryption'`.

### Specs

| Spec      | Value                |
| --------- | -------------------- |
| Processor | Ryzen 5 1600x        |
| Memory    | 32 GB DDR4 (3200MHz) |
| OS        | Linux 5.11           |

### Results

| Chunking | Packing | Encryption         | Compression | Read       | Write      |
| -------- | ------- | ------------------ | ----------- | ---------- | ---------- |
| Fixed    | None    | None               | None        | 6090 MiB/s | 1920 MiB/s |
| ZPAQ     | None    | None               | None        | 2670 MiB/s | 520 MiB/s  |
| Fixed    | Fixed   | XChaCha20-Poly1305 | None        | 870 MiB/s  | 610 MiB/s  |
| ZPAQ     | Fixed   | XChaCha20-Poly1305 | None        | 840 MiB/s  | 300 MiB/s  |

## MSRV Policy

The last two stable Rust releases are supported. Older releases may be supported
as well.

The MSRV will only be increased when necessary to take advantage of new Rust
features—not every time there is a new Rust release. An increase in the MSRV
will be accompanied by a minor semver bump if >=1.0.0 or a patch semver bump if
<1.0.0.

This policy was added with v0.13.0.

## Semver Policy

Prior to version 1.0.0, breaking changes will be accompanied by a minor version
bump, and new features and bug fixes will be accompanied by a patch version
bump.

This policy was added with v0.13.0.

## Similar Projects

Here are some similar projects to check out:

- [Persy](https://persy.rs/)
- [ZboxFS](https://zbox.io/fs/)

## Copyright

Copyright © 2019-2023 Wren Powell

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.