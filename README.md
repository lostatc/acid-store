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
- Optional compression using LZ4
- Optional content-based deduplication using the ZPAQ chunking algorithm
- Supports packing data into fixed-size blocks to avoid metadata leakage
- Integrity checking of data and metadata using checksums and (if encryption is enabled) AEAD
- Transactional operations providing atomicity, consistency, isolation, and durability (ACID)
- Copy-on-write semantics
- New storage backends are easy to implement

### Abstractions

This library provides the following abstractions for data storage.

- An object store which maps keys to seekable binary blobs
- A virtual file system which supports file metadata, special files, importing and exporting
files to the local OS file system, and being mounted via FUSE
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

The following results show read and write speeds for an in-memory repository with various configurations. An in-memory
repository is used to make benchmark results more consistent between runs and between machines. You can run the
benchmarks yourself by running `cargo bench --all-features`.

### Specs

Spec | Value
--- | ---
Processor | Ryzen 5 1600x
Memory | 32 GB (3200MHz)
OS | Linux 5.11

### Results

Chunking | Packing | Encryption | Compression | Read | Write
--- | --- | --- | --- | --- | ---
Fixed | None | None | None | 4680 MiB/s | 890 MiB/s
ZPAQ | None | None | None | 2870 MiB/s | 500 MiB/s
Fixed | Fixed | XChaCha20-Poly1305 | None | 820 MiB/s | 600 MiB/s
ZPAQ | Fixed | XChaCha20-Poly1305 | None | 820 MiB/s | 300 MiB/s

