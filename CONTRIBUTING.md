# Contributing

## Tests

If you run the tests locally, you will probably want to run them without the
`store-*` cargo features, because many of the `DataStore` tests rely on outside
services like S3, Redis, etc.

You will almost always want to run the tests with the `encryption` and
`compression` features enabled, because the way the test suite is architectured
means that most of the tests require those features.

### `DataStore` Tests

If you want to run tests for the different `DataStore` implementations, you will
need to provide the necessary services, either mocked or real, for them to test
against. You can configure the following environment variables to do this.

Below is a table of what those environment variables are and what Cargo features
they are associated with. The variables only need to be set if their
corresponding Cargo features are enabled when running the test suite. You can
specify these environment variables in a
[dotenv](https://crates.io/crates/dotenv) file and they will be loaded
automatically.

| Variable        | Description                                                         | Feature        |
| --------------- | ------------------------------------------------------------------- | -------------- |
| `REDIS_URL`     | The `redis://` URL of the Redis server to test against.             | `store-redis`  |
| `S3_BUCKET`     | The name of the S3 bucket to test against.                          | `store-s3`     |
| `S3_REGION`     | The name of the AWS region containing the S3 bucket.                | `store-s3`     |
| `S3_ACCESS_KEY` | The access key ID for accessing the S3 bucket.                      | `store-s3`     |
| `S3_SECRET_KEY` | The secret access key for accessing the S3 bucket.                  | `store-s3`     |
| `RCLONE_REMOTE` | The `<remote>:<path>` string for the rclone remote to test against. | `store-rclone` |
| `SFTP_SERVER`   | The URL of the SFTP server to test against.                         | `store-sftp`   |
| `SFTP_PATH`     | The path to use on the SFTP server.                                 | `store-sftp`   |
| `SFTP_USERNAME` | The username to access the SFTP server.                             | `store-sftp`   |
| `SFTP_PASSWORD` | The password to access the SFTP server.                             | `store-sftp`   |

### FUSE Tests

To test the FUSE file system implementation provided by this library, the
`/fuse-test` directory contains a `Dockerfile` which provides a test environment
containing [a number of file system testing
tools](https://github.com/billziss-gh/secfs.test). The `Dockerfile` builds
`acid-store` and provides a binary which mounts a FUSE file system backed by a
`MemoryStore`. To mount the FUSE file system, the container needs special
permissions and access to the host's `/dev/fuse` device.

To build the docker image:

```shell
docker build -t fuse-test -f ./fuse-test/Dockerfile .
```

To create the container and start an interactive shell:

```shell
docker run -it --rm --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined fuse-test bash
```

To mount the FUSE file system in the container:

```shell
mkdir ./mnt
./fuse-mount ./mnt &
```

To run `fstest` with some patches applied to support `acid-store`:

```shell
cd ./fstest/fstest
make
cd ../../mnt
prove -r ../fstest/fstest
```

## Documentation

When building the documentation normally, markers which identify which features
are required to use various parts of the library will be missing. That is
because this is an [unstable
feature](https://github.com/rust-lang/rust/issues/43781) of rustdoc that happens
to be enabled in docs.rs. To build the documentation correctly, run the
following command:

```shell
RUSTDOCFLAGS='--cfg docsrs' cargo +nightly doc --all-features
```
