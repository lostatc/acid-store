# Contributing
## Tests

Some tests are not run in CI because they rely on outside resources. These tests must be run locally and be configured
with environment variables. Below is a table of what those environment variables are and what Cargo features they are
associated with. The variables only need to be set if their corresponding Cargo features are enabled when running the
test suite.

| Variable | Description | Feature |
| --- | --- | --- |
| `REDIS_URL` | The `redis://` URL of the Redis server to test against. | `store-redis` |
| `S3_BUCKET` | The name of the S3 bucket to test against. | `store-s3` |
| `S3_REGION` | The name of the AWS region containing the S3 bucket. | `store-s3` |
| `S3_ACCESS_KEY` | The access key for accessing the S3 bucket. | `store-s3` |
| `S3_SECRET_KEY` | The secret key for accessing the S3 bucket. | `store-s3` |
| `RCLONE_REMOTE` | The `<remote>:<path>` string for the rclone remote to test against. | `store-rclone` |
| `SFTP_SERVER` | The URL of the SFTP server to test against. | `store-sftp` |
| `SFTP_PATH` | The path to use on the SFTP server. | `store-sftp` |
| `SFTP_USERNAME` | The username to access the SFTP server. | `store-sftp` |
| `SFTP_PASSWORD` | The password to access the SFTP server. | `store-sftp` |

## Documentation

When building the documentation normally, markers which identify which features are required to use various parts of the
library will be missing. That is because this is an [unstable feature](https://github.com/rust-lang/rust/issues/43781)
of rustdoc. To build the documentation correctly, run the following command:

```
RUSTDOCFLAGS='--cfg docsrs' cargo +nightly doc --all-features
```

