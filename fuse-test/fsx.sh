#!/usr/bin/env bash

# Mount the FUSE file system.
mkdir ./mnt
./fuse-mount ./mnt &

# Create an empty file to test against in the FUSE file system.
touch ./mnt/test-file

# Build fsx.
cd ./fstools/src/fsx
make

# Run fsx.
cd ../../../mnt
../fstools/src/fsx/fsx "$@"