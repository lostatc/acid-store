#!/usr/bin/env bash

# Mount the FUSE file system.
mkdir ./mnt
./fuse-mount ./mnt &

# Build fstest.
cd ./fstest/fstest
make

# Run fstest in the FUSE file system.
cd ../../mnt
prove -r ../fstest/fstest