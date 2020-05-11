with import <nixpkgs> {};

let
  inputs = import ./inputs.nix;
in
  mkShell {
    buildInputs = inputs;

    # This directory is not on tmpfs on NixOS. This is important for benchmarking.
    TMPDIR = "/tmp";
  }
