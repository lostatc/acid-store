with import <nixpkgs> {};

let
  inputs = import ./inputs.nix;
in
  mkShell {
    buildInputs = inputs;
    TMPDIR = "/tmp";
  }
