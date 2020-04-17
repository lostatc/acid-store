with import <nixpkgs> {};

let
  inputs = import ./inputs.nix;
in
  buildEnv {
    name = "rust-env";
    paths = inputs;
  }
