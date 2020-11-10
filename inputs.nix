let
  mozilla_overlay = import (builtins.fetchTarball https://github.com/mozilla/nixpkgs-mozilla/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [ mozilla_overlay ]; };
  rust_stable = (nixpkgs.latest.rustChannels.stable.rust.override { extensions = [ "rust-src" ]; });
in
  with nixpkgs; [
    rust_stable
    acl
    binutils
    gcc
    gnumake
    openssl
    pkgconfig
  ]
