let
  mozilla_overlay = import (builtins.fetchTarball https://github.com/mozilla/nixpkgs-mozilla/archive/master.tar.gz);
  nixpkgs = import <nixpkgs> { overlays = [ mozilla_overlay ]; };
in
  with nixpkgs; [
    latest.rustChannels.stable.rust
    binutils
    gcc
    gnumake
    openssl
    pkgconfig
  ]
