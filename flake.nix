{
  description = "Dev shell";

  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url  = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        rust = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rustfmt" "rust-analyzer" ];
        };
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        darwinDeps = if builtins.match ".*-darwin" system != null then with pkgs; [
          darwin.apple_sdk.frameworks.SystemConfiguration
        ] else [];
      in
      with pkgs;
      {
        devShells.default = mkShell {
          buildInputs = [
            pkg-config
            rust
          ] ++ darwinDeps;
        };
      }
    );
}
