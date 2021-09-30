{
  description = "Development environment";

  inputs.nixpkgs.url = github:NixOS/nixpkgs/nixpkgs-unstable;
  inputs.flake-utils.url = github:numtide/flake-utils;

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system: {
      devShell = import ./nix/shell.nix {
        pkgs = nixpkgs.legacyPackages.${system};
      };
    });
}
