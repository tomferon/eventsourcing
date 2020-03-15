{ nixpkgs }:

let
  inherit (nixpkgs) pkgs;

  ghcVersion = "ghc883";

  hsPkgs = pkgs.haskell.packages.${ghcVersion};

  packages = {
    inherit (hsPkgs)
      bytestring
      deepseq
      exceptions
      free
      hashable
      hedgehog
      lens
      mtl
      pipes
      postgresql-simple
      psqueues
      resource-pool
      stm
      tasty
      tasty-hedgehog
      text
      time
      unordered-containers;
  };

in
{
  inherit packages;
  ghcWithPackages = hsPkgs.ghcWithPackages (_: builtins.attrValues packages);
}
