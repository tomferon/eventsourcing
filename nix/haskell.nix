{ nixpkgs }:

let
  inherit (nixpkgs) pkgs;

  ghcVersion = "ghc865";

  hsPkgs = pkgs.haskell.packages.${ghcVersion}.override {
    overrides = self: super: {
      hedgehog       = self.callHackage "hedgehog" "1.0" {};
      tasty-hedgehog = self.callHackage "tasty-hedgehog" "1.0.0.1" {};
    };
  };

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
