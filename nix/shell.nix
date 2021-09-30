{ pkgs }:

let
  inherit (pkgs) lib;

  haskellCustom = import ./haskell.nix { inherit pkgs; };
  ghcCustom = haskellCustom.ghcWithPackages;

in
pkgs.mkShell {
  buildInputs = with pkgs; [
    cabal-install
    ghcCustom
  ];
}
