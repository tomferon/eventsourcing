let
  sources = import ./sources.nix;
  nixpkgs = import sources.nixpkgs {};
  inherit (nixpkgs) pkgs lib;

  haskellCustom = import ./haskell.nix { inherit nixpkgs; };
  ghcCustom = haskellCustom.ghcWithPackages;

in
pkgs.mkShell {
  buildInputs = with pkgs; [
    cabal-install
    ghcCustom
  ];
}
