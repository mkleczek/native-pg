{ withHaskellTools ? true }:
let
  nixpkgs = fetchTarball {
    url = "https://github.com/nixos/nixpkgs/archive/35d3407a3816f3b341d8cf1d60abaf2b7b8166ac.tar.gz";
    sha256 = "sha256-BoYPdqk6jlKXy+DyUzyGV/CtRGfAhk2MmIgBhsemTGI=";
  };
  pkgs = import nixpkgs { };
  # nixpkgs' default GHC, which its binary cache has
  # Cabal supplies skeletest and its preprocessor; this pin has an older version.
  ghc = pkgs.haskellPackages.ghcWithPackages (
    p: with p; [
      async
      base64-bytestring
      crypton
      memory
      network
      postgresql-binary
      scientific
      uuid
    ]
  );
in
# An external GHC must retain its system compiler and SDK environment.
(if withHaskellTools then pkgs.mkShell else pkgs.mkShellNoCC) {
  packages = [
    pkgs.pkg-config
    pkgs.postgresql
    pkgs.rustls-ffi
  ] ++ pkgs.lib.optionals withHaskellTools [
    ghc
    pkgs.cabal-install
  ];
}
