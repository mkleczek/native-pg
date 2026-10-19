let
  nixpkgs = fetchTarball {
    url = "https://github.com/nixos/nixpkgs/archive/35d3407a3816f3b341d8cf1d60abaf2b7b8166ac.tar.gz";
    sha256 = "sha256-BoYPdqk6jlKXy+DyUzyGV/CtRGfAhk2MmIgBhsemTGI=";
  };
  pkgs = import nixpkgs { };
  # nixpkgs' default GHC, which its binary cache has
  ghc = pkgs.haskellPackages.ghcWithPackages (
    p: with p; [
      async
      base64-bytestring
      crypton
      hspec
      memory
      network
      postgresql-binary
      scientific
      uuid
    ]
  );
in
pkgs.mkShell {
  packages = [
    ghc
    pkgs.cabal-install
    pkgs.pkg-config
    pkgs.postgresql
    pkgs.rustls-ffi
  ];
}
