{
  description = "A caching filesystem using FUSE";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages.cachefs = pkgs.callPackage ./nix/package.nix {};
        packages.default = self.packages.${system}.cachefs;

        apps.cachefs = flake-utils.lib.mkApp {
          drv = self.packages.${system}.cachefs;
        };
        apps.default = self.apps.${system}.cachefs;

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
          ];
        };
      }
    ) // {
      homeManagerModules.cachefs = import ./nix/hm-module.nix;
      homeManagerModules.default = self.homeManagerModules.cachefs;
    };
}
