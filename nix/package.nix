{ lib, buildGoModule }:

buildGoModule {
  pname = "cachefs";
  version = "0.1.0";

  src = ../.;

  # This will be replaced with the actual hash computed by Nix
  vendorHash = "sha256-Ww2+Z/ayzK3oFoqDVvcpaXdV5x/xIpqkSQMLy1Cfiek=";

  meta = with lib; {
    description = "A caching filesystem using FUSE";
    license = licenses.mit;
    platforms = platforms.linux;
  };
}
