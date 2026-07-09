{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.cachefs;
in {
  options.services.cachefs = {
    enable = mkEnableOption "cachefs FUSE filesystem mount";

    package = mkOption {
      type = types.package;
      default = pkgs.callPackage ./package.nix {};
      description = "The cachefs package to use.";
    };

    backendDir = mkOption {
      type = types.str;
      default = "${config.home.homeDirectory}/.cache/cachefs-backend";
      description = "Path to the backend storage directory.";
    };

    mountPoint = mkOption {
      type = types.str;
      default = "${config.home.homeDirectory}/cachefs";
      description = "Path to the mount point directory.";
    };

    extraArgs = mkOption {
      type = types.listOf types.str;
      default = [];
      description = "Extra command line arguments to pass to cachefs.";
    };
  };

  config = mkIf cfg.enable {
    systemd.user.services.cachefs = {
      Unit = {
        Description = "cachefs FUSE mount service";
        After = [ "local-fs.target" ];
      };

      Service = {
        # Run in foreground so systemd monitors the mount process and redirects logs to journald
        ExecStart = "${cfg.package}/bin/cachefs -fg ${escapeShellArgs cfg.extraArgs} ${cfg.backendDir} ${cfg.mountPoint}";

        # Ensure the backend and mount point directories exist before starting
        ExecStartPre = "${pkgs.coreutils}/bin/mkdir -p ${cfg.backendDir} ${cfg.mountPoint}";

        # Safety unmount if the process fails or exits cleanly but mount stays active.
        # Prefixing with '-' tells systemd to ignore errors if it was already unmounted.
        ExecStopPost = "-/run/wrappers/bin/fusermount -u ${cfg.mountPoint}";

        Restart = "on-failure";
        RestartSec = "5s";
      };

      Install = {
        WantedBy = [ "default.target" ];
      };
    };
  };
}
