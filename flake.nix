{
    description = "dbc";

    inputs.nixpkgs = {
        url = github:nixos/nixpkgs/nixos-21.11;
    };

    inputs.flake-utils = {
        url = github:numtide/flake-utils;
        inputs.nixpkgs.follows = "nixpkgs";
    };

    outputs = { self, nixpkgs, flake-utils, ... }:
      flake-utils.lib.eachDefaultSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };
          nodeDependencies = (pkgs.callPackage ./default.nix {}).shell.nodeDependencies;
          pname = "kocchi-website";
          nodejs = pkgs.nodejs-16_x;

          nativeBuildInputs = with pkgs; [
            nodejs-16_x
            nodePackages.yarn
            nodePackages.svelte-language-server
            nodePackages.typescript-language-server
            nixfmt
            git-crypt
            makeWrapper
          ];

          yarnPkg = pkgs.mkYarnPackage rec {
            inherit pname nativeBuildInputs;
            src = ./.;
            packageJSON = ./package.json;
            yarnLock = ./yarn.lock;

            buildPhase = ''
              yarn --offline --build-from-source svelte-kit build
            '';

            doCheck = false;

            postInstall = ''
              makeWrapper '${nodejs}/bin/node' "$out/bin/${pname}" \
                --add-flags "$out/${passthru.nodeAppDir}/build/index.js"
            '';

            distPhase = ''
              true
            '';

            passthru = {
                nodeAppDir = "libexec/${pname}/deps/${pname}";
            };
          };
        in rec
        {

            defaultPackage = yarnPkg;
            packages.${system}.${pname} = yarnPkg;

            defaultApp = {
                drv = self.defaultPackage;
            };

            apps.${pname} = defaultApp;

            devShell = pkgs.mkShell {
                inherit nativeBuildInputs;

                postInstall = ''
                  just build
                '';

                distPhase = "true";
            };
        }
    );
}
