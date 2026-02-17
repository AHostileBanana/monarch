{
  # modified from original https://github.com/dtgoitia/nix-python
  # usage:
  #
  # cd <this directory>
  # nix develop
  # # optional, only if not yet created
  # python -m venv .venv
  # . .venv/bin/activate
  # pip install -r requirements.txt
  #
  # # to save new requirements
  # pip freeze > requirements.txt
  #
  description = "Python development environment";

  inputs = {
    #nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";    
    nixpkgs-python.url = "github:cachix/nixpkgs-python";
  };

  outputs = { self, nixpkgs, nixpkgs-python }: 
    let
      system = "x86_64-linux";

      pythonVersion = "3.10.1";

      pkgs = import nixpkgs { inherit system; };
      myPython = nixpkgs-python.packages.${system}.${pythonVersion};
    in
    {
      devShells.${system}.default = pkgs.mkShell {        
        packages = [
          myPython
        ];
        # shellHook = ''
        #   python --version
        #   exec fish
        # '';
      };
    };
}
