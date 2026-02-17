To get started( consider # comments for shell commands )
```
   # this will get you python3 in a sandbox
   nix develop

   # this will get you pip and further sandbox you in a venv
   python -m venv .venv
   . .venv/bin/activate

   # this will get you dev tools
   pip install -r dev_requirements.txt

   # this will get you runtime dependencies
   pip install -r requirements.txt

   # TODO consider whether I can monkeypatch this URL at runtime instead of patching source at build-time.
   # monarchmoney must be built from source, there's a recent URL change not yet in that codebase.
   # also it had other differences between source and the pip-available package.
   #
   # download it, patch it, build it from source.
   mkdir tmp
   cd tmp
   git clone https://github.com/hammem/monarchmoney.git
   cd monarchmoney/monarchmoney
   patch monarchmoney.py < ../../../monarchmoney.py.patch
   # return to monarchmoney base dir to build and install
   cd ../
   python setup.py install

   cd ../../
   # get rid of the modified source dir
   rm -rf ./tmp

   # this will run the tests
   pytest

   # this will run the tool 
   python monarch.py --username "$MONARCH_EMAIL" --password "$MONARCH_PASSWORD" --token "$MONARCH_TOKEN"

```