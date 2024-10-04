# Setup
This virtual env is build for a locally modified zipline-reloaded
To start virtual env:
```shell
C:\Users\liu_w\AppData\Local\cache\zipline-reloaded\Scripts\activate.ps1
```

The package was installed from local package with command like
```shell
pip install -e /path/to/your/local/package
```

# Run

Before running any script, add local path to python path
Under powershell
```shell
$env:PYTHONPATH='C:\Users\liu_w\OneDrive\Documents\zipline\backtest\zipbird;'

OR
$env:PYTHONPATH='C:\Users\liu_w\OneDrive\Documents\zipline\backtest\zipbird;' + env:$PYTHONPATH
```
Under cmd
```shell
set PYTHONPATH="C:\Users\liu_w\OneDrive\Documents\zipline\backtest\zipbird;%PYTHONPATH%"
```

# Tests
To run tests, run the following under project root
```shell
python -m unittest discover
```
