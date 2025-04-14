# & 'C:\Program Files\Norgate Data Updater\bin\ndu.trigger.exe' UPDATE WAIT
# Set-Location C:\Users\liu_w\OneDrive\Documents\zipline\backtest
.\start-env.ps1
zipline clean -b norgatedata-all-us -k 1
zipline ingest -b norgatedata-all-us
zipline clean -b norgatedata-sp500 -k 1
zipline ingest -b norgatedata-sp500
zipline clean -b norgatedata-spy -k 1
zipline ingest -b norgatedata-spy
# zipline clean -b norgatedata-debug -k 1
# zipline ingest -b norgatedata-debug