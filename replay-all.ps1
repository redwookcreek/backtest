 
 Write-Host 'replay s1'
 python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s1 `
 --replay_strategies s1_sp500 `
 --replay_weights 1 -d 1 > logs/r1
 
 Write-Host 'replay s2'
  python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s2 `
 --replay_strategies s2_mrlong `
 --replay_weights 1 -d 1 > logs/r2
 
 Write-Host 'replay s3'
  python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s3 `
 --replay_strategies s3_mrshort `
 --replay_weights 1 -d 1 > logs/r3
 
 Write-Host 'replay s21'
  python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s21 `
 --replay_strategies s21_longmom `
 --replay_weights 1 -d 1 > logs/r21
 
 Write-Host 'replay s22'
  python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s22 `
 --replay_strategies s22_short_rsi_thrust `
 --replay_weights 1 -d 1 > logs/r22
 
 Write-Host 'replay s23'
  python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s23 `
 --replay_strategies s23_long_mr `
 --replay_weights 1 -d 1 > logs/r23

 Write-Host 'replay s24'
  python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s24 `
 --replay_strategies s24_low_vol `
 --replay_weights 1 -d 1 > logs/r24
 
 Write-Host 'replay s25'
 python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s25 `
 --replay_strategies s25_adx_mr_long `
 --replay_weights 1 -d 1 > logs/r25

 Write-Host 'replay s26'
  python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -l s26 `
 --replay_strategies s26_6day_surge_short `
 --replay_weights 1 -d 1 > logs/r26