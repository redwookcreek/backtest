 python .\zipbird\runner.py s1_sp500 -s 1995-01-01 -e 2023-12-31 -b norgatedata-sp500 -d 0  > logs/s1
 python .\zipbird\runner.py s2_mrlong -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -d 0  > logs/s2
 python .\zipbird\runner.py s3_mrshort -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -d 0  > logs/s3
 python .\zipbird\runner.py s21_longmom -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -d 0  > logs/s21
 python .\zipbird\runner.py s22_short_rsi_thrust -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -d 0  > logs/s22
 python .\zipbird\runner.py s23_long_mr -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -d 0  > logs/s23
 python .\zipbird\runner.py s24_low_vol_long -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -d 0  > logs/s24
 python .\zipbird\runner.py s25_adx_mr_long -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -d 0  > logs/s25
 python .\zipbird\runner.py s26_6day_surge_short -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us -d 0  > logs/s26

#  python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 1995-12-31 -b norgatedata-all-us `
#  --replay_strategies s1_sp500 s22_short_rsi_thrust s23_long_mr s24_low_vol_long s25_adx_mr_long s26_6day_surge_short `
#  --replay_weights 0.25 0.5 0.25 0.25 0.25 0.5 -d 1 > logs/r1

 python.exe .\zipbird\replay_runner.py -s 1995-01-01 -e 2023-12-31 -b norgatedata-all-us `
 --replay_strategies s1_sp500 `
 --replay_weights 1 -d 1 > logs/r1