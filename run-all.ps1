param(
    [Parameter(Mandatory=$true)]
    [string]$StartDay,
    
    [Parameter(Mandatory=$true)]
    [string]$EndDay
)
$strategies = @('s1_sp500',
                's2_mrlong',
                's3_mrshort',
                's21_longmom',
                's22_short_rsi_thrust', 
                's23_long_mr',
                's24_low_vol_long',
                's25_adx_mr_long',
                's26_6day_surge_short',
                's31_trend_50',
                's32_200_cross')
for ($i = 0; $i -lt $strategies.Length; $i++) {
    Write-Host "Attempting to run " 
    python .\zipbird\runner.py $strategies[$i] -s $StartDay -e $EndDay -b norgatedata-sp500 -d 0  > logs/s-$strategies[$i]
}

#zipline ingest -b norgatedata-sp500 ; zipline ingest -b norgatedata-all-us
# python .\zipbird\runner.py s1_sp500 -s $StartDay -e $EndDay -b norgatedata-sp500 -d 0  > logs/s1
# python .\zipbird\runner.py s2_mrlong -s $StartDay -e $EndDay -b norgatedata-all-us -d 0  > logs/s2
# python .\zipbird\runner.py s3_mrshort -s $StartDay -e $EndDay -b norgatedata-all-us -d 0  > logs/s3
# python .\zipbird\runner.py s21_longmom -s $StartDay -e $EndDay -b norgatedata-all-us -d 0  > logs/s21
# python .\zipbird\runner.py s22_short_rsi_thrust -s $StartDay -e $EndDay -b norgatedata-all-us -d 0  > logs/s22
# python .\zipbird\runner.py s23_long_mr -s $StartDay -e $EndDay -b norgatedata-all-us -d 0  > logs/s23
# python .\zipbird\runner.py s24_low_vol_long -s $StartDay -e $EndDay -b norgatedata-all-us -d 0  > logs/s24
# python .\zipbird\runner.py s25_adx_mr_long -s $StartDay -e $EndDay -b norgatedata-all-us -d 0  > logs/s25
# python .\zipbird\runner.py s26_6day_surge_short -s $StartDay -e $EndDay -b norgatedata-all-us -d 0  > logs/s26
# python .\zipbird\runner.py s31_trend_50 -s $StartDay -e $EndDay -b norgatedata-sp500 -d 0  > logs/s31
# python .\zipbird\runner.py s32_200_cross -s $StartDay -e $EndDay -b norgatedata-sp500 -d 0  > logs/s32

 python.exe .\zipbird\replay_runner.py -s $StartDay -e 1995-12-31 -b norgatedata-all-us `
 --replay_strategies s1_sp500 s22_short_rsi_thrust s23_long_mr s24_low_vol_long s25_adx_mr_long s26_6day_surge_short `
 --replay_weights 0.25 0.5 0.25 0.25 0.25 0.5 -d 1 > logs/r1

#  python.exe .\zipbird\replay_runner.py -s $StartDay -e $EndDay -b norgatedata-all-us `
#  --replay_strategies s1_sp500 `
#  --replay_weights 1 -d 1 > logs/r1

#  python.exe .\zipbird\replay_runner.py -s $StartDay -e $EndDay -b norgatedata-sp500 `
#  --replay_strategies s1_sp500 s1_sp500_m `
#  --replay_weights 0.5 0.5 -d 1 -l rotation > logs/r1-rotation