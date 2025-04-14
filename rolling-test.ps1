
$start_date = @('1995-01-01',
                '1996-01-01',
                '1997-01-01',
                '1998-01-01',
                '1999-01-01', 
                '2000-01-01',
                '2001-01-01',
                '2002-01-01',
                '2003-01-01',
                '2004-01-01',
                '2005-01-01',
                '2006-01-01',
                '2007-01-01',
                '2008-01-01',
                '2009-01-01',
                '2010-01-01',
                '2011-01-01',
                '2012-01-01',
                '2013-01-01',
                '2014-01-01'
                )
for ($i = 0; $i -lt $start_date.Length; $i++) {
    $start = $start_date[$i]  # Store the current strategy in a variable
    Write-Host "Attempting to run start $start" 
    python .\zipbird\runner.py s34_mom -s $start -e 2024-12-31 -b norgatedata-all-us -l bg-100high-$start -d 0 > logs/s34-bg-100high-$start
}
