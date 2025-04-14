# Set the ticker and date range
$tickerSymbol = "AAPL"
$startDate = "2024-01-01"
$endDate   = "2024-12-31"

$AmiBroker = New-Object -ComObject Broker.Application
$Version = $AmiBroker.GetType()
Write-Host "AmiBroker Version: $($Version)"

Write-Host $AmiBroker.ActiveWindow
Write-Host $AmiBroker.ActiveDocument

$AmiBroker.ActiveWindow.ZoomToRange($startDate, $endDate)
