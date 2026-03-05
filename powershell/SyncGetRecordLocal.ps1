param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [string]$format = "dagcbor",
    [Parameter(Mandatory=$true, Position = 0)]
    [string]$collection,
    [Parameter(Mandatory=$true, Position = 1)]
    [string]$rkey
)

. .\_Defaults.ps1


& $exePath /command SyncGetRecordLocal /collection $collection /rkey $rkey /format $format /logLevel $logLevel /logToDataDir $logToDataDir /dataDir $dataDir
