param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [Parameter(Position = 0)]
    [string]$sourceDataDir = $null,
    [Parameter(Position = 1)]
    [string]$destDataDir = $null
)

. .\_Defaults.ps1


& $exePath /command SyncRepo /sourceDataDir $sourceDataDir /destDataDir $destDataDir /logLevel $logLevel /logToDataDir $logToDataDir
