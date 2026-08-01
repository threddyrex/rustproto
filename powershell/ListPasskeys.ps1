param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [string]$dataDir = $null,
    [bool]$logToDataDir = $false
)

. .\_Defaults.ps1

& $exePath /command ListPasskeys /dataDir $dataDir /logLevel $logLevel /logtodatadir $logToDataDir
