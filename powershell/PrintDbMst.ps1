param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [string]$dataDir = $null,
    [bool]$logToDataDir = $false,
    [string]$format = "tree"
)

. .\_Defaults.ps1

& $exePath /command PrintDbMst /dataDir $dataDir /logLevel $logLevel /format $format /logtodatadir $logToDataDir
