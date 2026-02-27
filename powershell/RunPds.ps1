param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [string]$dataDir = $null,
    [bool]$logToDataDir = $false
)

. .\_Defaults.ps1


& $exePath /command RunPds /dataDir $dataDir /logLevel $logLevel /logToDataDir $logToDataDir
