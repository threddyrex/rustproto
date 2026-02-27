param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [string]$dataDir = $null,
    [bool]$logToDataDir = $false,
    [bool]$deleteExistingDb = $false
)

. .\_Defaults.ps1


& $exePath /command InstallDb /dataDir $dataDir /logLevel $logLevel /logToDataDir $logToDataDir /deleteExistingDb $deleteExistingDb
