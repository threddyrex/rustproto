param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [string]$dataDir = $null,
    [string]$text = "Hello from TestApplyWritesAndLogFirehose",
    [bool]$logToDataDir = $true
)

. .\_Defaults.ps1

& $exePath /command TestApplyWritesAndLogFirehose /dataDir $dataDir /logLevel $logLevel /text $text /logToDataDir $logToDataDir
