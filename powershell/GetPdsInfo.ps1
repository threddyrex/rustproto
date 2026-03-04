param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Position = 0)]
    [string]$actor = $null
)

. .\_Defaults.ps1


# call rustproto.exe to get PDS info
& $exePath /command GetPdsInfo /actor $actor /logLevel $logLevel /logToDataDir $logToDataDir /dataDir $dataDir
