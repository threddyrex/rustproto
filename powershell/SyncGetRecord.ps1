param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Mandatory=$true, Position = 0)]
    [string]$actor,
    [Parameter(Mandatory=$true, Position = 1)]
    [string]$collection,
    [Parameter(Mandatory=$true, Position = 2)]
    [string]$rkey
)

. .\_Defaults.ps1


& $exePath /command SyncGetRecord /actor $actor /collection $collection /rkey $rkey /logLevel $logLevel /logToDataDir $logToDataDir /dataDir $dataDir
