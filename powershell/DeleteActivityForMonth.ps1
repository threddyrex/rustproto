param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Position = 0)]
    [string]$actor = $null,
    [Parameter(Mandatory=$true)]
    [string]$month,
    [bool]$deleteLikes = $false,
    [bool]$deleteReposts = $false,
    [bool]$deletePosts = $false,
    [bool]$deleteBlocks = $false,
    [ValidateRange(0, [long]::MaxValue)]
    [long]$sleepSeconds = 2
)

. "$PSScriptRoot\_Defaults.ps1"

& $exePath /command DeleteActivityForMonth /actor $actor /logLevel $logLevel /logToDataDir $logToDataDir /month $month /dataDir $dataDir /deleteLikes $deleteLikes /deleteReposts $deleteReposts /deletePosts $deletePosts /deleteBlocks $deleteBlocks /sleepSeconds $sleepSeconds
