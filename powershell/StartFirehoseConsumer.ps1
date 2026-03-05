param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Position = 0)]
    [string]$actor = $null,
    [string]$cursor = $null,
    [bool]$showDagCborTypes = $false
)

. .\_Defaults.ps1

if($cursor -ne $null -and $cursor -ne "")
{
    & $exePath /command StartFirehoseConsumer /actor $actor /logLevel $logLevel /logToDataDir $logToDataDir /dataDir $dataDir /cursor $cursor /showDagCborTypes $showDagCborTypes
}
else
{
    & $exePath /command StartFirehoseConsumer /actor $actor /logLevel $logLevel /logToDataDir $logToDataDir /dataDir $dataDir /showDagCborTypes $showDagCborTypes
}
