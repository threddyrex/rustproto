param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Mandatory=$true, Position = 0)]
    [string]$actor,
    [Parameter(Mandatory=$true, Position = 1)]
    [string]$blobCid
)

. .\_Defaults.ps1


# call rustproto.exe to download a blob for the given actor and CID
& $exePath /command GetBlob /actor $actor /blobCid $blobCid /logLevel $logLevel /logToDataDir $logToDataDir /dataDir $dataDir
