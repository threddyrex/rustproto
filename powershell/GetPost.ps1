param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Mandatory=$true, Position = 0)]
    [string]$uri
)

. .\_Defaults.ps1


# call rustproto.exe to get post
& $exePath /command GetPost /uri $uri /logLevel $logLevel /logToDataDir $logToDataDir /dataDir $dataDir
