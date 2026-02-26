param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Position = 0)]
    [string]$actor = $null,
    [string]$repoFile = $null,
    [string]$collection = $null,
    [string]$month = $null
)

. .\_Defaults.ps1

$command = "/command PrintRepoRecords /dataDir $dataDir /logLevel $logLevel /logToDataDir $logToDataDir"

if(-not [string]::IsNullOrWhiteSpace($collection))
{
    $command += " /collection $collection"
}

if(-not [string]::IsNullOrWhiteSpace($month))
{
    $command += " /month $month"
}

if(-not [string]::IsNullOrWhiteSpace($repoFile))
{
    $command += " /repoFile $repoFile"
}
elseif(-not [string]::IsNullOrWhiteSpace($actor))
{
    $command += " /actor $actor"
}
else
{
    Write-Host "Usage: .\PrintRepoRecords.ps1 [-actor <handle_or_did>] [-repoFile <path>] [-collection <type>] [-month <yyyy-MM>]"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\PrintRepoRecords.ps1 threddyrex.org"
    Write-Host "  .\PrintRepoRecords.ps1 threddyrex.org -collection app.bsky.feed.post"
    Write-Host "  .\PrintRepoRecords.ps1 -repoFile ..\data\repos\did_web_threddyrex_org.car -month 2026-01"
    exit
}

# Use Invoke-Expression to run the command string
Invoke-Expression "& `"$exePath`" $command"
