param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [string]$dataDir = $null,
    [Parameter(Position = 0)]
    [string]$actor = $null,
    [string]$repoFile = $null
)

. .\_Defaults.ps1


if(-not [string]::IsNullOrWhiteSpace($repoFile))
{
    & $exePath /command PrintRepoStats /dataDir $dataDir /logLevel $logLevel /repoFile $repoFile
}
elseif(-not [string]::IsNullOrWhiteSpace($actor))
{
    & $exePath /command PrintRepoStats /dataDir $dataDir /logLevel $logLevel /actor $actor 
}
else
{
    Write-Host "Usage: .\PrintRepoStats.ps1 [-actor <handle_or_did>] [-repoFile <path>]"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\PrintRepoStats.ps1 threddyrex.org"
    Write-Host "  .\PrintRepoStats.ps1 -repoFile ..\data\repos\did_web_threddyrex_org.car"
}
