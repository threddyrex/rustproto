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
    & $exePath /command WalkMst /dataDir $dataDir /logLevel $logLevel /repoFile $repoFile
}
elseif(-not [string]::IsNullOrWhiteSpace($actor))
{
    & $exePath /command WalkMst /dataDir $dataDir /logLevel $logLevel /actor $actor
}
else
{
    Write-Host "Usage: .\WalkMst.ps1 -actor <handle_or_did>"
    Write-Host "   or: .\WalkMst.ps1 -repoFile <path>"
}
