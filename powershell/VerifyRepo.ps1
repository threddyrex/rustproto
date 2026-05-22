param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Position = 0)]
    [string]$actor = $null,
    [string]$repoFile = $null,
    [string]$did = $null
)

. .\_Defaults.ps1


if(-not [string]::IsNullOrWhiteSpace($repoFile))
{
    & $exePath /command VerifyRepo /dataDir $dataDir /logLevel $logLevel /logToDataDir $logToDataDir /repoFile $repoFile /did $did
}
elseif(-not [string]::IsNullOrWhiteSpace($actor))
{
    & $exePath /command VerifyRepo /dataDir $dataDir /logLevel $logLevel /logToDataDir $logToDataDir /actor $actor /did $did
}
else
{
    Write-Host "Usage: .\VerifyRepo.ps1 [-actor <handle_or_did>] [-repoFile <path>] [-did <expected_did>]"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\VerifyRepo.ps1 threddyrex.org"
    Write-Host "  .\VerifyRepo.ps1 -repoFile ..\data\repos\did_web_threddyrex_org.car"
}
