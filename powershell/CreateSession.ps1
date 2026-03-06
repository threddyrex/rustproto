param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Position = 0)]
    [string]$actor = $null,
    [string]$password = "",
    [string]$authFactorToken = ""
)

. .\_Defaults.ps1


$command = "/command CreateSession /dataDir $dataDir /logLevel $logLevel /logToDataDir $logToDataDir /actor $actor"

if(-not [string]::IsNullOrWhiteSpace($password))
{
    $command += " /password $password"
}

if(-not [string]::IsNullOrWhiteSpace($authFactorToken))
{
    $command += " /authFactorToken $authFactorToken"
}

& $exePath $command.Split(' ')
