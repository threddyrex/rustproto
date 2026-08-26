param (
    [string]$exePath = $null,
    [string]$logLevel = $null,
    [bool]$logToDataDir = $false,
    [string]$dataDir = $null,
    [Parameter(Mandatory=$true, Position = 0)]
    [string]$schema
)

. .\_Defaults.ps1


# call rustproto.exe to resolve a lexicon schema by NSID
& $exePath /command GetLexiconSchema /schema $schema /logLevel $logLevel /logToDataDir $logToDataDir /dataDir $dataDir
