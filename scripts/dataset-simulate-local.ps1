param(
    [Parameter(Mandatory=$true)][string]$AppRoot,
    [Parameter(Mandatory=$true)][ValidatePattern('^[a-zA-Z0-9-]{1,48}$')][string]$RunName,
    [ValidateRange(0, 2147483647)][int]$Seed = 42,
    [string]$Photo
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
if ($Photo) { $Photo = (Resolve-Path -LiteralPath $Photo).Path }
$names = @('YAP_DATASET_DSN','YAP_SUPABASE_LOCAL_URL','YAP_SUPABASE_LOCAL_ANON_KEY',
    'YAP_SUPABASE_LOCAL_SERVICE_KEY','YAP_SUPABASE_LOCAL_ADMIN_DSN','GOCACHE','GOMODCACHE')
$previous = @{}
foreach ($name in $names) { $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
$role = 'dataset_sim_' + [guid]::NewGuid().ToString('N')
$secret = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
$created = $false
Push-Location $AppRoot
try {
    $settings = & ./node_modules/.bin/supabase.cmd status -o json 2>$null | ConvertFrom-Json
    $db = [Uri]$settings.DB_URL
    if ($LASTEXITCODE -ne 0 -or $settings.API_URL -ne 'http://127.0.0.1:54321' -or
        $db.Host -ne '127.0.0.1' -or $db.Port -ne 54322 -or $db.AbsolutePath -ne '/postgres') {
        throw 'A running local YapChat Supabase is required.'
    }
    "create role $role login inherit password '$secret'; grant yap_recommendation_exporter to $role;" |
        docker exec -i supabase_db_Yap_chat psql -X -q -v ON_ERROR_STOP=1 --single-transaction -U postgres -d postgres
    if ($LASTEXITCODE -ne 0) { throw 'Cannot provision a temporary dataset login; apply the canonical migration.' }
    $created = $true
    $env:YAP_DATASET_DSN = "postgres://${role}:${secret}@127.0.0.1:54322/postgres?sslmode=disable"
    $env:YAP_SUPABASE_LOCAL_URL = $settings.API_URL
    $env:YAP_SUPABASE_LOCAL_ANON_KEY = $settings.ANON_KEY
    $env:YAP_SUPABASE_LOCAL_SERVICE_KEY = $settings.SERVICE_ROLE_KEY
    $env:YAP_SUPABASE_LOCAL_ADMIN_DSN = $settings.DB_URL
    $env:GOCACHE = Join-Path $root '.cache/go-build'
    $env:GOMODCACHE = Join-Path $root '.cache/gomod'
    Set-Location $root
    $parent = Join-Path $root '.local/output'
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
    $output = Join-Path $parent $RunName
    $argsForGo = @('run','./cmd/dataset-simulate','-output',$output,'-seed',"$Seed")
    if ($Photo) { $argsForGo += @('-photo',$Photo) }
    & go @argsForGo
    if ($LASTEXITCODE -ne 0) { throw 'Local dataset simulator failed.' }
    Write-Host "Capture configuration: $output/build.json"
} finally {
    try {
        if ($created) {
            "drop role $role;" | docker exec -i supabase_db_Yap_chat psql -X -q -v ON_ERROR_STOP=1 -U postgres -d postgres
            if ($LASTEXITCODE -ne 0) { throw "Temporary login cleanup failed: $role" }
        }
    } finally {
        foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
        Pop-Location
    }
}
