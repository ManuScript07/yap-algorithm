param(
    [Parameter(Mandatory = $true)][ValidateSet('setup', 'worker')][string]$Action,
    [string]$AppRoot,
    [ValidateRange(1, 8)][int]$Concurrency = 2,
    [switch]$Once,
    [switch]$RetryFailed
)
$ErrorActionPreference = 'Stop'
$serviceRoot = Split-Path $PSScriptRoot -Parent
$configPath = Join-Path $serviceRoot '.local/supabase-worker.json'

if ($Action -eq 'setup') {
    if (!$AppRoot) { throw 'AppRoot is required for setup.' }
    $cli = Join-Path $AppRoot 'node_modules/.bin/supabase.cmd'
    Push-Location $AppRoot
    try {
        $settings = & $cli status -o json 2>$null | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw 'Local Supabase is not running.' }
        $database = [Uri]$settings.DB_URL
        if ($database.Host -ne '127.0.0.1' -or $database.Port -ne 54322 -or $database.AbsolutePath -ne '/postgres') {
            throw 'Setup only accepts the local YapChat database on port 54322.'
        }
        $random = [byte[]]::new(32)
        $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
        try { $generator.GetBytes($random) } finally { $generator.Dispose() }
        $password = -join ($random | ForEach-Object { $_.ToString('x2') })
        # The generated password contains hex only and is passed on stdin. It
        # never appears in command arguments, console output or version control.
        $sql = @"
begin;
do `$`$ begin
  if to_regprocedure('recsys.pipeline_contract()') is null then
    raise exception 'Apply the canonical YapChat feed migration first';
  end if;
  if not exists(select 1 from pg_roles where rolname='yap_recsys_local_worker') then
    create role yap_recsys_local_worker login inherit nosuperuser nocreatedb nocreaterole nobypassrls;
  end if;
end `$`$;
alter role yap_recsys_local_worker password '$password';
grant yap_recommendation_worker to yap_recsys_local_worker;
commit;
"@
        $sql | & docker exec -i supabase_db_Yap_chat psql -U postgres -d postgres -v ON_ERROR_STOP=1 -q
        if ($LASTEXITCODE -ne 0) { throw 'Cannot provision the restricted local worker login.' }
        New-Item -ItemType Directory -Force -Path (Split-Path $configPath -Parent) | Out-Null
        @{ database_url = "postgres://yap_recsys_local_worker:${password}@127.0.0.1:54322/postgres?sslmode=disable";
           model_url = 'http://127.0.0.1:8091' } | ConvertTo-Json | Set-Content -LiteralPath $configPath -Encoding utf8
        Write-Host 'Restricted local worker login configured. Credentials saved under .local/.'
    } finally { Pop-Location }
    exit 0
}

if (!(Test-Path -LiteralPath $configPath)) { throw 'Run setup first.' }
$config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
$database = [Uri]$config.database_url
if ($database.Host -ne '127.0.0.1' -or $database.Port -ne 54322 -or $database.AbsolutePath -ne '/postgres') {
    throw 'This helper only starts a worker against local YapChat Supabase.'
}
$names = @('DATABASE_URL', 'MODEL_URL', 'GOMODCACHE', 'GOCACHE')
$previous = @{}
foreach ($name in $names) { $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
Push-Location $serviceRoot
try {
    $env:DATABASE_URL = $config.database_url
    $env:MODEL_URL = $config.model_url
    $env:GOMODCACHE = Join-Path $serviceRoot '.cache/gomod'
    $env:GOCACHE = Join-Path $serviceRoot '.cache/go-build'
    $arguments = @('run', './cmd/worker', '-concurrency', $Concurrency)
    if ($Once) { $arguments += '-once' }
    if ($RetryFailed) { $arguments += '-retry-failed' }
    & go @arguments
    if ($LASTEXITCODE -ne 0) { throw 'The worker exited with an error.' }
} finally {
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
    Pop-Location
}
