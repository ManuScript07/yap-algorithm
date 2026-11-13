param(
    [Parameter(Mandatory = $true)][ValidateSet('setup','worker')][string]$Action,
    [string]$AppRoot,
    [switch]$Isolated,
    [ValidateRange(1,2)][int]$Concurrency = 1,
    [switch]$Once,
    [switch]$RetryFailed,
    [switch]$Status
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
if ($AppRoot) { $AppRoot = [IO.Path]::GetFullPath($AppRoot) }
. (Join-Path $PSScriptRoot 'local-supabase-target.ps1')
$target = Get-LocalSupabaseTarget $root $AppRoot $Isolated.IsPresent
$configPath = Join-Path $target.StateDir 'supabase-media-worker.json'
if ($Action -eq 'setup') {
    if (!$AppRoot) { throw 'AppRoot is required.' }
    Push-Location $AppRoot
    try {
        $settings = & (Join-Path $AppRoot 'node_modules/.bin/supabase.cmd') --workdir $target.Workdir status -o json 2>$null | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $settings.API_URL -ne $target.Api -or !$settings.JWT_SECRET -or !(Test-LocalSupabaseDatabase $settings.DB_URL $target)) {
            throw 'Setup requires the running local YapChat Supabase.'
        }
        $password = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
        $sql = @"
begin;
do `$`$ begin
  if to_regprocedure('media.contract()')is null then raise exception 'Apply the canonical media migration first';end if;
  if not exists(select 1 from pg_roles where rolname='yap_media_local_worker')then
    create role yap_media_local_worker login inherit nosuperuser nocreatedb nocreaterole nobypassrls;
  end if;
end `$`$;
alter role yap_media_local_worker password '$password';
grant yap_feed_media_worker to yap_media_local_worker;
commit;
"@
        $sql | docker exec -i $target.Container psql -U postgres -d postgres -v ON_ERROR_STOP=1 -q
        if ($LASTEXITCODE -ne 0) { throw 'Cannot provision the local media role.' }
        function Base64Url([byte[]]$Value) { return [Convert]::ToBase64String($Value).TrimEnd('=').Replace('+','-').Replace('/','_') }
        $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
        $header = Base64Url ([Text.Encoding]::UTF8.GetBytes('{"alg":"HS256","typ":"JWT"}'))
        $payload = @{ role='yap_feed_media_worker'; sub=[guid]::NewGuid().ToString(); aud='authenticated'; iat=$now; exp=$now+604800 } | ConvertTo-Json -Compress
        $payload = Base64Url ([Text.Encoding]::UTF8.GetBytes($payload))
        $signer = [Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($settings.JWT_SECRET))
        try { $signature = Base64Url ($signer.ComputeHash([Text.Encoding]::UTF8.GetBytes("$header.$payload"))) }
        finally { $signer.Dispose() }
        New-Item -ItemType Directory -Force -Path (Split-Path $configPath -Parent) | Out-Null
        @{ database_url="postgres://yap_media_local_worker:${password}@127.0.0.1:$($target.DatabasePort)/postgres?sslmode=disable";
           url=$settings.API_URL; api_key=$settings.ANON_KEY; storage_token="$header.$payload.$signature" } |
            ConvertTo-Json | Set-Content -LiteralPath $configPath -Encoding utf8
        Write-Output 'Restricted local media identity configured. Token expires in 7 days; rerun setup to renew.'
    } finally { Pop-Location }
    exit 0
}
if (!(Test-Path -LiteralPath $configPath)) { throw 'Run media setup first.' }
$config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
if ($config.url -ne $target.Api -or !(Test-LocalSupabaseDatabase $config.database_url $target)) {
    throw 'This helper only accepts local YapChat Supabase.'
}
$values = @{MEDIA_DATABASE_URL=$config.database_url; SUPABASE_URL=$config.url; SUPABASE_PUBLISHABLE_KEY=$config.api_key;
    MEDIA_STORAGE_TOKEN=$config.storage_token; GOMODCACHE=(Join-Path $root '.cache/gomod'); GOCACHE=(Join-Path $root '.cache/go-build')}
$previous = @{}
Push-Location $root
try {
    foreach ($name in $values.Keys) {
        $previous[$name] = [Environment]::GetEnvironmentVariable($name,'Process')
        [Environment]::SetEnvironmentVariable($name,$values[$name],'Process')
    }
    $arguments = @('run','./cmd/media-worker','-concurrency',"$Concurrency")
    if ($Once) { $arguments += '-once' }
    if ($RetryFailed) { $arguments += '-retry-failed' }
    if ($Status) { $arguments += '-status' }
    & go @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Media worker failed.' }
} finally {
    foreach ($name in $previous.Keys) { [Environment]::SetEnvironmentVariable($name,$previous[$name],'Process') }
    Pop-Location
}
