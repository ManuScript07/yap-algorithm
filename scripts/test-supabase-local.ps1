param(
    [Parameter(Mandatory = $true)][string]$AppRoot,
    [switch]$Race,
    [switch]$WithModel,
    [switch]$WithLoad,
    [switch]$WithMedia,
    [string]$Run = '^TestSupabaseLocal(Pipeline|Media)$'
)
$ErrorActionPreference = 'Stop'
$serviceRoot = Split-Path $PSScriptRoot -Parent
$cli = Join-Path $AppRoot 'node_modules/.bin/supabase.cmd'
if (!(Test-Path -LiteralPath $cli)) { throw 'Supabase CLI is missing in AppRoot.' }
$names = @('YAP_SUPABASE_INTEGRATION', 'YAP_SUPABASE_LOCAL_ADMIN_DSN', 'YAP_SUPABASE_LOCAL_URL',
    'YAP_SUPABASE_LOCAL_ANON_KEY', 'YAP_SUPABASE_LOCAL_SERVICE_KEY', 'GOMODCACHE', 'GOCACHE', 'YAP_SUPABASE_MODEL_TEST', 'YAP_SUPABASE_LOAD_TEST',
    'YAP_SUPABASE_MEDIA_TEST','YAP_SUPABASE_MEDIA_DSN','YAP_SUPABASE_MEDIA_TOKEN')
$previous = @{}
foreach ($name in $names) { $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
Push-Location $AppRoot
try {
    $settings = & $cli status -o json 2>$null | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $settings.API_URL -ne 'http://127.0.0.1:54321') {
        throw 'Tests require the running local YapChat Supabase on port 54321.'
    }
    $env:YAP_SUPABASE_INTEGRATION = '1'
    $env:YAP_SUPABASE_MODEL_TEST = if ($WithModel) { '1' } else { '0' }
    $env:YAP_SUPABASE_LOAD_TEST = if ($WithLoad) { '1' } else { '0' }
    $env:YAP_SUPABASE_MEDIA_TEST = if ($WithMedia) { '1' } else { '0' }
    if ($WithMedia) {
        $media = Get-Content -LiteralPath (Join-Path $serviceRoot '.local/supabase-media-worker.json') -Raw | ConvertFrom-Json
        if ($media.url -ne $settings.API_URL) { throw 'Media identity does not match local Supabase.' }
        $env:YAP_SUPABASE_MEDIA_DSN = $media.database_url
        $env:YAP_SUPABASE_MEDIA_TOKEN = $media.storage_token
    }
    if ($WithModel) {
        $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8091/healthz' -TimeoutSec 5
        if (!$health.embedding_model) { throw 'Start the local embedding service and wait until it is ready.' }
    }
    $env:YAP_SUPABASE_LOCAL_ADMIN_DSN = $settings.DB_URL
    $env:YAP_SUPABASE_LOCAL_URL = $settings.API_URL
    $env:YAP_SUPABASE_LOCAL_ANON_KEY = $settings.ANON_KEY
    $env:YAP_SUPABASE_LOCAL_SERVICE_KEY = $settings.SERVICE_ROLE_KEY
    $env:GOMODCACHE = Join-Path $serviceRoot '.cache/gomod'
    $env:GOCACHE = Join-Path $serviceRoot '.cache/go-build'
    Set-Location $serviceRoot
    $timeout = if ($WithLoad) { '6m' } else { '3m' }
    $arguments = @('test', './tests', '-run', $Run, '-count=1', '-v', "-timeout=$timeout")
    if ($Race) { $arguments += '-race' }
    & go @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Supabase integration tests failed.' }
} finally {
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
    Pop-Location
}
