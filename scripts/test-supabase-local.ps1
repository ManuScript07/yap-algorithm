param(
    [Parameter(Mandatory = $true)][string]$AppRoot,
    [switch]$Race,
    [switch]$WithModel,
    [switch]$WithLoad
)
$ErrorActionPreference = 'Stop'
$serviceRoot = Split-Path $PSScriptRoot -Parent
$cli = Join-Path $AppRoot 'node_modules/.bin/supabase.cmd'
if (!(Test-Path -LiteralPath $cli)) { throw 'Supabase CLI is missing in AppRoot.' }
$names = @('YAP_SUPABASE_INTEGRATION', 'YAP_SUPABASE_LOCAL_ADMIN_DSN', 'YAP_SUPABASE_LOCAL_URL',
    'YAP_SUPABASE_LOCAL_ANON_KEY', 'YAP_SUPABASE_LOCAL_SERVICE_KEY', 'GOMODCACHE', 'GOCACHE', 'YAP_SUPABASE_MODEL_TEST', 'YAP_SUPABASE_LOAD_TEST')
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
    $arguments = @('test', './tests', '-run', '^TestSupabaseLocalPipeline$', '-count=1', '-v', '-timeout=3m')
    if ($Race) { $arguments += '-race' }
    & go @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Supabase integration tests failed.' }
} finally {
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
    Pop-Location
}
