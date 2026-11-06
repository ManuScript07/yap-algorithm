param(
    [Parameter(Mandatory = $true)][string]$AppRoot,
    [switch]$Race,
    [switch]$WithModel,
    [switch]$WithLoad,
    [switch]$WithMedia,
    [switch]$WithSoak,
    [ValidateRange(1,120)][int]$SoakMinutes = 60,
    [string]$Run = '^TestSupabaseLocal(Pipeline|Media|MediaCapacity|MediaDBOutage|Social|SocialSecurity|SocialLoad|Follows|FollowsLoad|Nearby|Preparation|PreparationLoad|FeedWorkBound|FollowAuthorWork|Feedback|FeedbackLoad)$'
)
$ErrorActionPreference = 'Stop'
$serviceRoot = Split-Path $PSScriptRoot -Parent
$AppRoot = [IO.Path]::GetFullPath($AppRoot)
if ($WithSoak) {
    $drives = @($env:SystemDrive.TrimEnd(':'), [IO.Path]::GetPathRoot($serviceRoot).Substring(0,1)) | Select-Object -Unique
    foreach ($drive in $drives) {
        if ((Get-PSDrive -Name $drive).Free -lt 4GB) { throw "Soak requires at least 4 GiB free on drive $drive." }
    }
}
$cli = Join-Path $AppRoot 'node_modules/.bin/supabase.cmd'
if (!(Test-Path -LiteralPath $cli)) { throw 'Supabase CLI is missing in AppRoot.' }
$names = @('YAP_SUPABASE_INTEGRATION', 'YAP_SUPABASE_APP_ROOT', 'YAP_SUPABASE_LOCAL_ADMIN_DSN', 'YAP_SUPABASE_LOCAL_URL',
    'YAP_SUPABASE_LOCAL_ANON_KEY', 'YAP_SUPABASE_LOCAL_SERVICE_KEY', 'GOMODCACHE', 'GOCACHE', 'YAP_SUPABASE_MODEL_TEST', 'YAP_SUPABASE_LOAD_TEST',
    'YAP_SUPABASE_MEDIA_TEST','YAP_SUPABASE_MEDIA_DSN','YAP_SUPABASE_MEDIA_TOKEN',
    'YAP_SUPABASE_LOCAL_JWT_SECRET','YAP_SUPABASE_MEDIA_SOAK','YAP_SUPABASE_SOAK_MINUTES','YAP_SUPABASE_RECSYS_DSN')
$previous = @{}
foreach ($name in $names) { $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
Push-Location $AppRoot
try {
    $settings = & $cli status -o json 2>$null | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $settings.API_URL -ne 'http://127.0.0.1:54321') {
        throw 'Tests require the running local YapChat Supabase on port 54321.'
    }
    $env:YAP_SUPABASE_INTEGRATION = '1'
    $env:YAP_SUPABASE_APP_ROOT = $AppRoot
    $env:YAP_SUPABASE_MODEL_TEST = if ($WithModel) { '1' } else { '0' }
    $env:YAP_SUPABASE_LOAD_TEST = if ($WithLoad) { '1' } else { '0' }
    $env:YAP_SUPABASE_MEDIA_TEST = if ($WithMedia -or $WithSoak) { '1' } else { '0' }
    $env:YAP_SUPABASE_MEDIA_SOAK = if ($WithSoak) { '1' } else { '0' }
    $env:YAP_SUPABASE_SOAK_MINUTES = "$SoakMinutes"
    $env:YAP_SUPABASE_LOCAL_JWT_SECRET = $settings.JWT_SECRET
    if ($WithMedia -or $WithSoak) {
        $media = Get-Content -LiteralPath (Join-Path $serviceRoot '.local/supabase-media-worker.json') -Raw | ConvertFrom-Json
        if ($media.url -ne $settings.API_URL) { throw 'Media identity does not match local Supabase.' }
        $env:YAP_SUPABASE_MEDIA_DSN = $media.database_url
        $env:YAP_SUPABASE_MEDIA_TOKEN = $media.storage_token
    }
    if ($WithModel) {
        $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8091/healthz' -TimeoutSec 5
        if (!$health.embedding_model) { throw 'Start the local embedding service and wait until it is ready.' }
        if ($WithSoak) {
            $recsys = Get-Content -Raw -LiteralPath (Join-Path $serviceRoot '.local/supabase-worker.json') | ConvertFrom-Json
            $recsysDatabase = [Uri]$recsys.database_url
            if ($recsysDatabase.Host -ne '127.0.0.1' -or $recsysDatabase.Port -ne 54322) { throw 'Local recommendation identity required.' }
            $env:YAP_SUPABASE_RECSYS_DSN = $recsys.database_url
        }
    }
    $env:YAP_SUPABASE_LOCAL_ADMIN_DSN = $settings.DB_URL
    $env:YAP_SUPABASE_LOCAL_URL = $settings.API_URL
    $env:YAP_SUPABASE_LOCAL_ANON_KEY = $settings.ANON_KEY
    $env:YAP_SUPABASE_LOCAL_SERVICE_KEY = $settings.SERVICE_ROLE_KEY
    $env:GOMODCACHE = Join-Path $serviceRoot '.cache/gomod'
    $env:GOCACHE = Join-Path $serviceRoot '.cache/go-build'
    Set-Location $serviceRoot
    $timeout = if ($WithLoad -and $WithMedia) { '10m' } elseif ($WithLoad) { '6m' } elseif ($WithMedia) { '5m' } else { '3m' }
    if ($WithSoak) { $timeout = "$($SoakMinutes+15)m"; $Run='^TestSupabaseLocalMediaSoak$' }
    $arguments = @('test', './tests', '-run', $Run, '-count=1', '-v', "-timeout=$timeout")
    if ($Race) { $arguments += '-race' }
    & go @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Supabase integration tests failed.' }
} finally {
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
    Pop-Location
}
