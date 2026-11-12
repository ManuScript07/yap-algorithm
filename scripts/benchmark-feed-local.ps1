param(
    [Parameter(Mandatory=$true)][string]$AppRoot,
    [ValidateSet('off','worker','inference')][string]$Mode='off',
    [ValidateRange(500,3000)][int]$RequestsPerMinute=500,
    [ValidateRange(10,180)][int]$Seconds=60,
    [ValidateRange(1,30)][int]$PageSize=10,
    [ValidatePattern('^[a-zA-Z0-9-]{1,48}$')][string]$Label='manual',
    [switch]$WithExport,
    [switch]$WithTopics
)
$ErrorActionPreference='Stop'
$names=@('YAP_FEED_PERF_MODE','YAP_FEED_PERF_RATE','YAP_FEED_PERF_SECONDS','YAP_FEED_PERF_LABEL','YAP_FEED_PERF_PAGE_SIZE','YAP_FEED_PERF_EXPORT','YAP_FEED_PERF_TOPICS')
$previous=@{}
foreach($name in $names){$previous[$name]=[Environment]::GetEnvironmentVariable($name,'Process')}
try{
    $env:YAP_FEED_PERF_MODE=$Mode
    $env:YAP_FEED_PERF_RATE="$RequestsPerMinute"
    $env:YAP_FEED_PERF_SECONDS="$Seconds"
    $env:YAP_FEED_PERF_LABEL=$Label
    $env:YAP_FEED_PERF_PAGE_SIZE="$PageSize"
    $env:YAP_FEED_PERF_EXPORT=if($WithExport){'1'}else{'0'}
    $env:YAP_FEED_PERF_TOPICS=if($WithTopics){'1'}else{'0'}
    $arguments=@{AppRoot=$AppRoot;WithLoad=$true;Run='^TestSupabaseLocalFeedPerformance$'}
    if($Mode -eq 'inference'){$arguments.WithModel=$true}
    & (Join-Path $PSScriptRoot 'test-supabase-local.ps1') @arguments
    if($LASTEXITCODE -ne 0){throw 'Feed benchmark failed; inspect per-surface results.'}
}finally{
    foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$previous[$name],'Process')}
}
