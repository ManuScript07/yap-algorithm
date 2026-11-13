param(
    [Parameter(Mandatory=$true)][string]$AppRoot,
    [switch]$Isolated,
    [ValidateSet('off','worker','inference')][string]$Mode='off',
    [ValidateRange(500,3000)][int]$RequestsPerMinute=500,
    [ValidateRange(10,180)][int]$Seconds=60,
    [ValidateRange(1,30)][int]$PageSize=10,
    [ValidateSet(60,500)][int]$Readers=60,
    [ValidatePattern('^[a-zA-Z0-9-]{1,48}$')][string]$Label='manual',
    [switch]$WithExport,
    [switch]$WithTopics,
    [switch]$WithScroll,
    [switch]$WithMixed
)
$ErrorActionPreference='Stop'
$names=@('YAP_FEED_PERF_MODE','YAP_FEED_PERF_RATE','YAP_FEED_PERF_SECONDS','YAP_FEED_PERF_LABEL','YAP_FEED_PERF_PAGE_SIZE','YAP_FEED_PERF_EXPORT','YAP_FEED_PERF_TOPICS','YAP_FEED_PERF_SCROLL','YAP_FEED_PERF_MIXED','YAP_FEED_PERF_READERS')
$previous=@{}
foreach($name in $names){$previous[$name]=[Environment]::GetEnvironmentVariable($name,'Process')}
try{
    $env:YAP_FEED_PERF_MODE=$Mode
    $env:YAP_FEED_PERF_RATE="$RequestsPerMinute"
    $env:YAP_FEED_PERF_SECONDS="$Seconds"
    $env:YAP_FEED_PERF_LABEL=$Label
    $env:YAP_FEED_PERF_PAGE_SIZE="$PageSize"
    $env:YAP_FEED_PERF_READERS="$Readers"
    $env:YAP_FEED_PERF_EXPORT=if($WithExport){'1'}else{'0'}
    $env:YAP_FEED_PERF_TOPICS=if($WithTopics){'1'}else{'0'}
    $env:YAP_FEED_PERF_SCROLL=if($WithScroll){'1'}else{'0'}
    $env:YAP_FEED_PERF_MIXED=if($WithMixed){'1'}else{'0'}
    $arguments=@{AppRoot=$AppRoot;Isolated=$Isolated;WithLoad=$true;Run='^TestSupabaseLocalFeedPerformance$'}
    if($Readers -eq 500){$arguments.TimeoutMinutes=15}
    if($Mode -eq 'inference'){$arguments.WithModel=$true}
    & (Join-Path $PSScriptRoot 'test-supabase-local.ps1') @arguments
    if($LASTEXITCODE -ne 0){throw 'Feed benchmark failed; inspect per-surface results.'}
}finally{
    foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$previous[$name],'Process')}
}
