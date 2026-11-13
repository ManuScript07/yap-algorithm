param(
    [Parameter(Mandatory = $true)][string]$AppRoot,
    [ValidateSet('history', 'write')][Parameter(Mandatory = $true)][string]$Profile,
    [ValidateSet(10000, 100000, 300000)][int]$HistoryPosts = 10000
)
$ErrorActionPreference = 'Stop'
$previousHistory = $env:YAP_STAGE7_HISTORY
$previousWrite = $env:YAP_STAGE7_WRITE_CAPACITY
$testScript = Join-Path $PSScriptRoot 'test-supabase-local.ps1'
try {
    if ($Profile -eq 'history') {
        $env:YAP_STAGE7_HISTORY = "$HistoryPosts"
        & $testScript -AppRoot $AppRoot -Isolated -TimeoutMinutes 15 -Run '^TestSupabaseLocalStage7HistoryBurst$'
    } else {
        $env:YAP_STAGE7_WRITE_CAPACITY = '1'
        & $testScript -AppRoot $AppRoot -Isolated -WithModel -TimeoutMinutes 25 -Run '^TestSupabaseLocalStage7WriteCapacity$'
    }
} finally {
    $env:YAP_STAGE7_HISTORY = $previousHistory
    $env:YAP_STAGE7_WRITE_CAPACITY = $previousWrite
}
