param(
    [Parameter(Mandatory = $true)][string]$AppRoot,
    [ValidateSet('start', 'stop')][string]$Action = 'start'
)
$ErrorActionPreference = 'Stop'
$serviceRoot = Split-Path $PSScriptRoot -Parent
$AppRoot = [IO.Path]::GetFullPath($AppRoot)
. (Join-Path $PSScriptRoot 'local-supabase-target.ps1')
$target = Get-LocalSupabaseTarget $serviceRoot $AppRoot $true
$cli = Join-Path $AppRoot 'node_modules/.bin/supabase.cmd'
if (!(Test-Path -LiteralPath $cli)) { throw 'Supabase CLI is missing in AppRoot.' }

$stageSupabase = Join-Path $target.Workdir 'supabase'
$stageConfig = Join-Path $stageSupabase 'config.toml'
if ($Action -eq 'start' -and !(Test-Path -LiteralPath $stageConfig)) {
    $sourceSupabase = Join-Path $AppRoot 'supabase'
    $config = Get-Content -LiteralPath (Join-Path $sourceSupabase 'config.toml') -Raw
    if ($config -notmatch '(?m)^project_id = "Yap_chat"\s*$') { throw 'Expected the canonical YapChat config.' }
    $config = $config.Replace('project_id = "Yap_chat"', 'project_id = "yap_stage7"')
    $config = [regex]::Replace($config, '(?m)(^\s*(?:port|shadow_port)\s*=\s*)5432([0-9])\b', '${1}5532${2}')
    $config = $config.Replace('inspector_port = 8083', 'inspector_port = 8183')
    $seedSetting = '(?m)(^\[db\.seed\]\r?\n(?:(?:#.*|)\r?\n)*enabled\s*=\s*)true'
    $config = [regex]::Replace($config, $seedSetting, '${1}false')
    New-Item -ItemType Directory -Path $stageSupabase | Out-Null
    Copy-Item -LiteralPath (Join-Path $sourceSupabase 'migrations') -Destination $stageSupabase -Recurse
    $installedPostgres = Join-Path $sourceSupabase '.temp/postgres-version'
    if (Test-Path -LiteralPath $installedPostgres) {
        $version = (Get-Content -LiteralPath $installedPostgres -Raw).Trim()
        if ($version -notmatch '^17\.\d+\.\d+\.\d+$') { throw 'Unexpected local Postgres image version.' }
        $stageTemp = New-Item -ItemType Directory -Path (Join-Path $stageSupabase '.temp')
        Set-Content -LiteralPath (Join-Path $stageTemp.FullName 'postgres-version') -Value $version -NoNewline -Encoding utf8
    }
    Set-Content -LiteralPath $stageConfig -Value $config -Encoding utf8
}

$config = Get-Content -LiteralPath $stageConfig -Raw
if ($config -notmatch '(?m)^project_id = "yap_stage7"\s*$' -or
    $config -notmatch '(?m)^port = 55321\s*$' -or $config -notmatch '(?m)^port = 55322\s*$' -or
    $config -notmatch '(?m)^\[db\.seed\]\r?\n(?:(?:#.*|)\r?\n)*enabled\s*=\s*false') {
    throw 'Unexpected stage 7 configuration.'
}

if ($Action -eq 'start') {
    # Acceptance needs Auth, REST, Storage and Realtime. Exclude unrelated UI and functions.
    & $cli --workdir $target.Workdir start --exclude 'studio,postgres-meta,edge-runtime,logflare,vector,mailpit'
} else {
    & $cli --workdir $target.Workdir stop
}
if ($LASTEXITCODE -ne 0) { throw "Could not $Action the isolated Supabase stack." }
