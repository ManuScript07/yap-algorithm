param(
    [ValidateSet('init','api','worker','model','test','seed','simulate','export','train')]
    [string]$Action = 'test',
    [int]$Posts = 1000,
    [int]$Rounds = 3
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot
$env:GOMODCACHE = Join-Path $projectRoot '.cache/gomod'
$env:GOCACHE = Join-Path $projectRoot '.cache/go-build'
$env:UV_CACHE_DIR = Join-Path $projectRoot '.cache/uv'
$env:HF_HOME = Join-Path $projectRoot '.cache/huggingface'
$env:TORCH_HOME = Join-Path $projectRoot '.cache/torch'
$env:HF_HUB_DISABLE_XET = '1'
$env:HF_HUB_DISABLE_TELEMETRY = '1'
$env:DATABASE_URL = 'postgres://yap_local_api:yap-local-api@127.0.0.1:55432/yap_algorithm?sslmode=disable'
$env:AUTH_MODE = 'local_fixture'
$env:LISTEN_ADDR = '127.0.0.1:8090'
$env:CURSOR_SECRET = 'local-only-cursor-secret-please-change-2026'
$env:EMBEDDING_MODEL = 'intfloat/multilingual-e5-small@614241f622f53c4eeff9890bdc4f31cfecc418b3:onnx-int8:query'
function Database-SQL([string]$Source,[string]$Database='yap_algorithm') {
    if($Database -notin @('yap_algorithm','yap_algorithm_test','postgres')){throw 'Unknown local database'}
    $result = $Source | docker exec -i yap_algorithm_db psql -U supabase_admin -d $Database -v ON_ERROR_STOP=1 -qAt
    if ($LASTEXITCODE -ne 0) { throw 'Local database operation failed' }
    return $result
}
switch ($Action) {
    'init' {
        docker inspect yap_algorithm_db *> $null
        if ($LASTEXITCODE -eq 0) { docker start yap_algorithm_db | Out-Null }
        else { docker compose up -d db; if ($LASTEXITCODE -ne 0) { throw 'Cannot start local database' } }
        $ready = $false
        for ($attempt=0;$attempt -lt 30;$attempt++) {
            docker exec yap_algorithm_db pg_isready -U supabase_admin -d yap_algorithm *> $null
            if ($LASTEXITCODE -eq 0) { $ready=$true;break }
            Start-Sleep -Seconds 1
        }
        if (!$ready) { throw 'Local database not ready' }
        foreach($database in @('yap_algorithm','yap_algorithm_test')){
        $exists = Database-SQL "select exists(select 1 from pg_database where datname='$database');" 'postgres'
        if($exists -ne 't'){Database-SQL "create database $database;" 'postgres'}
        $marker = Database-SQL "select to_regclass('private.yap_algorithm_fixture')is not null;" $database
        if ($marker -ne 't') {
            $existing = Database-SQL "select to_regclass('public.profiles')is not null;" $database
            if ($existing -eq 't') { throw 'Refusing to install fixtures over an existing application database' }
            Database-SQL (Get-Content -Raw -Encoding utf8 tests/fixtures/core.sql) $database
        }
        Database-SQL 'create table if not exists private.fixture_migrations(version text primary key,sha256 text not null);' $database
        foreach ($migration in Get-ChildItem supabase/migrations/*.sql | Sort-Object Name) {
            $version = $migration.BaseName
            $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $migration.FullName).Hash
            $applied = Database-SQL "select sha256 from private.fixture_migrations where version='$version';" $database
            if ($applied) { if ($applied -ne $hash) { throw "Changed applied migration: $version" };continue }
            Database-SQL (Get-Content -Raw -Encoding utf8 $migration.FullName) $database
            Database-SQL "insert into private.fixture_migrations values('$version','$hash');" $database
        }
        Database-SQL (Get-Content -Raw -Encoding utf8 tests/fixtures/roles.sql) $database
        }
        Write-Output 'Isolated demo and test databases ready on 127.0.0.1:55432.'
    }
    'api' { go run ./cmd/yap-algorithm -role api }
    'worker' { $env:DATABASE_URL='postgres://yap_local_worker:yap-local-worker@127.0.0.1:55432/yap_algorithm?sslmode=disable';go run ./cmd/yap-algorithm -role worker }
    'model' { & .venv/Scripts/python.exe ml/service.py }
    'test' { $env:YAP_INTEGRATION='1';$env:CGO_ENABLED='1';go test -race ./... -count=1 }
    'seed' { python scripts/seed.py --posts $Posts }
    'simulate' { python scripts/simulate.py --rounds $Rounds }
    'export' { python ml/export.py }
    'train' { & .venv/Scripts/python.exe ml/train.py output/dataset.jsonl }
}
if ($LASTEXITCODE -ne 0) { throw "Command failed: $Action" }
