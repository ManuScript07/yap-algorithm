function Get-LocalSupabaseTarget([string]$ServiceRoot, [string]$AppRoot, [bool]$Isolated) {
    if ($Isolated) {
        $lab = Join-Path $ServiceRoot '.local/stage7-supabase'
        return @{ Name='stage7'; Workdir=$lab; StateDir=$lab; Api='http://127.0.0.1:55321';
            DatabasePort=55322; Container='supabase_db_yap_stage7' }
    }
    return @{ Name='default'; Workdir=$AppRoot; StateDir=(Join-Path $ServiceRoot '.local');
        Api='http://127.0.0.1:54321'; DatabasePort=54322; Container='supabase_db_Yap_chat' }
}

function Test-LocalSupabaseDatabase([string]$ConnectionString, [hashtable]$Target) {
    $database = [Uri]$ConnectionString
    return $database.Scheme -in @('postgres', 'postgresql') -and $database.Host -eq '127.0.0.1' -and
        $database.Port -eq $Target.DatabasePort -and $database.AbsolutePath -eq '/postgres' -and
        $database.Query -in @('', '?sslmode=disable')
}
