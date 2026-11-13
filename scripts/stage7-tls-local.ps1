$ErrorActionPreference = 'Stop'
$serviceRoot = Split-Path $PSScriptRoot -Parent
. (Join-Path $PSScriptRoot 'local-supabase-target.ps1')
$target = Get-LocalSupabaseTarget $serviceRoot '' $true
$identity = Get-Content -LiteralPath (Join-Path $target.StateDir 'supabase-worker.json') -Raw | ConvertFrom-Json
if (!(Test-LocalSupabaseDatabase $identity.database_url $target)) { throw 'Expected the isolated worker identity.' }

function Invoke-TlsSql([string]$Sql) {
    $Sql | docker exec -i $target.Container psql -X -qAt -v ON_ERROR_STOP=1 -U supabase_admin -d postgres
    if ($LASTEXITCODE -ne 0) { throw 'Could not configure TLS in the isolated database.' }
}

if ((Invoke-TlsSql 'show ssl;') -ne 'off') { throw 'The TLS rehearsal requires the isolated stack default configuration.' }
$certDir = docker exec -u postgres $target.Container mktemp -d /tmp/yap-stage7-tls.XXXXXX
if ($LASTEXITCODE -ne 0 -or $certDir -notmatch '^/tmp/yap-stage7-tls\.[A-Za-z0-9]{6}$') { throw 'Cannot create isolated certificate files.' }
$caFile = Join-Path $target.StateDir 'tls-rehearsal-ca.crt'
$oldDatabase = $env:DATABASE_URL
$oldModel = $env:MODEL_URL
Push-Location $serviceRoot
try {
    $rsa = [Security.Cryptography.RSA]::Create(2048)
    try {
        $x509 = [Security.Cryptography.X509Certificates.CertificateRequest]::new('CN=localhost', $rsa,
            [Security.Cryptography.HashAlgorithmName]::SHA256, [Security.Cryptography.RSASignaturePadding]::Pkcs1)
        $names = [Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder]::new()
        $names.AddDnsName('localhost')
        $x509.CertificateExtensions.Add($names.Build())
        $certificate = $x509.CreateSelfSigned([DateTimeOffset]::UtcNow.AddMinutes(-1), [DateTimeOffset]::UtcNow.AddDays(1))
        try { $certificate.ExportCertificatePem() | Set-Content -LiteralPath $caFile -Encoding ascii }
        finally { $certificate.Dispose() }
        $rsa.ExportPkcs8PrivateKeyPem() | docker exec -i -u postgres $target.Container sh -c 'umask 077; cat > "$1/key.pem"' sh $certDir
        if ($LASTEXITCODE -ne 0) { throw 'Cannot install the isolated private key.' }
    } finally { $rsa.Dispose() }
    Get-Content -Raw -LiteralPath $caFile | docker exec -i -u postgres $target.Container sh -c 'cat > "$1/cert.pem"' sh $certDir
    if ($LASTEXITCODE -ne 0) { throw 'Cannot install the isolated TLS certificate.' }
    Invoke-TlsSql @"
alter system set ssl_cert_file='$certDir/cert.pem';
alter system set ssl_key_file='$certDir/key.pem';
alter system set ssl=on;
select pg_reload_conf();
"@ | Out-Null
    $ready = $false
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        if ((Invoke-TlsSql 'show ssl;') -eq 'on') { $ready = $true; break }
        Start-Sleep -Milliseconds 250
    }
    if (!$ready) { throw 'The isolated Postgres listener did not enable TLS.' }

    $dsn = [UriBuilder]::new($identity.database_url)
    $dsn.Query = 'sslmode=verify-full&sslrootcert=' + [Uri]::EscapeDataString($caFile)
    $dsn.Host = 'localhost'
    $env:DATABASE_URL = $dsn.Uri.AbsoluteUri
    $env:MODEL_URL = 'http://127.0.0.1:8091'
    & go run ./cmd/worker -once
    if ($LASTEXITCODE -ne 0) { throw 'Worker could not start with a verified TLS connection.' }

    $dsn.Host = '127.0.0.1'
    $env:DATABASE_URL = $dsn.Uri.AbsoluteUri
    $rejected = & go run ./cmd/worker -once 2>&1
    if ($LASTEXITCODE -eq 0 -or ($rejected -join "`n") -notmatch 'cannot verify worker database role') {
        throw 'A connection with a mismatched certificate hostname was not rejected as expected.'
    }
    Write-Output 'Worker accepted a trusted TLS connection and rejected a certificate hostname mismatch.'
} finally {
    $env:DATABASE_URL = $oldDatabase
    $env:MODEL_URL = $oldModel
    Pop-Location
    Invoke-TlsSql @'
alter system reset ssl;
alter system reset ssl_cert_file;
alter system reset ssl_key_file;
select pg_reload_conf();
'@ | Out-Null
    docker exec -u postgres $target.Container rm -f -- "$certDir/key.pem" "$certDir/cert.pem"
    if ($LASTEXITCODE -ne 0) { throw 'Cannot remove the isolated TLS files.' }
    docker exec -u postgres $target.Container rmdir -- $certDir
    if ($LASTEXITCODE -ne 0) { throw 'Cannot remove the isolated TLS directory.' }
}
