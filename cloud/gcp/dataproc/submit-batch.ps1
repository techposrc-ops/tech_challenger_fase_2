param(
    [Parameter(Mandatory = $true)]
    [string]$Projeto,
    [string]$Regiao = "us-central1",
    [Parameter(Mandatory = $true)]
    [string]$Bucket,
    [Parameter(Mandatory = $true)]
    [string]$ContaServico,
    [string]$Subrede = "alfabetizacao-dev-subnet",
    [switch]$Executar
)

$ErrorActionPreference = "Stop"

$RaizProjeto = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$PastaTemporaria = Join-Path $RaizProjeto "tmp\dataproc"
$PastaPacote = Join-Path $PastaTemporaria "pacote"
$PastaAlfabetizacao = Join-Path $PastaPacote "alfabetizacao"
$PastaBatch = Join-Path $PastaAlfabetizacao "batch"
$ArquivoZip = Join-Path $PastaTemporaria "alfabetizacao.zip"
$ArquivoPrincipal = Join-Path $RaizProjeto "src\alfabetizacao\batch\orquestracao.py"
$PastaCodigoGcs = "gs://$Bucket/codigo-batch"

New-Item -ItemType Directory -Path $PastaTemporaria -Force | Out-Null

if (Test-Path $PastaPacote) {
    Remove-Item -LiteralPath $PastaPacote -Recurse
}

if (Test-Path $ArquivoZip) {
    Remove-Item -LiteralPath $ArquivoZip
}

# Copia somente os arquivos Python usados pelo processamento Batch.
New-Item -ItemType Directory -Path $PastaBatch -Force | Out-Null
Copy-Item `
    (Join-Path $RaizProjeto "src\alfabetizacao\__init__.py") `
    $PastaAlfabetizacao
Copy-Item `
    (Join-Path $RaizProjeto "src\alfabetizacao\observabilidade.py") `
    $PastaAlfabetizacao
Copy-Item `
    (Join-Path $RaizProjeto "src\alfabetizacao\batch\*.py") `
    $PastaBatch

Compress-Archive `
    -Path $PastaAlfabetizacao `
    -DestinationPath $ArquivoZip

Write-Host "Enviando o código para $PastaCodigoGcs..."
gcloud storage cp $ArquivoZip "$PastaCodigoGcs/alfabetizacao.zip"
if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível enviar alfabetizacao.zip."
}

gcloud storage cp $ArquivoPrincipal "$PastaCodigoGcs/orquestracao.py"
if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível enviar orquestracao.py."
}

if (-not $Executar) {
    Write-Host "Código publicado. Use -Executar para iniciar o processamento pago."
    return
}

$DataHora = Get-Date -Format "yyyyMMdd-HHmmss"
$NomeLote = "alfabetizacao-batch-$DataHora".ToLower()

Write-Host "Iniciando o lote $NomeLote..."
$ArgumentosDataproc = @(
    "dataproc", "batches", "submit", "pyspark",
    "$PastaCodigoGcs/orquestracao.py",
    "--project=$Projeto",
    "--region=$Regiao",
    "--batch=$NomeLote",
    "--version=2.2",
    "--service-account=$ContaServico",
    "--subnet=$Subrede",
    "--deps-bucket=$Bucket",
    "--py-files=$PastaCodigoGcs/alfabetizacao.zip",
    "--",
    "--ambiente", "cloud",
    "--pasta-base", "gs://$Bucket",
    "--projeto-faturamento", $Projeto
)

gcloud @ArgumentosDataproc

if ($LASTEXITCODE -ne 0) {
    throw "O lote do Dataproc terminou com erro."
}

Write-Host "Pipeline Bronze, Silver e Gold concluído."
