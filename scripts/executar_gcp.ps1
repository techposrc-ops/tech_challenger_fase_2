param(
    [Parameter(Mandatory = $true)]
    [string]$Projeto,

    [ValidateSet("planejar", "batch", "streaming", "desligar-streaming")]
    [string]$Etapa = "planejar",

    [string]$Regiao = "us-central1",
    [string]$Ambiente = "dev",
    [string]$BucketEstado,
    [switch]$ConfirmarCustos
)

$ErrorActionPreference = "Stop"
$RaizProjeto = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$PastaTerraform = Join-Path $RaizProjeto "infrastructure\terraform"
$ArquivoVariaveis = Join-Path $PastaTerraform "terraform.tfvars"
$Subrede = "alfabetizacao-$Ambiente-subnet"

if (-not $BucketEstado) {
    $BucketEstado = "$Projeto-tfstate"
}

function Conferir-Comando($Nome) {
    if (-not (Get-Command $Nome -ErrorAction SilentlyContinue)) {
        throw "Comando não encontrado: $Nome"
    }
}

function Conferir-Saida($Mensagem) {
    if ($LASTEXITCODE -ne 0) {
        throw $Mensagem
    }
}

function Executar-Terraform($Argumentos) {
    & terraform @Argumentos
    Conferir-Saida "O Terraform terminou com erro."
}

Conferir-Comando "gcloud"
Conferir-Comando "terraform"

gcloud config set project $Projeto | Out-Null
Conferir-Saida "Não foi possível selecionar o projeto GCP."

gcloud auth application-default print-access-token | Out-Null
Conferir-Saida (
    "Credenciais ADC não encontradas. Execute: " +
    "gcloud auth application-default login"
)

if ($Etapa -ne "planejar" -and -not $ConfirmarCustos) {
    throw "Use -ConfirmarCustos para permitir criação ou remoção de recursos GCP."
}

gcloud storage buckets describe "gs://$BucketEstado" --project=$Projeto 2>$null | Out-Null
$BucketEstadoExiste = $LASTEXITCODE -eq 0

if (-not $BucketEstadoExiste) {
    if (-not $ConfirmarCustos) {
        throw "Crie o bucket de estado ou execute novamente com -ConfirmarCustos."
    }

    Write-Host "Criando bucket de estado: gs://$BucketEstado"
    gcloud services enable storage.googleapis.com serviceusage.googleapis.com `
        cloudresourcemanager.googleapis.com --project=$Projeto
    Conferir-Saida "Não foi possível habilitar as APIs iniciais."

    gcloud storage buckets create "gs://$BucketEstado" `
        --project=$Projeto `
        --location=$Regiao `
        --uniform-bucket-level-access
    Conferir-Saida "Não foi possível criar o bucket de estado."
}

if (-not (Test-Path $ArquivoVariaveis)) {
    @"
gcp_project_id = "$Projeto"
gcp_region     = "$Regiao"
environment    = "$Ambiente"

enable_streaming               = false
enable_kafka                   = false
enable_streaming_producer      = false
enable_streaming_consumer      = false
enable_streaming_orchestration = false

monthly_budget_usd = 50
"@ | Set-Content -LiteralPath $ArquivoVariaveis -Encoding UTF8
    Write-Host "Arquivo local criado: $ArquivoVariaveis"
}

Push-Location $PastaTerraform

try {
    Executar-Terraform @(
        "init",
        "-reconfigure",
        "-backend-config=bucket=$BucketEstado"
    )
    Executar-Terraform @("fmt", "-check")
    Executar-Terraform @("validate")

    $Estado = & terraform state list 2>$null
    $GoldJaExiste = $Estado -match "google_bigquery_table.gold_externa"
    $GoldInicial = if ($GoldJaExiste) { "true" } else { "false" }

    if ($Etapa -eq "planejar") {
        Executar-Terraform @(
            "plan",
            "-var=enable_gold_tables=$GoldInicial",
            "-out=tfplan"
        )
        Write-Host "Plano criado em infrastructure/terraform/tfplan."
        return
    }

    if ($Etapa -eq "batch") {
        Executar-Terraform @(
            "plan",
            "-var=enable_gold_tables=$GoldInicial",
            "-out=tfplan-base"
        )
        Executar-Terraform @("apply", "-auto-approve", "tfplan-base")

        $BucketDados = & terraform output -raw data_lake_bucket
        $ContaBatch = & terraform output -raw batch_service_account
        Conferir-Saida "Não foi possível ler os resultados do Terraform."

        Pop-Location
        & "$RaizProjeto\cloud\gcp\dataproc\submit-batch.ps1" `
            -Projeto $Projeto `
            -Regiao $Regiao `
            -Bucket $BucketDados `
            -ContaServico $ContaBatch `
            -Subrede $Subrede `
            -Executar
        Conferir-Saida "O pipeline Batch terminou com erro."
        Push-Location $PastaTerraform

        Executar-Terraform @(
            "plan",
            "-var=enable_gold_tables=true",
            "-out=tfplan-gold"
        )
        Executar-Terraform @("apply", "-auto-approve", "tfplan-gold")
        Write-Host "Batch concluído e tabelas Gold registradas."
        return
    }

    if ($Etapa -eq "streaming") {
        Executar-Terraform @(
            "plan",
            "-var=enable_gold_tables=true",
            "-var=enable_kafka=true",
            "-var=enable_streaming_producer=false",
            "-var=enable_streaming_consumer=false",
            "-var=enable_streaming_orchestration=false",
            "-out=tfplan-kafka"
        )
        Executar-Terraform @("apply", "-auto-approve", "tfplan-kafka")

        $ClusterKafka = & terraform output -raw kafka_cluster
        $BucketDados = & terraform output -raw data_lake_bucket
        Conferir-Saida "Não foi possível ler os recursos do Streaming."

        $Bootstrap = & gcloud managed-kafka clusters describe $ClusterKafka `
            --project=$Projeto `
            --location=$Regiao `
            --format="value(bootstrapAddress)"
        Conferir-Saida "Não foi possível obter o endereço do Kafka."

        $Imagem = (
            "$Regiao-docker.pkg.dev/$Projeto/alfabetizacao/" +
            "produtor:latest"
        )

        Pop-Location
        gcloud builds submit `
            --project=$Projeto `
            --config="$RaizProjeto\cloud\gcp\cloud_build\produtor.yaml" `
            --substitutions="_IMAGEM=$Imagem" `
            $RaizProjeto
        Conferir-Saida "Não foi possível publicar a imagem do produtor."
        Push-Location $PastaTerraform

        Executar-Terraform @(
            "plan",
            "-var=enable_gold_tables=true",
            "-var=enable_kafka=true",
            "-var=enable_streaming_producer=true",
            "-var=enable_streaming_consumer=true",
            "-var=enable_streaming_orchestration=true",
            "-var=producer_image=$Imagem",
            "-var=kafka_bootstrap_servers=$Bootstrap",
            "-out=tfplan-streaming"
        )
        Executar-Terraform @("apply", "-auto-approve", "tfplan-streaming")

        $ClusterDataproc = & terraform output -raw dataproc_cluster
        Conferir-Saida "Não foi possível obter o cluster Dataproc."

        Pop-Location
        gcloud storage cp `
            "$RaizProjeto\src\alfabetizacao\streaming\spark_job.py" `
            "gs://$BucketDados/jobs/spark_job.py"
        Conferir-Saida "Não foi possível enviar o consumidor PySpark."

        $TrabalhoConsumidor = Start-Job -ScriptBlock {
            param($Script, $Projeto, $Regiao, $Cluster, $Bucket, $Bootstrap)
            $ErrorActionPreference = "Stop"
            & $Script `
                -IdProjeto $Projeto `
                -Regiao $Regiao `
                -Cluster $Cluster `
                -Bucket $Bucket `
                -ServidoresKafka $Bootstrap
            if ($LASTEXITCODE -ne 0) {
                throw "O job PySpark terminou com erro."
            }
        } -ArgumentList `
            "$RaizProjeto\cloud\gcp\dataproc\submit-streaming.ps1", `
            $Projeto, $Regiao, $ClusterDataproc, $BucketDados, $Bootstrap

        Write-Host "Aguardando o consumidor iniciar..."
        Start-Sleep -Seconds 30
        gcloud scheduler jobs run "alfabetizacao-$Ambiente-producer" `
            --project=$Projeto `
            --location=$Regiao
        Conferir-Saida "Não foi possível acionar o produtor."

        Wait-Job $TrabalhoConsumidor | Out-Null
        Receive-Job $TrabalhoConsumidor
        if ($TrabalhoConsumidor.State -ne "Completed") {
            throw "O consumidor Streaming terminou com erro."
        }
        Remove-Job $TrabalhoConsumidor
        Write-Host "Teste de Streaming concluído."
        return
    }

    if ($Etapa -eq "desligar-streaming") {
        Executar-Terraform @(
            "plan",
            "-var=enable_gold_tables=true",
            "-var=enable_streaming=false",
            "-var=enable_kafka=false",
            "-var=enable_streaming_producer=false",
            "-var=enable_streaming_consumer=false",
            "-var=enable_streaming_orchestration=false",
            "-var=producer_image=null",
            "-var=kafka_bootstrap_servers=null",
            "-out=tfplan-desligar"
        )
        Executar-Terraform @("apply", "-auto-approve", "tfplan-desligar")
        Write-Host "Recursos de Streaming desativados."
    }
}
finally {
    if ((Get-Location).Path -ne $RaizProjeto) {
        Pop-Location -ErrorAction SilentlyContinue
    }
}
