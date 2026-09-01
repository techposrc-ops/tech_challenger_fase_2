param(
    [Parameter(Mandatory = $true)][string]$IdProjeto,
    [Parameter(Mandatory = $true)][string]$Regiao,
    [Parameter(Mandatory = $true)][string]$Cluster,
    [Parameter(Mandatory = $true)][string]$Bucket,
    [Parameter(Mandatory = $true)][string]$ServidoresKafka
)

$argumentos = @(
    "dataproc", "jobs", "submit", "pyspark",
    "gs://$Bucket/jobs/spark_job.py",
    "--project=$IdProjeto",
    "--region=$Regiao",
    "--cluster=$Cluster",
    "--properties-file=cloud/gcp/dataproc/streaming.properties",
    "--",
    "--bootstrap-servers", $ServidoresKafka,
    "--topic", "alfabetizacao-indicadores",
    "--output-path", "gs://$Bucket/bronze/streaming",
    "--checkpoint-path", "gs://$Bucket/checkpoints/alfabetizacao",
    "--tempo-maximo-segundos", "300"
)

& gcloud $argumentos
exit $LASTEXITCODE
