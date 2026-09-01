# Estratégia aplicada ao projeto

Terraform, Kafka, PySpark, arquitetura
medalhão, checkpoints, idempotência e observabilidade — usando serviços Google Cloud e código
portável.

## Componentes

1. Terraform provisiona rede, storage, catálogo e streaming opcional.
2. Cloud Run hospeda o produtor Python com identidade da service account.
3. Managed Service for Apache Kafka mantém a interface Kafka padrão.
4. Dataproc executa Spark Structured Streaming sem APIs proprietárias no job.
5. Cloud Storage recebe Parquet particionado e checkpoints.
6. BigQuery/BigLake disponibiliza a camada analítica.
7. Scheduler e Workflows cuidam do agendamento e orquestração.
8. Logging e Monitoring fornecem observabilidade.

## Organização

- `batch/bronze.py`, `silver.py` e `gold.py`: regras do pipeline.
- `streaming/producer.py`: produtor Kafka configurável.
- `streaming/spark_job.py`: consumidor PySpark.
- `streaming/simulator.py`: eventos locais reproduzíveis.

## FinOps

- Kafka e Dataproc usam `enable_streaming = false` por padrão.
- Checkpoints temporários possuem expiração.
- Logs operacionais usam nível moderado.
- O Terraform pode criar alertas de orçamento e nunca executa `apply` automaticamente.
- O Dataproc deve ser removido após demonstrações sem consumo contínuo.

## Implantação

1. Autenticar com Application Default Credentials.
2. Preencher `terraform.tfvars.example`.
3. Executar `terraform init`, `fmt`, `validate` e `plan`.
4. Revisar custos antes de habilitar streaming.
5. Construir o contêiner em `cloud/gcp/cloud_run`.
6. Enviar job e login handler Kafka ao Cloud Storage.
7. Submeter o job com `cloud/gcp/dataproc/submit-streaming.ps1`.
