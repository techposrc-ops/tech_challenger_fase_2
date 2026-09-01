# Infraestrutura Terraform no Google Cloud

Com `enable_streaming = false`, cria APIs, VPC privada, Cloud Storage versionado, dataset
BigQuery, service account e orçamento opcional. Com `true`, também cria Kafka gerenciado, tópico e
cluster Dataproc. `producer_image` habilita Cloud Run, Scheduler e Workflows depois que a imagem
for publicada. Kafka e Dataproc têm custo enquanto provisionados e a imagem exige
`enable_streaming = true`.

```powershell
gcloud auth application-default login
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init
terraform fmt -check
terraform validate
terraform plan -out=tfplan
```

Preencha o projeto, revise preços e plano antes de executar `apply`; o projeto nunca o executa
automaticamente. Para o streaming, envie `spark_job.py` e o login handler oficial ao bucket e use
`cloud/gcp/dataproc/submit-streaming.ps1`.
