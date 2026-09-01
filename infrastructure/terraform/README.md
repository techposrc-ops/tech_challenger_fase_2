# Infraestrutura Terraform no Google Cloud

Com `enable_streaming = false`, cria APIs, VPC privada, Cloud Storage versionado, dataset
BigQuery, service account e orçamento opcional. Com `true`, também cria Kafka gerenciado, tópico e
cluster Dataproc. `producer_image` habilita Cloud Run, Scheduler e Workflows depois que a imagem
for publicada. Kafka e Dataproc têm custo enquanto provisionados e a imagem exige
`enable_streaming = true`.

```powershell
gcloud auth application-default login
Copy-Item terraform.tfvars.example terraform.tfvars
terraform init -backend-config="bucket=SEU_BUCKET_DE_ESTADO"
terraform fmt -check
terraform validate
terraform plan -out=tfplan
```

O bucket de estado deve existir antes do `terraform init`. Se o backend já tiver sido inicializado
com outro bucket, acrescente `-reconfigure` ao comando. Preencha o projeto, revise preços e plano
antes de executar `apply`; o projeto nunca o executa automaticamente. Para o streaming, envie
`spark_job.py` e o login handler oficial ao bucket e use
`cloud/gcp/dataproc/submit-streaming.ps1`.
