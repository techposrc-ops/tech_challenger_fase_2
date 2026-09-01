# Implantação no Google Cloud

O streaming preserva Kafka e PySpark:

```text
Cloud Scheduler -> Cloud Run -> Managed Service for Apache Kafka
                                      |
                                      v
                           Dataproc / PySpark Streaming
                                      |
                                      v
                       Cloud Storage -> BigQuery/BigLake
```

O produtor está em `cloud/gcp/cloud_run`; a lógica compartilhada fica em
`src/alfabetizacao/streaming`. O consumidor é PySpark puro, sem dependência de um serviço de
nuvem. O script `cloud/gcp/dataproc/submit-streaming.ps1` mostra como submetê-lo ao Dataproc.

O cluster Kafka e o Dataproc são opcionais no Terraform devido ao custo contínuo. Consulte
`infrastructure/terraform/README.md` antes de ativá-los.
