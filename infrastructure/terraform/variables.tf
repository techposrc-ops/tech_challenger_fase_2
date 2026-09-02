variable "gcp_project_id" {
  description = "ID do projeto Google Cloud."
  type        = string
}
variable "gcp_region" {
  description = "Região da solução."
  type        = string
  default     = "us-central1"
}
variable "project_name" {
  type    = string
  default = "alfabetizacao"
}
variable "environment" {
  type    = string
  default = "dev"
}
variable "subnet_cidr" {
  type    = string
  default = "10.20.0.0/20"
}
variable "artifact_registry_repository" {
  description = "Nome do repositório Docker no Artifact Registry."
  type        = string
  default     = "alfabetizacao"
}
variable "enable_gold_tables" {
  description = "Registra as tabelas Gold externas após a geração dos arquivos Parquet."
  type        = bool
  default     = true
}
variable "enable_streaming" {
  description = "Compatibilidade: ativa todos os recursos de streaming. Prefira os controles separados."
  type        = bool
  default     = false
}
variable "enable_kafka" {
  description = "Cria somente o cluster Kafka gerenciado e o tópico."
  type        = bool
  default     = false
}
variable "enable_streaming_producer" {
  description = "Implanta o produtor Kafka no Cloud Run quando imagem e bootstrap forem informados."
  type        = bool
  default     = false
}
variable "enable_streaming_consumer" {
  description = "Cria o cluster Dataproc usado pelo consumidor PySpark."
  type        = bool
  default     = false
}
variable "enable_streaming_orchestration" {
  description = "Cria Scheduler e Workflow após o produtor estar disponível."
  type        = bool
  default     = false
}
variable "kafka_topic" {
  type    = string
  default = "alfabetizacao-indicadores"
}
variable "kafka_partition_count" {
  type    = number
  default = 3
}
variable "kafka_vcpu_count" {
  type    = number
  default = 3
}
variable "kafka_memory_bytes" {
  type    = number
  default = 3221225472
}
variable "dataproc_machine_type" {
  type    = string
  default = "e2-standard-2"
}
variable "dataproc_image_version" {
  type    = string
  default = "2.2-debian12"
}
variable "billing_account_id" {
  type     = string
  default  = null
  nullable = true
}
variable "monthly_budget_usd" {
  type    = number
  default = 50
}
variable "alert_email" {
  description = "E-mail opcional que receberá alertas de falha dos pipelines."
  type        = string
  default     = null
  nullable    = true
}
variable "producer_image" {
  description = "Imagem do produtor no Artifact Registry; null não implanta Cloud Run/Scheduler."
  type        = string
  default     = null
  nullable    = true
}
variable "kafka_bootstrap_servers" {
  description = "Endereço bootstrap do Kafka; null não implanta o produtor."
  type        = string
  default     = null
  nullable    = true
}
variable "events_per_invocation" {
  type    = number
  default = 10
}
