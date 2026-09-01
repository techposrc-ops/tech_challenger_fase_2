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
variable "enable_streaming" {
  description = "Cria Kafka, cluster Dataproc Streaming, Cloud Run e orquestração Streaming."
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
