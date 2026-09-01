output "data_lake_bucket" {
  value = google_storage_bucket.data_lake.name
}
output "bigquery_dataset" {
  value = google_bigquery_dataset.alfabetizacao.dataset_id
}
output "batch_service_account" {
  value = google_service_account.batch.email
}
output "artifact_registry_repository" {
  value = google_artifact_registry_repository.docker.name
}
output "streaming_service_account" {
  value = local.create_streaming_runtime ? google_service_account.streaming[0].email : null
}
output "kafka_cluster" {
  value = local.create_kafka ? google_managed_kafka_cluster.streaming[0].cluster_id : null
}
output "dataproc_cluster" {
  value = local.deploy_consumer ? google_dataproc_cluster.streaming[0].name : null
}
output "producer_url" {
  value = local.deploy_producer ? google_cloud_run_v2_service.producer[0].uri : null
}
output "pipeline_failure_alert" {
  value = google_monitoring_alert_policy.pipeline_falha.display_name
}
