output "data_lake_bucket" {
  value = google_storage_bucket.data_lake.name
}
output "bigquery_dataset" {
  value = google_bigquery_dataset.alfabetizacao.dataset_id
}
output "batch_service_account" {
  value = google_service_account.batch.email
}
output "streaming_service_account" {
  value = var.enable_streaming ? google_service_account.streaming[0].email : null
}
output "kafka_cluster" {
  value = var.enable_streaming ? google_managed_kafka_cluster.streaming[0].cluster_id : null
}
output "dataproc_cluster" {
  value = var.enable_streaming ? google_dataproc_cluster.streaming[0].name : null
}
output "producer_url" {
  value = local.deploy_producer ? google_cloud_run_v2_service.producer[0].uri : null
}
