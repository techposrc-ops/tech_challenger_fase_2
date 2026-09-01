provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

data "google_project" "current" {
  project_id = var.gcp_project_id
}

locals {
  prefix      = "${var.project_name}-${var.environment}"
  bucket_name = "${local.prefix}-${data.google_project.current.number}"
  create_kafka = (
    var.enable_streaming
    || var.enable_kafka
  )
  create_streaming_runtime = (
    var.enable_streaming
    || var.enable_streaming_producer
    || var.enable_streaming_consumer
  )
  deploy_producer = (
    (var.enable_streaming || var.enable_streaming_producer)
    && local.create_kafka
    && var.producer_image != null
    && var.kafka_bootstrap_servers != null
  )
  deploy_consumer = (
    var.enable_streaming
    || var.enable_streaming_consumer
  )
  deploy_orchestration = (
    (var.enable_streaming || var.enable_streaming_orchestration)
    && local.deploy_producer
  )
  labels = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
  tabelas_gold = toset([
    "indicadores_brasil",
    "indicadores_uf",
    "indicadores_municipio"
  ])
  batch_apis = toset([
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "compute.googleapis.com",
    "dataproc.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com"
  ])
  streaming_apis = toset([
    "managedkafka.googleapis.com",
  ])
  producer_apis = toset([
    "run.googleapis.com",
  ])
  orchestration_apis = toset([
    "cloudscheduler.googleapis.com",
    "workflows.googleapis.com"
  ])
  billing_apis = toset([
    "billingbudgets.googleapis.com"
  ])
  apis = setunion(
    local.batch_apis,
    local.create_kafka ? local.streaming_apis : toset([]),
    local.deploy_producer ? local.producer_apis : toset([]),
    local.deploy_orchestration ? local.orchestration_apis : toset([]),
    var.billing_account_id != null ? local.billing_apis : toset([])
  )
}

resource "google_project_service" "required" {
  for_each           = local.apis
  project            = var.gcp_project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "docker" {
  location      = var.gcp_region
  repository_id = var.artifact_registry_repository
  description   = "Imagens Docker do projeto de alfabetização."
  format        = "DOCKER"
  labels        = local.labels

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "cloud_build_builder" {
  project = var.gcp_project_id
  role    = "roles/cloudbuild.builds.builder"
  member  = "serviceAccount:${data.google_project.current.number}-compute@developer.gserviceaccount.com"

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket" "data_lake" {
  name                        = local.bucket_name
  location                    = var.gcp_region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.labels
  versioning { enabled = true }
  lifecycle_rule {
    condition {
      matches_prefix = ["checkpoints/"]
      age            = 7
    }
    action { type = "Delete" }
  }
  lifecycle_rule {
    condition {
      matches_prefix = ["bronze/"]
      age            = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  depends_on = [google_project_service.required]
}

resource "google_bigquery_dataset" "alfabetizacao" {
  dataset_id                 = replace(local.prefix, "-", "_")
  friendly_name              = "Indicadores de alfabetização"
  location                   = var.gcp_region
  delete_contents_on_destroy = false
  labels                     = local.labels
  depends_on                 = [google_project_service.required]
}

resource "google_bigquery_table" "gold_externa" {
  for_each            = local.tabelas_gold
  dataset_id          = google_bigquery_dataset.alfabetizacao.dataset_id
  table_id            = each.value
  description         = "Tabela Gold em Parquet armazenada no Cloud Storage."
  deletion_protection = false
  labels              = local.labels

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris = [
      "gs://${google_storage_bucket.data_lake.name}/gold/${each.value}/*.parquet"
    ]
  }
}

resource "google_compute_network" "data" {
  name                    = "${local.prefix}-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.required]
}

resource "google_compute_subnetwork" "data" {
  name                     = "${local.prefix}-subnet"
  region                   = var.gcp_region
  network                  = google_compute_network.data.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true
}

resource "google_compute_firewall" "dataproc_internal" {
  name               = "${local.prefix}-dataproc-internal"
  network            = google_compute_network.data.name
  direction          = "INGRESS"
  source_ranges      = [var.subnet_cidr]
  destination_ranges = [var.subnet_cidr]

  allow {
    protocol = "all"
  }
}

resource "google_service_account" "batch" {
  account_id   = substr("${local.prefix}-batch", 0, 30)
  display_name = "Pipeline Batch Bronze Silver Gold"
  depends_on   = [google_project_service.required]
}

resource "google_project_iam_member" "batch_roles" {
  for_each = toset([
    "roles/bigquery.jobUser",
    "roles/bigquery.readSessionUser",
    "roles/dataproc.worker",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter"
  ])
  project = var.gcp_project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.batch.email}"
}

resource "google_storage_bucket_iam_member" "batch_data_lake" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.batch.email}"
}

resource "google_bigquery_dataset_iam_member" "batch_editor" {
  dataset_id = google_bigquery_dataset.alfabetizacao.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.batch.email}"
}

resource "google_service_account" "streaming" {
  count        = local.create_streaming_runtime ? 1 : 0
  account_id   = substr("${local.prefix}-stream", 0, 30)
  display_name = "Pipeline de streaming"
}

resource "google_project_iam_member" "streaming_roles" {
  for_each = local.create_streaming_runtime ? setunion(
    toset([
      "roles/managedkafka.client"
    ]),
    local.deploy_consumer ? toset([
      "roles/dataproc.worker",
      "roles/logging.logWriter",
      "roles/monitoring.metricWriter"
    ]) : toset([])
  ) : toset([])
  project = var.gcp_project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.streaming[0].email}"
}

resource "google_service_account_iam_member" "streaming_token_self" {
  for_each = local.create_streaming_runtime ? toset([
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.serviceAccountOpenIdTokenCreator"
  ]) : toset([])

  service_account_id = google_service_account.streaming[0].name
  role               = each.value
  member             = "serviceAccount:${google_service_account.streaming[0].email}"
}

resource "google_storage_bucket_iam_member" "streaming_data_lake" {
  count  = local.deploy_consumer ? 1 : 0
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.streaming[0].email}"
}

resource "google_managed_kafka_cluster" "streaming" {
  count      = local.create_kafka ? 1 : 0
  cluster_id = "${local.prefix}-kafka"
  location   = var.gcp_region
  labels     = local.labels
  capacity_config {
    vcpu_count   = var.kafka_vcpu_count
    memory_bytes = var.kafka_memory_bytes
  }
  gcp_config {
    access_config {
      network_configs { subnet = google_compute_subnetwork.data.id }
    }
  }
  depends_on = [google_project_service.required]
}

resource "google_managed_kafka_topic" "alfabetizacao" {
  count              = local.create_kafka ? 1 : 0
  topic_id           = var.kafka_topic
  cluster            = google_managed_kafka_cluster.streaming[0].cluster_id
  location           = var.gcp_region
  partition_count    = var.kafka_partition_count
  replication_factor = 3
}

resource "google_dataproc_cluster" "streaming" {
  count  = local.deploy_consumer ? 1 : 0
  name   = "${local.prefix}-spark"
  region = var.gcp_region
  labels = local.labels
  cluster_config {
    staging_bucket = google_storage_bucket.data_lake.name
    gce_cluster_config {
      subnetwork       = google_compute_subnetwork.data.id
      service_account  = google_service_account.streaming[0].email
      internal_ip_only = true
    }
    master_config {
      num_instances = 1
      machine_type  = var.dataproc_machine_type
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 50
      }
    }
    worker_config {
      num_instances = 2
      machine_type  = var.dataproc_machine_type
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 50
      }
    }
    software_config { image_version = var.dataproc_image_version }
  }
  depends_on = [google_project_iam_member.streaming_roles]
}

resource "google_compute_router" "streaming" {
  count   = local.deploy_consumer ? 1 : 0
  name    = "${local.prefix}-streaming-router"
  region  = var.gcp_region
  network = google_compute_network.data.id
}

resource "google_compute_router_nat" "streaming" {
  count                              = local.deploy_consumer ? 1 : 0
  name                               = "${local.prefix}-streaming-nat"
  router                             = google_compute_router.streaming[0].name
  region                             = var.gcp_region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  subnetwork {
    name                    = google_compute_subnetwork.data.id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }
}

resource "google_service_account" "orchestrator" {
  count        = local.deploy_orchestration ? 1 : 0
  account_id   = substr("${local.prefix}-orchestrator", 0, 30)
  display_name = "Agendamento e orquestração"
}

resource "google_cloud_run_v2_service" "producer" {
  count               = local.deploy_producer ? 1 : 0
  name                = "${local.prefix}-producer"
  location            = var.gcp_region
  deletion_protection = false
  template {
    service_account = google_service_account.streaming[0].email
    containers {
      image = var.producer_image
      env {
        name  = "KAFKA_BOOTSTRAP_SERVERS"
        value = var.kafka_bootstrap_servers
      }
      env {
        name  = "KAFKA_TOPIC"
        value = var.kafka_topic
      }
      env {
        name  = "EVENTS_PER_INVOCATION"
        value = tostring(var.events_per_invocation)
      }
    }
    vpc_access {
      network_interfaces {
        network    = google_compute_network.data.name
        subnetwork = google_compute_subnetwork.data.name
      }
      egress = "PRIVATE_RANGES_ONLY"
    }
  }
  depends_on = [google_managed_kafka_topic.alfabetizacao]
}

resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  count    = local.deploy_orchestration ? 1 : 0
  project  = var.gcp_project_id
  location = var.gcp_region
  name     = google_cloud_run_v2_service.producer[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.orchestrator[0].email}"
}

resource "google_cloud_scheduler_job" "producer" {
  count     = local.deploy_orchestration ? 1 : 0
  name      = "${local.prefix}-producer"
  region    = var.gcp_region
  schedule  = "0 * * * *"
  time_zone = "America/Sao_Paulo"
  http_target {
    http_method = "POST"
    uri         = google_cloud_run_v2_service.producer[0].uri
    oidc_token {
      service_account_email = google_service_account.orchestrator[0].email
      audience              = google_cloud_run_v2_service.producer[0].uri
    }
  }
  depends_on = [google_cloud_run_v2_service_iam_member.scheduler_invoker]
}

resource "google_workflows_workflow" "pipeline" {
  count           = local.deploy_orchestration ? 1 : 0
  name            = "${local.prefix}-pipeline"
  region          = var.gcp_region
  service_account = google_service_account.orchestrator[0].email
  source_contents = file("${path.module}/../../cloud/gcp/workflows/pipeline.yaml")
  depends_on      = [google_project_service.required]
}

resource "google_billing_budget" "monthly" {
  count           = var.billing_account_id == null ? 0 : 1
  billing_account = var.billing_account_id
  display_name    = "${local.prefix}-monthly-budget"
  budget_filter { projects = ["projects/${data.google_project.current.number}"] }
  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.monthly_budget_usd)
    }
  }
  threshold_rules { threshold_percent = 0.5 }
  threshold_rules { threshold_percent = 0.9 }
  threshold_rules { threshold_percent = 1.0 }
}

resource "google_logging_metric" "pipeline_sucesso" {
  name        = "${local.prefix}-pipeline-sucesso"
  description = "Quantidade de execuções finalizadas com sucesso."
  filter      = "jsonPayload.tipo=\"pipeline_fim\" AND jsonPayload.status=\"sucesso\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

resource "google_logging_metric" "pipeline_falha" {
  name        = "${local.prefix}-pipeline-falha"
  description = "Quantidade de execuções finalizadas com falha."
  filter      = "jsonPayload.tipo=\"pipeline_fim\" AND jsonPayload.status=\"falha\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

resource "google_monitoring_notification_channel" "email" {
  count        = var.alert_email == null ? 0 : 1
  display_name = "E-mail de alertas do pipeline"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
}

resource "google_monitoring_alert_policy" "pipeline_falha" {
  display_name = "Falha nos pipelines de alfabetização"
  combiner     = "OR"
  enabled      = true

  conditions {
    display_name = "Falha no Batch Serverless"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_falha.name}\" AND resource.type=\"cloud_dataproc_batch\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  conditions {
    display_name = "Falha no Dataproc Streaming"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_falha.name}\" AND resource.type=\"cloud_dataproc_job\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  conditions {
    display_name = "Falha no cluster Dataproc"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_falha.name}\" AND resource.type=\"cloud_dataproc_cluster\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  conditions {
    display_name = "Falha no produtor Cloud Run"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_falha.name}\" AND resource.type=\"cloud_run_revision\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = google_monitoring_notification_channel.email[*].name
  alert_strategy {
    auto_close = "1800s"
  }

  depends_on = [google_project_service.required]
}
