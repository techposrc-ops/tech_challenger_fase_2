# Observabilidade dos pipelines

## Logs estruturados

Os pipelines escrevem uma linha JSON para cada evento importante. Os campos principais são:

- `timestamp`;
- `severity`;
- `tipo` (`pipeline_inicio`, `camada_fim` ou `pipeline_fim`);
- `pipeline`;
- `id_execucao`;
- `status`;
- `duracao_segundos`;
- `quantidade_registros`;
- `quantidade_rejeitados`, quando aplicável;
- `erro`, quando ocorrer uma falha.

Exemplo:

```json
{"severity":"INFO","tipo":"pipeline_fim","pipeline":"batch","status":"sucesso","duracao_segundos":232.84,"quantidade_registros":10324}
```

O mesmo formato é usado pelo Batch, pelo produtor Kafka e pelo consumidor PySpark. No Batch também
é emitido um evento para cada camada da Arquitetura Medalhão.

## Métricas

O Terraform cria duas métricas baseadas nos logs:

- `alfabetizacao-dev-pipeline-sucesso`;
- `alfabetizacao-dev-pipeline-falha`.

Cada ocorrência de `pipeline_fim` aumenta a métrica correspondente. Assim é possível acompanhar o
número de execuções concluídas e com erro no Cloud Monitoring.

## Alerta de falha

A política `Falha nos pipelines de alfabetização` abre um incidente quando a métrica de falha fica
maior que zero para Batch Serverless, Dataproc Streaming ou Cloud Run.

Para receber o alerta por e-mail, informe no `terraform.tfvars`:

```hcl
alert_email = "seu-email@exemplo.com"
```

Depois do `terraform apply`, a GCP pode pedir a confirmação do canal de notificação. Sem o e-mail, a
política continua aparecendo no console, mas não envia notificação externa.

## Consulta no Cloud Logging

Execuções com falha:

```text
jsonPayload.tipo="pipeline_fim"
jsonPayload.status="falha"
```

Execuções finalizadas:

```text
jsonPayload.tipo="pipeline_fim"
```

Os recursos do Terraform criam a estrutura de monitoramento. É necessário executar `terraform
apply` para materializar as métricas e o alerta no projeto GCP.
