# Arquitetura da solução

## Batch

```text
Base dos Dados (BigQuery) ou CSV -> Bronze -> Silver -> Gold
```

## Streaming no Google Cloud

```text
Cloud Scheduler -> Cloud Run -> Kafka gerenciado -> Dataproc/PySpark
                                                       |
                                                       v
                 BigQuery/BigLake <- Cloud Storage (Bronze/Silver/Gold)
```

Workflows coordena acionamentos pontuais; Cloud Logging e Monitoring concentram observabilidade.
O fluxo usa esquema explícito, `event_id`, watermark, deduplicação e checkpoint persistente.

## Portabilidade

- Regras medalhão vivem em `src/alfabetizacao/jobs`.
- Produtor e job PySpark ficam em `src/alfabetizacao/streaming` e recebem configuração externa.
- `cloud/gcp` contém somente adaptadores de implantação.
- Caminhos locais e `gs://` são fornecidos por parâmetros.
- Kafka, Spark e Parquet permanecem padrões abertos.

## Decisões

- Bronze preserva os valores recebidos e metadados da ingestão.
- Silver tipa, normaliza, valida e deduplica.
- Gold documenta suas agregações por ano e território.
- Credenciais não são armazenadas no repositório.

## Organização didática do código

O projeto evita hierarquias de classes e padrões complexos. As configurações ficam em dicionários
e o pipeline segue uma sequência fácil de acompanhar: ler, validar, criar Bronze, criar Silver,
integrar Gold e salvar. Os nomes em inglês que restaram pertencem às bibliotecas ou aos contratos
externos e não devem ser traduzidos no código.
