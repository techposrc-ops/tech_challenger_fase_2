# FinOps e controle de custos

## Objetivo

O projeto usa recursos temporários para evitar cobrança contínua. O Batch usa Dataproc Serverless,
que encerra os recursos ao terminar. Kafka, Cloud Run, Dataproc Streaming e Cloud NAT são ativados
por variáveis separadas no Terraform e foram removidos depois do teste ponta a ponta.

## Evidência do teste Batch

O job `alfabetizacao-batch-20260901-003629` terminou com estado `SUCCEEDED` e informou:

- `2.002.650 milliDCU-seconds`;
- `202.800 GiB-seconds` de shuffle;
- duração total aproximada de 3 minutos e 53 segundos.

Usando os preços de referência de `us-central1`:

```text
DCU-horas = 2.002.650 / 1.000 / 3.600 = 0,5563
Custo DCU = 0,5563 * US$ 0,06 = US$ 0,0334

Shuffle GiB-horas = 202.800 / 3.600 = 56,3333
Custo shuffle = 56,3333 * US$ 0,000054795 = US$ 0,0031

Estimativa Batch = US$ 0,0365
```

## Estimativa do teste Streaming

Para uma estimativa conservadora foram consideradas uma hora de Kafka e meia hora de cluster
Dataproc. O valor real depende do tempo faturado pela GCP.

| Componente | Hipótese | Estimativa |
|---|---:|---:|
| Kafka gerenciado | 3 vCPU, 3 GiB RAM, 300 GiB local, 1 hora | US$ 0,2589 |
| Dataproc Streaming | 3 VMs `e2-standard-2`, 0,5 hora | US$ 0,1305 |
| Dataproc Serverless Batch | consumo informado pelo job | US$ 0,0365 |
| Cloud Run | uma chamada, escala mínima igual a zero | dentro da faixa gratuita esperada |
| Cloud Storage | aproximadamente 0,13 GiB | cerca de US$ 0,0026/mês |

O total computacional estimado do teste é de aproximadamente **US$ 0,43**, sem impostos, câmbio,
rede, discos e operações de armazenamento. Esse valor é uma estimativa acadêmica, não substitui o
relatório de faturamento da GCP.

## BigQuery

O projeto lê somente as colunas necessárias da fonte pública e publica a Gold como tabelas externas.
Consultas sob demanda têm franquia mensal e são cobradas por bytes processados depois dessa faixa.
Para evitar leituras desnecessárias:

- selecionar apenas colunas usadas pelo pipeline;
- filtrar anos na origem quando possível;
- consultar a Gold em vez dos microdados de alunos;
- usar a estimativa de bytes do BigQuery antes de consultas grandes;
- configurar limite de bytes faturados em consultas de exploração.

## Decisões que reduzem custos

- região única `us-central1` para processamento e armazenamento;
- Parquet, compressão e partições nas camadas do Data Lake;
- Dataproc Serverless no Batch, sem cluster ocioso;
- Cloud Run com mínimo de zero instâncias;
- Kafka e Dataproc Streaming desativados por padrão;
- variáveis Terraform independentes para Kafka, produtor, consumidor e orquestração;
- regra de exclusão de checkpoints após sete dias;
- migração da Bronze para Nearline depois de trinta dias;
- tabelas externas no BigQuery, evitando uma cópia adicional da Gold;
- orçamento mensal com alertas em 50%, 90% e 100%;
- remoção dos recursos temporários imediatamente após a validação.

## Como repetir a estimativa

```powershell
.\.venv\Scripts\python.exe scripts\estimar_custos.py
```

Os valores podem ser alterados pelos argumentos mostrados em:

```powershell
.\.venv\Scripts\python.exe scripts\estimar_custos.py --help
```

Os preços devem ser revisados antes de uma nova execução, pois podem mudar.
