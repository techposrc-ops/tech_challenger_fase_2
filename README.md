# Tech Challenge - Pipeline de Alfabetização

O projeto implementa uma pipeline híbrida baseada na Arquitetura Medalhão. Os fluxos Batch e
Streaming foram validados no Google Cloud, usando BigQuery, Cloud Storage, Dataproc, Cloud Run e
Managed Service for Apache Kafka.

## Contexto

O projeto utiliza o conjunto
[Avaliação da Alfabetização](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72),
do INEP, disponibilizado pela Base dos Dados. O objetivo é integrar metas, resultados territoriais
e dados educacionais para apoiar análises sobre a alfabetização no Brasil.

## Conteúdo publicado hoje

Esta versão contém:

- código Python do pipeline Batch;
- ingestão das seis tabelas do conjunto de alfabetização;
- camadas Bronze, Silver e Gold em Parquet;
- validações de colunas, chaves, duplicidades e percentuais;
- integração de metas, resultados por UF e município e dados agregados dos alunos;
- arquivos originais para desenvolvimento local;
- quatro notebooks de exploração e construção das camadas;
- simulador e produtor de eventos Kafka;
- consumidor com PySpark Structured Streaming;
- proposta de infraestrutura GCP com Terraform;
- documentação da arquitetura, dos dados e do cronograma da entrega.

## Fontes utilizadas

O fluxo integrado processa as tabelas públicas de
`basedosdados.br_inep_avaliacao_alfabetizacao`:

- `uf`;
- `meta_alfabetizacao_brasil`;
- `meta_alfabetizacao_uf`;
- `meta_alfabetizacao_municipio`;
- `municipio`;
- `alunos`.

## Arquitetura Medalhão

```text
Base dos Dados ou CSV
          |
          v
 Bronze - dados recebidos e metadados de ingestão
          |
          v
 Silver - limpeza, tipos, chaves e validações
          |
          v
 Gold - metas e indicadores integrados
```

- **Bronze:** preserva os dados recebidos e adiciona informações da ingestão.
- **Silver:** padroniza textos e tipos, remove duplicidades e valida chaves e percentuais.
- **Gold:** cria metas normalizadas e indicadores anuais por UF e município, incluindo informações
  agregadas dos alunos.

Os resultados gerados em `data/bronze`, `data/silver` e `data/gold` permanecem fora do Git e podem
ser reproduzidos pela execução do pipeline.

## Estrutura publicada

```text
cloud/gcp/                 adaptação proposta para serviços do Google Cloud
data/arquivos_raw/         arquivos usados somente no desenvolvimento local
docs/                      arquitetura, dicionário de dados e cronograma
infrastructure/terraform/  infraestrutura GCP declarada como código
notebooks/                 exploração da Bronze e análise integrada
src/alfabetizacao/         código-fonte Batch e streaming
.env.example               exemplo de variável para consulta ao BigQuery
.gitignore                 arquivos que não devem ser publicados
pyproject.toml             dependências e comandos do projeto
```

## Organização didática do código

O código utiliza funções pequenas, nomes em português e dicionários simples. Para acompanhar o
fluxo, a ordem recomendada é:

1. `src/alfabetizacao/catalog.py`: configurações das seis tabelas.
2. `src/alfabetizacao/batch/bronze.py`: leitura dos arquivos locais ou do BigQuery.
3. `src/alfabetizacao/batch/silver.py`: limpeza, validação e separação dos inválidos.
4. `src/alfabetizacao/batch/gold.py`: integração das fontes e criação dos indicadores.
5. `src/alfabetizacao/batch/orquestracao.py`: execução sequencial das três camadas.

## Preparação do ambiente

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

O projeto requer Python 3.11 ou superior.

## Execução com as amostras

Na raiz do projeto:

```powershell
python -m alfabetizacao.batch.orquestracao --ambiente local --pasta-raw data/arquivos_raw --pasta-base data
```

Produtos Gold gerados:

```text
data/gold/indicadores_brasil
data/gold/indicadores_uf
data/gold/indicadores_municipio
```

## Consulta da Base dos Dados

Para consultar as fontes públicas no BigQuery:

```powershell
gcloud auth application-default login
$env:GCP_BILLING_PROJECT_ID="seu-projeto-gcp"
python -m alfabetizacao.batch.orquestracao --ambiente cloud --pasta-base gs://SEU_BUCKET --projeto-faturamento SEU_PROJETO
```

A consulta considera registros a partir de 2023. O projeto GCP informado é usado para processar e
faturar as consultas do BigQuery.

## Notebooks publicados

Instale as dependências e abra o JupyterLab:

```powershell
python -m pip install -e ".[notebooks]"
python -m jupyter lab
```

- `notebooks/analise_exploratoria.ipynb`: explora os arquivos originais;
- `notebooks/construcao_bronze.ipynb`: demonstra a criação da Bronze;
- `notebooks/construcao_silver.ipynb`: demonstra tratamentos e validações;
- `notebooks/construcao_gold.ipynb`: demonstra indicadores e comparação com as metas.

## Streaming disponível no código

Para gerar eventos localmente:

```powershell
alfabetizacao-stream-simulator --quantity 100 --output streaming/events/uf.jsonl
```

O projeto também contém:

- produtor Kafka em `src/alfabetizacao/streaming/producer.py`;
- consumidor PySpark em `src/alfabetizacao/streaming/spark_job.py`;
- empacotamento do produtor para Cloud Run;
- script de submissão do job ao Dataproc;
- exemplo de orquestração com Workflows.

O fluxo foi validado ponta a ponta na GCP com 10 eventos publicados, consumidos e gravados como
10 registros Parquet na Bronze de Streaming.

## Infraestrutura GCP

O Terraform propõe:

- VPC e sub-rede privada;
- Cloud Storage;
- dataset BigQuery;
- service accounts e IAM;
- Kafka gerenciado e Dataproc opcionais;
- Cloud Run, Scheduler e Workflows opcionais;
- orçamento configurável.

Kafka, produtor, consumidor e orquestração possuem controles independentes no Terraform. Todos
ficam desativados por padrão. A infraestrutura foi validada com `terraform apply`, e os recursos
temporários do Streaming foram removidos depois do teste.

## FinOps e otimização de custos

O Batch usa Dataproc Serverless para pagar somente durante a execução. O Streaming usa Kafka,
Cloud Run e Dataproc somente quando as variáveis correspondentes são ativadas. Cloud Run escala
para zero, os dados são gravados em Parquet e as camadas ficam na mesma região dos serviços.

O job Batch consumiu `2.002.650 milliDCU-seconds` e `202.800 GiB-seconds` de shuffle. Com os preços
de referência de `us-central1`, seu custo foi estimado em **US$ 0,0365**. Considerando também uma
hora de Kafka e meia hora do cluster Dataproc, o teste completo foi estimado em aproximadamente
**US$ 0,43**, sem impostos, câmbio, rede e operações de armazenamento.

O Terraform configura orçamento com alertas em 50%, 90% e 100%. Checkpoints são removidos após
sete dias e a Bronze muda para Nearline após trinta dias. Os detalhes, fórmulas e o script de
cálculo estão em [`docs/finops.md`](docs/finops.md).

## Observabilidade

Os pipelines registram eventos JSON de início e fim, duração, status e quantidade de registros. O
Batch também registra a duração e o volume de cada camada e a quantidade rejeitada na Silver.

O Terraform declara métricas de sucesso e falha no Cloud Logging e uma política de alerta no Cloud
Monitoring. O canal de e-mail é opcional e configurado pela variável `alert_email`. Consultas,
campos e exemplo de log estão em [`docs/observabilidade.md`](docs/observabilidade.md).

## Validações realizadas localmente

Antes desta publicação, o pipeline foi executado com as seis amostras, os notebooks foram
executados e as verificações locais existentes foram aprovadas. A pasta local de testes foi
retirada do conjunto publicado nesta versão e está listada no `.gitignore`.

O código publicado ainda pode ser verificado com:

```powershell
ruff check .
alfabetizacao-pipeline --source-dir data/sample --output-dir data
```

## Limitações desta versão

- painéis visuais de observabilidade ainda podem ser adicionados no Cloud Monitoring;
- a estimativa FinOps deve ser revisada sempre que os preços da GCP mudarem;
- o vídeo executivo ainda precisa ser preparado.

## Próximas entregas

O cronograma até 01/09/2026 está em [`docs/status_entrega.md`](docs/status_entrega.md). As próximas
prioridades são revisão final da documentação, observabilidade e vídeo executivo.
