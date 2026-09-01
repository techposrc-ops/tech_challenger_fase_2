"""Construção da camada Bronze em batch com PySpark."""

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pyspark.sql.functions import input_file_name, lit

FONTES = {
    "uf": "br_inep_avaliacao_alfabetizacao_uf.csv.gz",
    "meta_alfabetizacao_brasil": (
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv.gz"
    ),
    "meta_alfabetizacao_uf": (
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv.gz"
    ),
    "meta_alfabetizacao_municipio": (
        "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv.gz"
    ),
    "municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv.gz",
    "alunos": "br_inep_avaliacao_alfabetizacao_aluno.csv.gz",
}

PROJETO_FONTE = "basedosdados"
CONJUNTO_FONTE = "br_inep_avaliacao_alfabetizacao"

TABELAS_BIGQUERY = {
    "uf": "uf",
    "meta_alfabetizacao_brasil": "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf": "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio": "meta_alfabetizacao_municipio",
    "municipio": "municipio",
    "alunos": "alunos",
}


def conferir_arquivos(pasta_raw):
    pasta_raw = Path(pasta_raw)

    for nome_arquivo in FONTES.values():
        caminho = pasta_raw / nome_arquivo
        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")


def ler_arquivo_bronze(
    spark,
    nome_tabela,
    caminho_arquivo,
    id_ingestao,
    data_ingestao,
):
    """Adiciona os metadados da Bronze."""
    dataframe_raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .csv(Path(caminho_arquivo).as_posix())
    )

    return (
        dataframe_raw.withColumn("_id_ingestao", lit(id_ingestao))
        .withColumn("_data_ingestao", lit(data_ingestao))
        .withColumn("_arquivo_origem", input_file_name())
        .withColumn("_tabela_origem", lit(nome_tabela))
    )


def ler_tabela_bigquery(
    spark,
    nome_tabela,
    id_ingestao,
    data_ingestao,
    projeto_faturamento,
):

    projeto_faturamento = projeto_faturamento or os.getenv(
        "GCP_BILLING_PROJECT_ID"
    )

    if not projeto_faturamento:
        raise ValueError(
            "Informe o projeto GCP ou defina GCP_BILLING_PROJECT_ID."
        )

    tabela_bigquery = TABELAS_BIGQUERY[nome_tabela]
    tabela_completa = f"{PROJETO_FONTE}.{CONJUNTO_FONTE}.{tabela_bigquery}"

    dataframe_raw = (
        spark.read.format("bigquery")
        .option("table", tabela_completa)
        .option("parentProject", projeto_faturamento)
        .load()
    )

    return (
        dataframe_raw.withColumn("_id_ingestao", lit(id_ingestao))
        .withColumn("_data_ingestao", lit(data_ingestao))
        .withColumn("_arquivo_origem", lit(tabela_completa))
        .withColumn("_tabela_origem", lit(nome_tabela))
    )


def montar_caminho_saida(pasta_bronze, nome_tabela, id_ingestao):
    pasta_bronze = str(pasta_bronze)

    if "://" in pasta_bronze:
        return f"{pasta_bronze.rstrip('/')}/{nome_tabela}/id_ingestao={id_ingestao}"

    return (Path(pasta_bronze) / nome_tabela / f"id_ingestao={id_ingestao}").as_posix()


def executar(
    spark,
    pasta_raw,
    pasta_bronze,
    origem="local",
    projeto_faturamento=None,
):
    """grava os Parquets da camada Bronze."""
    if origem not in {"local", "bigquery"}:
        raise ValueError("A origem deve ser 'local' ou 'bigquery'.")

    if origem == "local":
        pasta_raw = Path(pasta_raw)
        conferir_arquivos(pasta_raw)

    id_ingestao = str(uuid4())
    data_ingestao = datetime.now(UTC).isoformat(timespec="seconds")
    resultado = {}

    for nome_tabela, nome_arquivo in FONTES.items():
        if origem == "local":
            dataframe = ler_arquivo_bronze(
                spark,
                nome_tabela,
                pasta_raw / nome_arquivo,
                id_ingestao,
                data_ingestao,
            )
        else:
            dataframe = ler_tabela_bigquery(
                spark,
                nome_tabela,
                id_ingestao,
                data_ingestao,
                projeto_faturamento,
            )

        caminho_saida = montar_caminho_saida(
            pasta_bronze,
            nome_tabela,
            id_ingestao,
        )
        dataframe.write.mode("overwrite").parquet(caminho_saida)

        resultado[nome_tabela] = {
            "caminho": caminho_saida,
            "registros": dataframe.count(),
        }

    return resultado
