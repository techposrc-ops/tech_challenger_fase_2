"""Execução sequencial das camadas Bronze, Silver e Gold."""

import argparse
import os

from alfabetizacao.batch import bronze, gold, silver


def juntar_caminho(pasta, nome):

    return f"{str(pasta).rstrip('/')}/{nome}"


def ler_argumentos(argumentos=None):
    """Lê os parâmetros usados na execução local ou cloud."""
    analisador = argparse.ArgumentParser(
        description="Executa as camadas Bronze, Silver e Gold.",
    )
    analisador.add_argument(
        "--ambiente",
        choices=["local", "cloud"],
        default=os.getenv("AMBIENTE_EXECUCAO", "local"),
    )
    analisador.add_argument(
        "--pasta-base",
        default=os.getenv("PASTA_BASE_DADOS", "data"),
    )
    analisador.add_argument(
        "--pasta-raw",
        default=os.getenv("PASTA_RAW", "data/arquivos_raw"),
    )
    analisador.add_argument(
        "--projeto-faturamento",
        default=os.getenv("GCP_BILLING_PROJECT_ID"),
    )
    parametros = analisador.parse_args(argumentos)

    if parametros.ambiente == "cloud":
        parametros.origem = "bigquery"
        parametros.pasta_raw = None

        if not parametros.pasta_base.startswith("gs://"):
            analisador.error(
                "No ambiente cloud, --pasta-base deve começar com gs://"
            )
    else:
        parametros.origem = "local"

        if not parametros.pasta_raw:
            analisador.error(
                "No ambiente local, informe --pasta-raw"
            )

    return parametros


def executar_pipeline(
    spark,
    pasta_base,
    origem="local",
    pasta_raw=None,
    projeto_faturamento=None,
):

    pasta_bronze = juntar_caminho(pasta_base, "bronze")
    pasta_silver = juntar_caminho(pasta_base, "silver")
    pasta_gold = juntar_caminho(pasta_base, "gold")

    resultado_bronze = bronze.executar(
        spark=spark,
        pasta_raw=pasta_raw,
        pasta_bronze=pasta_bronze,
        origem=origem,
        projeto_faturamento=projeto_faturamento,
    )
    resultado_silver = silver.executar(
        spark=spark,
        pasta_bronze=pasta_bronze,
        pasta_silver=pasta_silver,
    )
    resultado_gold = gold.executar(
        spark=spark,
        pasta_silver=pasta_silver,
        pasta_gold=pasta_gold,
    )

    return {
        "bronze": resultado_bronze,
        "silver": resultado_silver,
        "gold": resultado_gold,
    }


def principal(argumentos=None):
    """Cria a sessão Spark e inicia o pipeline Batch."""
    from pyspark.sql import SparkSession

    parametros = ler_argumentos(argumentos)
    spark = (
        SparkSession.builder
        .appName("pipeline-batch-alfabetizacao")
        .getOrCreate()
    )

    try:
        resultado = executar_pipeline(
            spark=spark,
            pasta_base=parametros.pasta_base,
            origem=parametros.origem,
            pasta_raw=parametros.pasta_raw,
            projeto_faturamento=parametros.projeto_faturamento,
        )

        for camada, tabelas in resultado.items():
            print(f"Camada {camada}: {len(tabelas)} resultados")
    finally:
        spark.stop()


if __name__ == "__main__":
    principal()
