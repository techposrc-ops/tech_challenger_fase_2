"""Execução sequencial das camadas Bronze, Silver e Gold."""

import argparse
import os
import time
from uuid import uuid4

from alfabetizacao.batch import bronze, gold, silver
from alfabetizacao.observabilidade import registrar_evento


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
    id_execucao = str(uuid4())
    inicio_pipeline = time.perf_counter()
    pasta_bronze = juntar_caminho(pasta_base, "bronze")
    pasta_silver = juntar_caminho(pasta_base, "silver")
    pasta_gold = juntar_caminho(pasta_base, "gold")
    registrar_evento(
        "pipeline_inicio",
        "batch",
        id_execucao=id_execucao,
        ambiente=origem,
    )

    try:
        inicio_camada = time.perf_counter()
        resultado_bronze = bronze.executar(
            spark=spark,
            pasta_raw=pasta_raw,
            pasta_bronze=pasta_bronze,
            origem=origem,
            projeto_faturamento=projeto_faturamento,
        )
        registros_bronze = sum(
            tabela.get("registros", 0)
            for tabela in resultado_bronze.values()
            if isinstance(tabela, dict)
        )
        registrar_evento(
            "camada_fim",
            "batch",
            id_execucao=id_execucao,
            camada="bronze",
            status="sucesso",
            duracao_segundos=round(time.perf_counter() - inicio_camada, 2),
            quantidade_registros=registros_bronze,
        )

        inicio_camada = time.perf_counter()
        resultado_silver = silver.executar(
            spark=spark,
            pasta_bronze=pasta_bronze,
            pasta_silver=pasta_silver,
            id_execucao=id_execucao,
        )
        tabelas_silver = [
            tabela
            for nome, tabela in resultado_silver.items()
            if nome in silver.CHAVES_TABELAS
        ]
        registros_silver = sum(
            tabela.get("registros_validos", 0)
            for tabela in tabelas_silver
            if isinstance(tabela, dict)
        )
        rejeitados_silver = sum(
            tabela.get("registros_rejeitados", 0)
            for tabela in tabelas_silver
            if isinstance(tabela, dict)
        )
        registrar_evento(
            "camada_fim",
            "batch",
            id_execucao=id_execucao,
            camada="silver",
            status="sucesso",
            duracao_segundos=round(time.perf_counter() - inicio_camada, 2),
            quantidade_registros=registros_silver,
            quantidade_rejeitados=rejeitados_silver,
        )
        registrar_evento(
            "qualidade_dados",
            "batch",
            id_execucao=id_execucao,
            status=(
                "falha"
                if resultado_silver["qualidade"]["erros_criticos"]
                else "sucesso"
            ),
            quantidade_registros=registros_silver,
            quantidade_rejeitados=rejeitados_silver,
            erros_criticos=resultado_silver["qualidade"]["erros_criticos"],
            caminho_relatorio=resultado_silver["qualidade"]["caminho"],
        )
        silver.validar_erros_criticos(resultado_silver)

        inicio_camada = time.perf_counter()
        resultado_gold = gold.executar(
            spark=spark,
            pasta_silver=pasta_silver,
            pasta_gold=pasta_gold,
        )
        registros_gold = sum(
            tabela.get("registros", 0)
            for tabela in resultado_gold.values()
            if isinstance(tabela, dict)
        )
        registrar_evento(
            "camada_fim",
            "batch",
            id_execucao=id_execucao,
            camada="gold",
            status="sucesso",
            duracao_segundos=round(time.perf_counter() - inicio_camada, 2),
            quantidade_registros=registros_gold,
        )

        resultado = {
            "bronze": resultado_bronze,
            "silver": resultado_silver,
            "gold": resultado_gold,
        }
        registrar_evento(
            "pipeline_fim",
            "batch",
            id_execucao=id_execucao,
            status="sucesso",
            duracao_segundos=round(time.perf_counter() - inicio_pipeline, 2),
            quantidade_registros=registros_gold,
        )
        return resultado
    except Exception as erro:
        registrar_evento(
            "pipeline_fim",
            "batch",
            id_execucao=id_execucao,
            status="falha",
            duracao_segundos=round(time.perf_counter() - inicio_pipeline, 2),
            erro=str(erro),
        )
        raise


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
