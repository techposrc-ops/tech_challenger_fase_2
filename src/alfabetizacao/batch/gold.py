"""Construção da camada Gold em batch com PySpark."""

from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    round,
    when,
)
from pyspark.sql.functions import sum as soma

TABELAS_SILVER = [
    "alunos",
    "municipio",
    "uf",
    "meta_alfabetizacao_municipio",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_brasil",
]

ANOS_METAS = range(2024, 2031)
COLUNAS_METAS = [f"meta_alfabetizacao_{ano}" for ano in ANOS_METAS]


def juntar_caminho(pasta, nome):
    return f"{str(pasta).rstrip('/')}/{nome}"


def criar_indicadores_municipio(tabelas_silver):
    alunos_municipais = (
        tabelas_silver["alunos"]
        .filter((col("rede") == "3") & (col("presenca") == 1))
        .groupBy("ano", "id_municipio")
        .agg(
            count("*").alias("alunos_presentes"),
            soma("alfabetizado").alias("alunos_alfabetizados"),
            avg("proficiencia").alias("media_proficiencia"),
        )
        .withColumn(
            "taxa_microdados",
            round(col("alunos_alfabetizados") *
                  100 / col("alunos_presentes"), 2),
        )
    )

    resultado_municipio = (
        tabelas_silver["municipio"]
        .filter(col("rede") == "3")
        .select(
            "ano",
            "id_municipio",
            col("taxa_alfabetizacao").alias("taxa_oficial"),
            "media_portugues",
        )
    )

    metas_municipio = tabelas_silver["meta_alfabetizacao_municipio"].select(
        "ano",
        "id_municipio",
        "nivel_alfabetizacao",
        "percentual_participacao",
        *COLUNAS_METAS,
    )

    indicadores_municipio = (
        alunos_municipais.join(
            resultado_municipio,
            ["ano", "id_municipio"],
            "left",
        )
        .join(metas_municipio, ["ano", "id_municipio"], "left")
    )
    return adicionar_comparacao_meta(indicadores_municipio)


def criar_indicadores_uf(tabelas_silver):
    resultado_uf = (
        tabelas_silver["uf"]
        .filter(col("rede") == "5")
        .select(
            "ano",
            "sigla_uf",
            col("taxa_alfabetizacao").alias("taxa_oficial"),
            "media_portugues",
        )
    )

    metas_uf = tabelas_silver["meta_alfabetizacao_uf"].select(
        "ano",
        "sigla_uf",
        "percentual_participacao",
        *COLUNAS_METAS,
    )

    return adicionar_comparacao_meta(
        resultado_uf.join(metas_uf, ["ano", "sigla_uf"], "left")
    )


def criar_indicadores_brasil(tabelas_silver):
    brasil = tabelas_silver["meta_alfabetizacao_brasil"].select(
        "ano",
        col("taxa_alfabetizacao").alias("taxa_oficial"),
        "percentual_participacao",
        *COLUNAS_METAS,
    )
    return adicionar_comparacao_meta(brasil)


def adicionar_comparacao_meta(dataframe):
    meta_do_ano = when(
        col("ano") == 2024,
        col("meta_alfabetizacao_2024"),
    )

    for ano in range(2025, 2031):
        meta_do_ano = meta_do_ano.when(
            col("ano") == ano,
            col(f"meta_alfabetizacao_{ano}"),
        )

    return (
        dataframe.withColumn("meta_do_ano", meta_do_ano)
        .withColumn(
            "diferenca_para_meta",
            round(col("taxa_oficial") - col("meta_do_ano"), 2),
        )
        .withColumn(
            "situacao_meta",
            when(col("meta_do_ano").isNull(), "Sem meta")
            .when(col("taxa_oficial") >= col("meta_do_ano"), "Atingida")
            .otherwise("Não atingida"),
        )
        .withColumn("_data_criacao_gold", current_timestamp())
    )


def criar_tabelas_gold(tabelas_silver):
    return {
        "indicadores_municipio": criar_indicadores_municipio(tabelas_silver),
        "indicadores_uf": criar_indicadores_uf(tabelas_silver),
        "indicadores_brasil": criar_indicadores_brasil(tabelas_silver),
    }


def executar(spark, pasta_silver, pasta_gold):
    tabelas_silver = {}

    for nome_tabela in TABELAS_SILVER:
        caminho = juntar_caminho(pasta_silver, nome_tabela)
        tabelas_silver[nome_tabela] = spark.read.parquet(caminho)

    tabelas_gold = criar_tabelas_gold(tabelas_silver)
    resultado = {}

    for nome_tabela, dataframe in tabelas_gold.items():
        caminho_saida = juntar_caminho(pasta_gold, nome_tabela)
        dataframe.write.mode("overwrite").parquet(caminho_saida)
        resultado[nome_tabela] = {
            "caminho": caminho_saida,
            "registros": dataframe.count(),
        }

    return resultado
