"""Execução sequencial das camadas Bronze, Silver e Gold."""

from alfabetizacao.jobs import bronze, gold, silver


def juntar_caminho(pasta, nome):

    return f"{str(pasta).rstrip('/')}/{nome}"


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
