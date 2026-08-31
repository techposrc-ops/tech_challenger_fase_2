from pyspark.sql.functions import col, current_timestamp, max, trim, when

CHAVES_TABELAS = {
    "alunos": ["ano", "id_aluno"],
    "meta_alfabetizacao_brasil": ["ano", "rede"],
    "meta_alfabetizacao_municipio": ["ano", "id_municipio", "rede"],
    "meta_alfabetizacao_uf": ["ano", "sigla_uf", "rede"],
    "municipio": ["ano", "id_municipio", "serie", "rede"],
    "uf": ["ano", "sigla_uf", "serie", "rede"],
}

COLUNAS_INTEIRAS = [
    "ano",
    "caderno",
    "serie",
    "presenca",
    "preenchimento_caderno",
    "alfabetizado",
    "nivel_alfabetizacao",
]

COLUNAS_DECIMAIS = [
    "proficiencia",
    "peso_aluno",
    "taxa_alfabetizacao",
    "media_portugues",
    "percentual_participacao",
    "meta_alfabetizacao_2024",
    "meta_alfabetizacao_2025",
    "meta_alfabetizacao_2026",
    "meta_alfabetizacao_2027",
    "meta_alfabetizacao_2028",
    "meta_alfabetizacao_2029",
    "meta_alfabetizacao_2030",
    "proporcao_aluno_nivel_0",
    "proporcao_aluno_nivel_1",
    "proporcao_aluno_nivel_2",
    "proporcao_aluno_nivel_3",
    "proporcao_aluno_nivel_4",
    "proporcao_aluno_nivel_5",
    "proporcao_aluno_nivel_6",
    "proporcao_aluno_nivel_7",
    "proporcao_aluno_nivel_8",
]


def juntar_caminho(pasta, nome):
    return f"{str(pasta).rstrip('/')}/{nome}"


def tratar_tabela(nome_tabela, dataframe):
    """regras de limpeza definidas no notebook Silver."""
    for nome_coluna, tipo_coluna in dataframe.dtypes:
        if tipo_coluna == "string":
            dataframe = dataframe.withColumn(
                nome_coluna, trim(col(nome_coluna)))

    if "rede" in dataframe.columns:
        dataframe = dataframe.withColumn(
            "rede",
            when(col("rede") == "P�blica", "Pública").otherwise(col("rede")),
        )

    for nome_coluna in COLUNAS_INTEIRAS:
        if nome_coluna in dataframe.columns:
            dataframe = dataframe.withColumn(
                nome_coluna,
                col(nome_coluna).cast("integer"),
            )

    for nome_coluna in COLUNAS_DECIMAIS:
        if nome_coluna in dataframe.columns:
            dataframe = dataframe.withColumn(
                nome_coluna,
                col(nome_coluna).cast("double"),
            )

    if "_data_ingestao" in dataframe.columns:
        dataframe = dataframe.withColumn(
            "_data_ingestao",
            col("_data_ingestao").cast("timestamp"),
        )

    return (
        dataframe.dropDuplicates(CHAVES_TABELAS[nome_tabela])
        .withColumn("_data_tratamento", current_timestamp())
    )


def validar_tabela(nome_tabela, dataframe):
    chaves = CHAVES_TABELAS[nome_tabela]
    total = dataframe.count()
    total_sem_duplicidade = dataframe.dropDuplicates(chaves).count()

    filtro_nulos = col(chaves[0]).isNull()
    for chave in chaves[1:]:
        filtro_nulos = filtro_nulos | col(chave).isNull()

    return {
        "registros": total,
        "duplicidades": total - total_sem_duplicidade,
        "chaves_nulas": dataframe.filter(filtro_nulos).count(),
    }


def ler_carga_mais_recente(spark, pasta_bronze, nome_tabela):
    caminho = juntar_caminho(
        pasta_bronze,
        f"{nome_tabela}/id_ingestao=*",
    )
    dataframe = spark.read.parquet(caminho)

    if "_data_ingestao" not in dataframe.columns:
        return dataframe

    data_mais_recente = dataframe.agg(
        max("_data_ingestao").alias("data_mais_recente")
    ).collect()[0]["data_mais_recente"]

    return dataframe.filter(col("_data_ingestao") == data_mais_recente)


def executar(spark, pasta_bronze, pasta_silver):
    resultado = {}

    for nome_tabela in CHAVES_TABELAS:
        dataframe_bronze = ler_carga_mais_recente(
            spark,
            pasta_bronze,
            nome_tabela,
        )
        dataframe_silver = tratar_tabela(nome_tabela, dataframe_bronze)
        caminho_saida = juntar_caminho(pasta_silver, nome_tabela)

        dataframe_silver.write.mode("overwrite").parquet(caminho_saida)

        resultado[nome_tabela] = {
            "caminho": caminho_saida,
            **validar_tabela(nome_tabela, dataframe_silver),
        }

    return resultado
