from uuid import uuid4

from pyspark.sql.functions import col, current_timestamp, lit, max, trim, when

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

COLUNAS_PERCENTUAIS = [
    "taxa_alfabetizacao",
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

UF_POR_CODIGO_IBGE = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}


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


def criar_filtro_chaves_nulas(nome_tabela):
    """Cria a condição usada para localizar chaves nulas."""
    chaves = CHAVES_TABELAS[nome_tabela]
    filtro_nulos = col(chaves[0]).isNull()

    for chave in chaves[1:]:
        filtro_nulos = filtro_nulos | col(chave).isNull()

    return filtro_nulos


def criar_filtro_percentuais_invalidos(dataframe):
    """Cria a condição para percentuais menores que 0 ou maiores que 100."""
    filtro_percentuais = None

    for nome_coluna in COLUNAS_PERCENTUAIS:
        if nome_coluna in dataframe.columns:
            percentual_invalido = (
                (col(nome_coluna) < 0) | (col(nome_coluna) > 100)
            )
            if filtro_percentuais is None:
                filtro_percentuais = percentual_invalido
            else:
                filtro_percentuais = filtro_percentuais | percentual_invalido

    return filtro_percentuais


def adicionar_validacao(nome_tabela, dataframe):
    """Marca registros válidos e informa o motivo dos inválidos."""
    chaves_nulas = criar_filtro_chaves_nulas(nome_tabela)
    percentuais_invalidos = criar_filtro_percentuais_invalidos(dataframe)

    if percentuais_invalidos is None:
        percentuais_invalidos = lit(False)

    motivo = (
        when(
            chaves_nulas & percentuais_invalidos,
            "Chave obrigatória nula; Percentual fora do intervalo 0 a 100",
        )
        .when(chaves_nulas, "Chave obrigatória nula")
        .when(
            percentuais_invalidos,
            "Percentual fora do intervalo 0 a 100",
        )
    )

    return (
        dataframe.withColumn("_motivo_invalido", motivo)
        .withColumn(
            "_registro_valido",
            col("_motivo_invalido").isNull(),
        )
    )


def separar_registros(dataframe):
    """Separa os registros que seguem no pipeline dos rejeitados."""
    registros_validos = dataframe.filter(col("_registro_valido"))
    registros_rejeitados = dataframe.filter(~col("_registro_valido"))
    return registros_validos, registros_rejeitados


def validar_tabela(nome_tabela, dataframe):
    chaves = CHAVES_TABELAS[nome_tabela]
    total = dataframe.count()
    total_sem_duplicidade = dataframe.dropDuplicates(chaves).count()
    filtro_nulos = criar_filtro_chaves_nulas(nome_tabela)
    filtro_percentuais = criar_filtro_percentuais_invalidos(dataframe)

    percentuais_invalidos = 0
    if filtro_percentuais is not None:
        percentuais_invalidos = dataframe.filter(filtro_percentuais).count()

    return {
        "registros": total,
        "duplicidades": total - total_sem_duplicidade,
        "chaves_nulas": dataframe.filter(filtro_nulos).count(),
        "percentuais_invalidos": percentuais_invalidos,
    }


def adicionar_sigla_uf(dataframe):
    """Obtém a UF usando os dois primeiros números do município."""
    codigo_uf = col("id_municipio").substr(1, 2)
    sigla_uf = None

    for codigo, sigla in UF_POR_CODIGO_IBGE.items():
        if sigla_uf is None:
            sigla_uf = when(codigo_uf == codigo, sigla)
        else:
            sigla_uf = sigla_uf.when(codigo_uf == codigo, sigla)

    return dataframe.withColumn("_sigla_uf", sigla_uf)


def validar_relacionamentos(tabelas_silver):
    """Valida alunos com municípios e municípios com UFs."""
    municipios_existentes = tabelas_silver["municipio"].select(
        "ano",
        "id_municipio",
    ).dropDuplicates()

    alunos_sem_municipio = (
        tabelas_silver["alunos"]
        .join(
            municipios_existentes,
            ["ano", "id_municipio"],
            "left_anti",
        )
        .count()
    )

    ufs_existentes = (
        tabelas_silver["uf"]
        .select("ano", "sigla_uf")
        .dropDuplicates()
    )
    municipios_com_uf = adicionar_sigla_uf(
        tabelas_silver["municipio"]
    ).select(
        "ano",
        "id_municipio",
        "_sigla_uf",
    ).dropDuplicates()

    municipios_sem_uf = (
        municipios_com_uf.join(
            ufs_existentes,
            (municipios_com_uf["ano"] == ufs_existentes["ano"])
            & (municipios_com_uf["_sigla_uf"] == ufs_existentes["sigla_uf"]),
            "left_anti",
        )
        .count()
    )

    return {
        "alunos_sem_municipio": alunos_sem_municipio,
        "municipios_sem_uf": municipios_sem_uf,
    }


def criar_relatorio_qualidade(resultado):
    """Organiza as validações em linhas simples para consulta."""
    linhas = []

    for nome_tabela in CHAVES_TABELAS:
        dados = resultado[nome_tabela]
        linhas.append(
            {
                "tabela": nome_tabela,
                "registros": dados["registros"],
                "registros_validos": dados["registros_validos"],
                "registros_rejeitados": dados["registros_rejeitados"],
                "duplicidades": dados["duplicidades"],
                "chaves_nulas": dados["chaves_nulas"],
                "percentuais_invalidos": dados["percentuais_invalidos"],
                "erros_relacionamento": 0,
            }
        )

    relacionamentos = resultado["relacionamentos"]
    linhas.append(
        {
            "tabela": "relacionamentos",
            "registros": 0,
            "registros_validos": 0,
            "registros_rejeitados": 0,
            "duplicidades": 0,
            "chaves_nulas": 0,
            "percentuais_invalidos": 0,
            "erros_relacionamento": sum(relacionamentos.values()),
        }
    )
    return linhas


def encontrar_erros_criticos(resultado):
    """Retorna problemas que impedem a construção segura da Gold."""
    erros = []

    for nome_tabela in CHAVES_TABELAS:
        if nome_tabela not in resultado:
            erros.append(f"Tabela ausente: {nome_tabela}")
        elif resultado[nome_tabela]["registros_validos"] == 0:
            erros.append(f"Tabela sem registros válidos: {nome_tabela}")

    relacionamentos = resultado.get("relacionamentos", {})
    if relacionamentos.get("alunos_sem_municipio", 0) > 0:
        erros.append("Existem alunos sem município correspondente")
    if relacionamentos.get("municipios_sem_uf", 0) > 0:
        erros.append("Existem municípios sem UF correspondente")

    return erros


def validar_erros_criticos(resultado):
    """Interrompe o pipeline quando a qualidade compromete a camada Gold."""
    erros = encontrar_erros_criticos(resultado)
    if erros:
        raise ValueError("Erros críticos de qualidade: " + "; ".join(erros))


def salvar_relatorio_qualidade(spark, pasta_silver, id_execucao, resultado):
    """Grava as métricas de qualidade em Parquet para cada execução."""
    caminho = juntar_caminho(
        pasta_silver,
        f"_qualidade/id_execucao={id_execucao}",
    )
    dataframe = (
        spark.createDataFrame(criar_relatorio_qualidade(resultado))
        .withColumn("id_execucao", lit(id_execucao))
        .withColumn("data_execucao", current_timestamp())
    )
    dataframe.write.mode("overwrite").parquet(caminho)
    return caminho


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


def executar(spark, pasta_bronze, pasta_silver, id_execucao=None):
    resultado = {}
    tabelas_silver = {}
    id_execucao = id_execucao or str(uuid4())

    for nome_tabela in CHAVES_TABELAS:
        dataframe_bronze = ler_carga_mais_recente(
            spark,
            pasta_bronze,
            nome_tabela,
        )
        validacao_inicial = validar_tabela(nome_tabela, dataframe_bronze)
        dataframe_tratado = tratar_tabela(nome_tabela, dataframe_bronze)
        dataframe_validado = adicionar_validacao(
            nome_tabela,
            dataframe_tratado,
        )
        dataframe_silver, dataframe_rejeitados = separar_registros(
            dataframe_validado
        )
        caminho_saida = juntar_caminho(pasta_silver, nome_tabela)
        caminho_rejeitados = juntar_caminho(
            pasta_silver,
            f"_rejeitados/{nome_tabela}",
        )

        dataframe_silver.write.mode("overwrite").parquet(caminho_saida)
        dataframe_rejeitados.write.mode("overwrite").parquet(
            caminho_rejeitados
        )
        tabelas_silver[nome_tabela] = dataframe_silver

        resultado[nome_tabela] = {
            "caminho": caminho_saida,
            "caminho_rejeitados": caminho_rejeitados,
            "registros_validos": dataframe_silver.count(),
            "registros_rejeitados": dataframe_rejeitados.count(),
            **validacao_inicial,
        }

    resultado["relacionamentos"] = validar_relacionamentos(tabelas_silver)
    resultado["qualidade"] = {
        "id_execucao": id_execucao,
        "caminho": salvar_relatorio_qualidade(
            spark,
            pasta_silver,
            id_execucao,
            resultado,
        ),
        "erros_criticos": encontrar_erros_criticos(resultado),
    }

    return resultado
