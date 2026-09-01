from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, from_json, hour, month, to_timestamp, year
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


def criar_esquema_evento():
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_timestamp", StringType(), False),
            StructField("sequence", LongType(), False),
            StructField("ano", IntegerType(), False),
            StructField("sigla_uf", StringType(), False),
            StructField("serie", StringType(), False),
            StructField("rede", StringType(), False),
            StructField("taxa_alfabetizacao", DoubleType(), False),
            StructField("source", StringType(), False),
            StructField("schema_version", IntegerType(), False),
        ]
    )


def ler_argumentos():
    analisador = argparse.ArgumentParser(description="Consumidor Kafka com PySpark.")
    analisador.add_argument("--bootstrap-servers", dest="servidores", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    analisador.add_argument("--topic", dest="topico", default=os.getenv("KAFKA_TOPIC", "alfabetizacao-indicadores"))
    analisador.add_argument("--output-path", dest="caminho_saida", default=os.getenv("STREAM_OUTPUT_PATH"))
    analisador.add_argument("--checkpoint-path", dest="caminho_checkpoint", default=os.getenv("CHECKPOINT_PATH"))
    analisador.add_argument("--auth-mode", dest="modo_autenticacao", default=os.getenv("KAFKA_AUTH_MODE", "gcp_iam"))
    analisador.add_argument(
        "--tempo-maximo-segundos",
        dest="tempo_maximo_segundos",
        type=int,
        default=int(os.getenv("STREAM_TIMEOUT_SECONDS", "300")),
        help="Tempo máximo do consumidor. Use 0 para executar continuamente.",
    )
    argumentos = analisador.parse_args()
    obrigatorios = ["servidores", "caminho_saida", "caminho_checkpoint"]
    faltantes = [nome for nome in obrigatorios if not getattr(argumentos, nome)]
    if faltantes:
        analisador.error(f"Parâmetros obrigatórios ausentes: {', '.join(faltantes)}")
    return argumentos


def principal():
    argumentos = ler_argumentos()
    spark = SparkSession.builder.appName("alfabetizacao-kafka-streaming").getOrCreate()
    leitura_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", argumentos.servidores)
        .option("subscribe", argumentos.topico)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
    )
    if argumentos.modo_autenticacao == "gcp_iam":
        leitura_kafka = (
            leitura_kafka.option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "OAUTHBEARER")
            .option(
                "kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
            )
            .option(
                "kafka.sasl.login.callback.handler.class",
                "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
            )
        )
    dados_brutos = leitura_kafka.load()
    dados_json = dados_brutos.select(
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), criar_esquema_evento()).alias("data"),
    ).where(col("data").isNotNull())
    eventos = (
        dados_json.select("kafka_timestamp", "data.*")
        .withColumn("timestamp_evento", to_timestamp("event_timestamp"))
        .withColumn("particao_ano", year("timestamp_evento"))
        .withColumn("particao_mes", month("timestamp_evento"))
        .withColumn("particao_dia", dayofmonth("timestamp_evento"))
        .withColumn("particao_hora", hour("timestamp_evento"))
        .where(col("taxa_alfabetizacao").between(0, 100))
        .withWatermark("timestamp_evento", "10 minutes")
        .dropDuplicates(["event_id"])
    )
    consulta = (
        eventos.writeStream.format("parquet")
        .option("path", argumentos.caminho_saida)
        .option("checkpointLocation", argumentos.caminho_checkpoint)
        .partitionBy("particao_ano", "particao_mes", "particao_dia", "particao_hora")
        .outputMode("append")
        .start()
    )
    if argumentos.tempo_maximo_segundos > 0:
        terminou = consulta.awaitTermination(argumentos.tempo_maximo_segundos)
        if not terminou:
            consulta.stop()
    else:
        consulta.awaitTermination()


if __name__ == "__main__":
    principal()
