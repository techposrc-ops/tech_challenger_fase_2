from __future__ import annotations

import base64
import json
import os
import random
import time
from datetime import UTC, datetime
from uuid import uuid4

from flask import Flask, jsonify

from alfabetizacao.observabilidade import registrar_evento

UF_CODES = ("AC", "AL", "AM", "BA", "CE", "DF", "MG", "PA", "PE", "PR", "RJ", "RS", "SC", "SP")

app = Flask(__name__)
credenciais_gcp = None
token_gcp = None
expiracao_token_gcp = 0


def criar_evento(numero, sorteador=None):
    sorteador = sorteador or random.Random()
    return {
        "event_id": str(uuid4()),
        "event_timestamp": datetime.now(UTC).isoformat(),
        "sequence": numero,
        "ano": 2024,
        "sigla_uf": sorteador.choice(UF_CODES),
        "serie": "2º Ano",
        "rede": sorteador.choice(("Estadual", "Municipal")),
        "taxa_alfabetizacao": round(sorteador.uniform(45, 90), 2),
        "source": "produtor-kafka",
        "schema_version": 1,
    }


def obter_token_gcp(_):
    global credenciais_gcp, token_gcp, expiracao_token_gcp

    import google.auth
    import urllib3
    from google.auth.transport.urllib3 import Request

    if token_gcp and expiracao_token_gcp > time.time() + 60:
        return token_gcp, expiracao_token_gcp

    if credenciais_gcp is None:
        if os.getenv("K_SERVICE"):
            from google.auth.compute_engine import Credentials

            credenciais_gcp = Credentials()
        else:
            credenciais_gcp, _ = google.auth.default()
    if not credenciais_gcp.valid:
        credenciais_gcp.refresh(Request(urllib3.PoolManager()))
    principal = getattr(credenciais_gcp, "service_account_email", None)
    if not credenciais_gcp.token or not credenciais_gcp.expiry or not principal:
        raise RuntimeError("Não foi possível obter o token IAM do Google Cloud.")

    def encode(value: str) -> str:
        return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")

    header = json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})
    claims = json.dumps(
        {
            "exp": credenciais_gcp.expiry.replace(tzinfo=UTC).timestamp(),
            "iat": datetime.now(UTC).timestamp(),
            "iss": "Google",
            "sub": principal,
        }
    )
    token_gcp = ".".join((encode(header), encode(claims), encode(credenciais_gcp.token)))
    expiracao_token_gcp = credenciais_gcp.expiry.replace(tzinfo=UTC).timestamp()
    return token_gcp, expiracao_token_gcp


def criar_configuracao_produtor():
    configuracao = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "message.timeout.ms": 30_000,
        "enable.idempotence": True,
    }
    modo_autenticacao = os.getenv("KAFKA_AUTH_MODE", "gcp_iam")
    if modo_autenticacao == "gcp_iam":
        obter_token_gcp(None)
        configuracao.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "OAUTHBEARER",
                "oauth_cb": obter_token_gcp,
            }
        )
    elif modo_autenticacao == "plaintext":
        configuracao["security.protocol"] = "PLAINTEXT"
    else:
        raise ValueError(f"KAFKA_AUTH_MODE não suportado: {modo_autenticacao}")
    return configuracao


def publicar_eventos(quantidade):
    from confluent_kafka import Producer

    produtor = Producer(criar_configuracao_produtor())
    topico = os.getenv("KAFKA_TOPIC", "alfabetizacao-indicadores")
    entregues = 0

    def confirmar_entrega(erro, _):
        nonlocal entregues
        if erro is None:
            entregues += 1

    for numero in range(quantidade):
        evento = criar_evento(numero)
        produtor.produce(
            topico,
            key=str(evento["event_id"]).encode(),
            value=json.dumps(evento, ensure_ascii=False).encode(),
            callback=confirmar_entrega,
        )
        produtor.poll(0)
    pendentes = produtor.flush(timeout=30)
    if pendentes:
        raise RuntimeError(f"Falha ao entregar {pendentes} eventos ao Kafka.")
    return entregues


@app.get("/")
def verificar_servico():
    return jsonify({"servico": "produtor-kafka", "status": "disponivel"})


@app.post("/produzir")
def produzir():
    quantidade = int(os.getenv("EVENTS_PER_INVOCATION", "10"))
    id_execucao = str(uuid4())
    inicio = time.perf_counter()
    registrar_evento(
        "pipeline_inicio",
        "streaming_produtor",
        id_execucao=id_execucao,
        quantidade_solicitada=quantidade,
    )
    if quantidade < 1 or quantidade > 1000:
        registrar_evento(
            "pipeline_fim",
            "streaming_produtor",
            id_execucao=id_execucao,
            status="falha",
            duracao_segundos=round(time.perf_counter() - inicio, 2),
            erro="Quantidade de eventos fora do intervalo permitido.",
        )
        return jsonify({"erro": "EVENTS_PER_INVOCATION deve estar entre 1 e 1000."}), 400

    try:
        entregues = publicar_eventos(quantidade)
    except Exception as erro:
        app.logger.exception("Falha ao publicar eventos no Kafka.")
        registrar_evento(
            "pipeline_fim",
            "streaming_produtor",
            id_execucao=id_execucao,
            status="falha",
            duracao_segundos=round(time.perf_counter() - inicio, 2),
            erro=str(erro),
        )
        return jsonify({"erro": str(erro)}), 500

    registrar_evento(
        "pipeline_fim",
        "streaming_produtor",
        id_execucao=id_execucao,
        status="sucesso",
        duracao_segundos=round(time.perf_counter() - inicio, 2),
        quantidade_registros=entregues,
    )
    return jsonify({"topico": os.getenv("KAFKA_TOPIC"), "eventos_entregues": entregues})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
