"""Funções simples para registrar eventos estruturados em JSON."""

import json
from datetime import UTC, datetime


def registrar_evento(tipo, pipeline, **dados):
    """Escreve um evento JSON que pode ser lido pelo Cloud Logging."""
    evento = {
        "timestamp": datetime.now(UTC).isoformat(),
        "severity": "ERROR" if dados.get("status") == "falha" else "INFO",
        "tipo": tipo,
        "pipeline": pipeline,
        **dados,
    }
    print(json.dumps(evento, ensure_ascii=False, default=str), flush=True)
    return evento
