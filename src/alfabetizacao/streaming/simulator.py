from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

UF_CODES = ("AC", "AL", "AM", "BA", "CE", "DF", "MG", "PA", "PE", "PR", "RJ", "RS", "SC", "SP")


def criar_evento(numero, sorteador):
    """Cria um evento idempotente compatível com a chave de negócio da tabela UF."""
    return {
        "event_id": str(uuid4()),
        "event_timestamp": datetime.now(UTC).isoformat(),
        "sequence": numero,
        "ano": 2024,
        "sigla_uf": sorteador.choice(UF_CODES),
        "serie": "2º Ano",
        "rede": sorteador.choice(("Estadual", "Municipal")),
        "taxa_alfabetizacao": round(sorteador.uniform(45, 90), 2),
        "source": "simulador",
        "schema_version": 1,
    }


def gerar_arquivo_json(caminho, quantidade=100, semente=42):
    """Gera eventos locais; qualquer produtor Kafka pode reutilizar o mesmo contrato."""
    if quantidade < 1:
        raise ValueError("A quantidade de eventos deve ser positiva.")
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    sorteador = random.Random(semente)
    with caminho.open("w", encoding="utf-8") as arquivo:
        for numero in range(quantidade):
            evento = criar_evento(numero, sorteador)
            arquivo.write(json.dumps(evento, ensure_ascii=False) + "\n")
    return caminho


def principal():
    analisador = argparse.ArgumentParser(description="Gera eventos de alfabetização em JSON Lines.")
    analisador.add_argument("--output", dest="caminho_saida", type=Path, default=Path("streaming/events/uf.jsonl"))
    analisador.add_argument("--quantity", dest="quantidade", type=int, default=100)
    analisador.add_argument("--seed", dest="semente", type=int, default=42)
    argumentos = analisador.parse_args()
    caminho = gerar_arquivo_json(argumentos.caminho_saida, argumentos.quantidade, argumentos.semente)
    print(f"{argumentos.quantidade} eventos gerados em {caminho}")


if __name__ == "__main__":
    principal()
