"""
protocol.py — Protocolo de aplicação: NDJSON sobre TCP
=======================================================
Cada frame é uma linha UTF-8 terminada em \\n contendo um objeto JSON.
Muito mais simples que WebSocket: sem handshake SHA-1, sem frames binários,
sem máscara XOR. Qualquer socket TCP consegue usar isso.

Tipos de mensagem:
  Cliente → Servidor:  login, message, ping
  Servidor → Cliente:  welcome, chat, user_joined, user_left, error, pong, history
  Servidor → Servidor: pub/sub via Redis (interno)
"""

import json

NEWLINE = b"\n"


def encode(payload: dict) -> bytes:
    """Serializa dict como linha NDJSON (inclui o \\n final)."""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode() + NEWLINE


def decode(line: bytes) -> dict:
    """Desserializa uma linha NDJSON. Retorna {} se vazia."""
    stripped = line.rstrip(NEWLINE)
    if not stripped:
        return {}
    return json.loads(stripped.decode("utf-8"))
