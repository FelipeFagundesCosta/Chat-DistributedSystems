"""
ws_protocol.py — Implementação manual do protocolo WebSocket (RFC 6455)
=======================================================================
Sem bibliotecas externas. Apenas hashlib, base64, struct — todos da stdlib.

O WebSocket começa como HTTP e faz "upgrade" para um protocolo binário
de frames. Este módulo implementa as duas etapas:
  1. handshake()  — negocia o upgrade HTTP → WebSocket
  2. recv_frame() — decodifica um frame recebido do browser
  3. send_frame() — codifica e envia uma mensagem de texto ao browser
"""

import hashlib
import base64
import struct

# Chave mágica definida pelo RFC 6455 — nunca muda
_WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def handshake(conn: object) -> bool:
    """
    Lê a requisição HTTP de upgrade do browser e responde com 101.
    Retorna True se o handshake foi bem-sucedido, False caso contrário.

    O browser envia algo assim:
        GET / HTTP/1.1
        Host: localhost:5000
        Upgrade: websocket
        Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
        ...
    """
    try:
        raw = b""
        # Lê até encontrar o fim dos headers HTTP (\r\n\r\n)
        while b"\r\n\r\n" not in raw:
            chunk = conn.recv(1024)
            if not chunk:
                return False
            raw += chunk

        request = raw.decode("utf-8", errors="ignore")

        # Extrai a chave Sec-WebSocket-Key dos headers
        key = None
        for line in request.split("\r\n"):
            if line.lower().startswith("sec-websocket-key:"):
                key = line.split(":", 1)[1].strip()
                break

        # Verifica se é realmente um upgrade WebSocket
        if key is None or "upgrade" not in request.lower():
            return False

        # Calcula o accept: SHA1(key + magic), depois base64
        accept = base64.b64encode(
            hashlib.sha1((key + _WS_MAGIC).encode()).digest()
        ).decode()

        # Responde com HTTP 101 Switching Protocols
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "\r\n"
        )
        conn.sendall(response.encode())
        return True

    except Exception:
        return False


def recv_frame(conn: object) -> str | None:
    """
    Lê e decodifica um frame WebSocket enviado pelo browser.
    Retorna a mensagem como string, ou None se a conexão fechou.

    Estrutura de um frame WebSocket (simplificado para payloads < 126 bytes):
        Byte 0: FIN + opcode  (0x81 = mensagem de texto final)
        Byte 1: MASK + length (browser SEMPRE envia com máscara)
        Bytes 2-5: chave de máscara (4 bytes)
        Bytes 6+:  payload XOR com a máscara
    """
    try:
        # Lê os 2 primeiros bytes (cabeçalho básico)
        header = _recv_exactly(conn, 2)
        if header is None:
            return None

        opcode = header[0] & 0x0F

        # Opcode 0x8 = close frame — sinaliza fechamento limpo
        if opcode == 0x8:
            return None

        # Opcode 0x9 = ping — responde com pong (0xA) e continua
        if opcode == 0x9:
            payload_len = header[1] & 0x7F
            if payload_len > 0:
                _recv_exactly(conn, payload_len + 4)  # descarta ping payload
            pong = bytes([0x8A, 0x00])
            conn.sendall(pong)
            return recv_frame(conn)  # aguarda próximo frame

        masked = bool(header[1] & 0x80)
        length = header[1] & 0x7F

        # Comprimentos estendidos (126 = 2 bytes extras, 127 = 8 bytes extras)
        if length == 126:
            ext = _recv_exactly(conn, 2)
            if ext is None:
                return None
            length = struct.unpack(">H", ext)[0]
        elif length == 127:
            ext = _recv_exactly(conn, 8)
            if ext is None:
                return None
            length = struct.unpack(">Q", ext)[0]

        # Lê a máscara (4 bytes), presente quando masked=True (sempre no browser→servidor)
        mask = None
        if masked:
            mask = _recv_exactly(conn, 4)
            if mask is None:
                return None

        # Lê o payload
        payload = _recv_exactly(conn, length)
        if payload is None:
            return None

        # Desfaz a máscara XOR
        if masked and mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))

        return payload.decode("utf-8", errors="ignore")

    except Exception:
        return None


def send_frame(conn: object, message: str) -> bool:
    """
    Codifica uma string como frame WebSocket e envia ao browser.
    Servidor → browser NÃO usa máscara (ao contrário do browser → servidor).

    Retorna True se enviou com sucesso, False caso contrário.
    """
    try:
        payload = message.encode("utf-8")
        length = len(payload)

        # Constrói o cabeçalho de acordo com o tamanho do payload
        if length <= 125:
            header = bytes([0x81, length])
        elif length <= 65535:
            header = bytes([0x81, 126]) + struct.pack(">H", length)
        else:
            header = bytes([0x81, 127]) + struct.pack(">Q", length)

        conn.sendall(header + payload)
        return True

    except Exception:
        return False


def serve_http(conn: object, html: str) -> None:
    """
    Responde a uma requisição HTTP GET comum com o HTML do frontend.
    Usado quando o browser acessa a raiz (/) antes do upgrade WebSocket.
    """
    body = html.encode("utf-8")
    response = (
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode() + body
    try:
        conn.sendall(response)
    except Exception:
        pass


# ── Interno ───────────────────────────────────────────────────────────────────

def _recv_exactly(conn: object, n: int) -> bytes | None:
    """Lê exatamente n bytes do socket. Retorna None se a conexão fechar."""
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data
