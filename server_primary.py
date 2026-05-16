"""
server_primary.py — Servidor de Chat Primário
=============================================
Stack: socket + threading da stdlib. Zero dependências externas.

Responsabilidades:
  - Servir o index.html via HTTP (GET /)
  - Fazer o handshake WebSocket manualmente
  - Instanciar uma thread por cliente conectado (requisito)
  - Fazer broadcast de mensagens para todos os clientes
  - Enviar heartbeats HTTP ao servidor secundário
  - Responder GET /ping para o keep-alive do cron externo
  - Responder GET /health para o UptimeRobot
"""

import json
import os
import socket
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from ws_protocol import handshake, recv_frame, send_frame, serve_http

# ── Configuração (via variáveis de ambiente para o Render) ───────────────────
HOST             = os.environ.get("HOST", "0.0.0.0")
PORT             = int(os.environ.get("PORT", 5000))
SECONDARY_URL    = os.environ.get("SECONDARY_URL", "http://localhost:5001")
HEARTBEAT_INTERVAL = 2   # segundos entre heartbeats ao secundário

# ── HTML do frontend (embutido — um único arquivo, zero dependências) ─────────
# Em produção, este conteúdo vem do Cloudflare Pages.
# Embutimos aqui para que o servidor seja 100% autossuficiente localmente.
FRONTEND_HTML = (Path(__file__).parent / "index.html").read_text(encoding="utf-8")

# ── Estado global ─────────────────────────────────────────────────────────────
# {socket_conn: {"username": str, "addr": tuple}}
clients: dict = {}
clients_lock = threading.Lock()  # protege acesso concorrente ao dicionário


# ── Mensagens JSON ─────────────────────────────────────────────────────────────

def make_msg(msg_type: str, **kwargs) -> str:
    return json.dumps({
        "type": msg_type,
        "timestamp": datetime.now().strftime("%H:%M"),
        **kwargs
    })


# ── Broadcast ─────────────────────────────────────────────────────────────────

def broadcast(payload: str, exclude_conn=None):
    """Envia payload para todos os clientes, exceto o remetente."""
    with clients_lock:
        targets = [(c, info) for c, info in clients.items() if c != exclude_conn]

    dead = []
    for conn, info in targets:
        ok = send_frame(conn, payload)
        if not ok:
            dead.append(conn)

    # Remove clientes mortos
    if dead:
        with clients_lock:
            for conn in dead:
                clients.pop(conn, None)


def broadcast_user_list():
    """Envia a lista atualizada de usuários online para todos."""
    with clients_lock:
        users = [info["username"] for info in clients.values()]
        conns = list(clients.keys())

    payload = make_msg("user_list", users=users)
    for conn in conns:
        send_frame(conn, payload)


# ── Handler de cada cliente (roda em thread dedicada) ─────────────────────────

def handle_client(conn: socket.socket, addr: tuple):
    """
    Gerencia o ciclo de vida completo de um cliente:
      1. Detecta se é HTTP ou WebSocket
      2. Faz o handshake WebSocket
      3. Aguarda o 'join' com o nome do usuário
      4. Loop de recebimento e broadcast de mensagens
      5. Limpeza ao desconectar

    Esta função é executada em uma thread exclusiva por cliente.
    """
    username = None
    print(f"[+] Nova conexão: {addr}")

    try:
        # Lê o início da requisição para distinguir HTTP de WS
        peek = conn.recv(4096, socket.MSG_PEEK)
        first_line = peek.decode("utf-8", errors="ignore").split("\r\n")[0]

        # Verifica especificamente por "upgrade: websocket" (case-insensitive)
        # O Chrome envia "Upgrade-Insecure-Requests: 1" em GETs normais,
        # o que contém "Upgrade" mas NÃO é um upgrade WebSocket.
        headers_raw = peek.decode("utf-8", errors="ignore").lower()
        is_ws = "upgrade: websocket" in headers_raw

        # ── Requisição HTTP comum (GET / ou GET /ping ou GET /health) ─────────
        if not is_ws:
            conn.recv(4096)  # consome os dados
            if "/ping" in first_line or "/health" in first_line:
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                    b"Content-Length: 2\r\n\r\nOK"
                )
            elif first_line.startswith("GET"):
                serve_http(conn, FRONTEND_HTML)
            return  # qualquer outra rota: fecha silenciosamente

        # ── Upgrade para WebSocket ─────────────────────────────────────────────
        ok = handshake(conn)
        if not ok:
            print(f"[-] Handshake falhou para {addr}")
            return

        # ── Aguarda o 'join' com o nome do usuário ────────────────────────────
        raw = recv_frame(conn)
        if raw is None:
            return

        data = json.loads(raw)
        if data.get("type") != "join" or not data.get("username", "").strip():
            send_frame(conn, make_msg("error", text="Nome de usuário inválido."))
            return

        username = data["username"].strip()[:24]

        # Verifica duplicidade
        with clients_lock:
            taken = any(i["username"] == username for i in clients.values())
            if taken:
                send_frame(conn, make_msg("error", text="Nome já em uso. Escolha outro."))
                return
            clients[conn] = {"username": username, "addr": addr}

        print(f"[✔] '{username}' entrou. Total: {len(clients)}")

        # Confirma conexão ao cliente
        send_frame(conn, make_msg("joined", username=username, server="primário"))

        # Notifica todos
        broadcast(make_msg("system", text=f"🟢 {username} entrou no chat."))
        broadcast_user_list()

        # ── Loop principal de mensagens ────────────────────────────────────────
        while True:
            raw = recv_frame(conn)
            if raw is None:
                break  # conexão encerrada

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if data.get("type") == "message":
                text = data.get("text", "").strip()[:500]
                if text:
                    print(f"[{username}]: {text}")
                    payload = make_msg("message", username=username, text=text)
                    broadcast(payload, exclude_conn=conn)  # outros usuários
                    send_frame(conn, payload)               # confirmação ao remetente

    except Exception as e:
        print(f"[!] Erro no handler de {addr}: {e}")

    finally:
        # ── Limpeza ────────────────────────────────────────────────────────────
        if username:
            with clients_lock:
                clients.pop(conn, None)
            print(f"[✖] '{username}' desconectou. Total: {len(clients)}")
            broadcast(make_msg("system", text=f"🔴 {username} saiu do chat."))
            broadcast_user_list()
        try:
            conn.close()
        except Exception:
            pass


# ── Heartbeat ao servidor secundário ──────────────────────────────────────────

def heartbeat_loop():
    """
    Envia GET /heartbeat ao secundário a cada HEARTBEAT_INTERVAL segundos.
    Roda em thread daemon — encerra junto com o processo principal.
    """
    url = f"{SECONDARY_URL}/heartbeat"
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        try:
            req = urllib.request.Request(url, method="GET")
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass  # secundário pode não estar no ar; não é fatal


# ── Loop principal ─────────────────────────────────────────────────────────────

def main():
    # Inicia heartbeat em background
    t_hb = threading.Thread(target=heartbeat_loop, daemon=True)
    t_hb.start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(50)

    print(f"[PRIMÁRIO] Escutando em {HOST}:{PORT}")
    print(f"[PRIMÁRIO] Heartbeat → {SECONDARY_URL}")
    print(f"[PRIMÁRIO] Frontend disponível em http://localhost:{PORT}")

    while True:
        try:
            conn, addr = server.accept()
            # Requisito: uma thread por conexão
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
        except KeyboardInterrupt:
            print("\n[PRIMÁRIO] Encerrando...")
            break
        except Exception as e:
            print(f"[!] Erro ao aceitar conexão: {e}")

    server.close()


if __name__ == "__main__":
    main()
