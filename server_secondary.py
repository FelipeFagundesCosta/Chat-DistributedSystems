"""
server_secondary.py — Servidor de Chat Secundário (Failover)
============================================================
Idêntico ao primário em funcionalidade de chat.
Diferencial: monitora o primário via HTTP e aceita clientes quando ele cai.

Na nuvem (Render), cada servidor tem sua própria URL pública.
O failover aqui não é "assumir a porta" — é simplesmente estar disponível
numa URL diferente. O cliente tenta essa URL quando o primário não responde.
"""

import http.server
import json
import os
import socket
import threading
import time
from datetime import datetime
from pathlib import Path

from ws_protocol import handshake, recv_frame, send_frame, serve_http

# ── Configuração ──────────────────────────────────────────────────────────────
HOST              = "0.0.0.0"
PORT              = int(os.environ.get("PORT", 5001))
HEARTBEAT_TIMEOUT = 6    # segundos sem heartbeat → primário considerado morto

FRONTEND_HTML = (Path(__file__).parent / "index.html").read_text(encoding="utf-8")

# ── Estado global ─────────────────────────────────────────────────────────────
clients: dict = {}
clients_lock = threading.Lock()

last_heartbeat: float = time.time()
primary_alive: bool = True


# ── Mensagens ─────────────────────────────────────────────────────────────────

def make_msg(msg_type: str, **kwargs) -> str:
    return json.dumps({
        "type": msg_type,
        "timestamp": datetime.now().strftime("%H:%M"),
        **kwargs
    })


# ── Broadcast ─────────────────────────────────────────────────────────────────

def broadcast(payload: str, exclude_conn=None):
    with clients_lock:
        targets = [(c, info) for c, info in clients.items() if c != exclude_conn]
    dead = []
    for conn, info in targets:
        if not send_frame(conn, payload):
            dead.append(conn)
    if dead:
        with clients_lock:
            for c in dead:
                clients.pop(c, None)


def broadcast_user_list():
    with clients_lock:
        users = [i["username"] for i in clients.values()]
        conns = list(clients.keys())
    payload = make_msg("user_list", users=users)
    for conn in conns:
        send_frame(conn, payload)


# ── Handler de cliente ────────────────────────────────────────────────────────

def handle_client(conn: socket.socket, addr: tuple):
    """Mesma lógica do primário. Thread dedicada por conexão."""
    username = None
    print(f"[+] Nova conexão: {addr}")

    try:
        peek = conn.recv(4096, socket.MSG_PEEK)
        first_line = peek.decode("utf-8", errors="ignore").split("\r\n")[0]
        headers_raw = peek.decode("utf-8", errors="ignore")
        is_ws = "upgrade: websocket" in headers_raw.lower()

        # ── HTTP comum: /ping, /health ou /heartbeat ──────────────────────────
        if not is_ws:
            conn.recv(4096)

            if "/heartbeat" in first_line:
                # Recebe heartbeat do primário — atualiza timestamp
                global last_heartbeat, primary_alive
                last_heartbeat = time.time()
                if not primary_alive:
                    print("[✔] Primário voltou a enviar heartbeats.")
                primary_alive = True
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                    b"Content-Length: 2\r\n\r\nOK"
                )
            elif "/ping" in first_line or "/health" in first_line:
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                    b"Content-Length: 2\r\n\r\nOK"
                )
            else:
                serve_http(conn, FRONTEND_HTML)
            return

        # ── WebSocket ─────────────────────────────────────────────────────────
        if not handshake(conn):
            return

        raw = recv_frame(conn)
        if raw is None:
            return

        data = json.loads(raw)
        if data.get("type") != "join" or not data.get("username", "").strip():
            send_frame(conn, make_msg("error", text="Nome inválido."))
            return

        username = data["username"].strip()[:24]

        with clients_lock:
            if any(i["username"] == username for i in clients.values()):
                send_frame(conn, make_msg("error", text="Nome já em uso."))
                return
            clients[conn] = {"username": username, "addr": addr}

        status = "primário (recuperado)" if not primary_alive else "secundário"
        print(f"[✔] '{username}' conectou ao SECUNDÁRIO. Total: {len(clients)}")

        send_frame(conn, make_msg("joined", username=username, server=status))
        broadcast(make_msg("system", text=f"🟢 {username} entrou no chat."))
        broadcast_user_list()

        while True:
            raw = recv_frame(conn)
            if raw is None:
                break
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if data.get("type") == "message":
                text = data.get("text", "").strip()[:500]
                if text:
                    payload = make_msg("message", username=username, text=text)
                    broadcast(payload, exclude_conn=conn)  # outros usuários
                    send_frame(conn, payload)               # confirmação ao remetente

    except Exception as e:
        print(f"[!] Erro: {e}")
    finally:
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


# ── Monitor do primário ────────────────────────────────────────────────────────

def monitor_primary():
    """
    Verifica periodicamente se o primário está enviando heartbeats.
    Se o timeout expirar, loga o failover (clientes já tentam esta URL).
    """
    global primary_alive
    while True:
        time.sleep(2)
        elapsed = time.time() - last_heartbeat
        if elapsed > HEARTBEAT_TIMEOUT and primary_alive:
            primary_alive = False
            print(f"[⚠] Primário não responde há {elapsed:.1f}s — assumindo papel principal.")
        elif elapsed <= HEARTBEAT_TIMEOUT and not primary_alive:
            primary_alive = True
            print("[✔] Primário voltou. Secundário retorna ao modo de backup.")


# ── Loop principal ─────────────────────────────────────────────────────────────

def main():
    threading.Thread(target=monitor_primary, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(50)

    print(f"[SECUNDÁRIO] Escutando em {HOST}:{PORT}")
    print(f"[SECUNDÁRIO] Aguardando heartbeats do primário (timeout: {HEARTBEAT_TIMEOUT}s)")

    while True:
        try:
            conn, addr = server.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
        except KeyboardInterrupt:
            print("\n[SECUNDÁRIO] Encerrando...")
            break
        except Exception as e:
            print(f"[!] Erro ao aceitar conexão: {e}")

    server.close()


if __name__ == "__main__":
    main()
