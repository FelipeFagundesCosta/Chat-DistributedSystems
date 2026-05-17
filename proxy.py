"""
proxy.py — Proxy HTTP → TCP
============================
Bridges the browser (HTTP/SSE) to the TCP chat server.

Browser                     Proxy                    TCP Server
──────                      ─────                    ──────────
POST /login          →      login via TCP       →    authenticate
POST /message        →      message via TCP     →    broadcast
GET  /events (SSE)   ←      recv thread         ←    event stream
GET  /               ←      serves index.html

Thread dedicada à recepção: cada sessão tem uma TCPSession._recv_thread
que fica bloqueada lendo o socket TCP — satisfaz o requisito acadêmico.

Variáveis de ambiente:
  SERVER_HOST  — host do servidor TCP (padrão: localhost)
  SERVER_PORT  — porta do servidor TCP (padrão: 5000)
  PROXY_PORT   — porta HTTP deste proxy (padrão: 8080)
"""

import json
import logging
import os
import queue
import socket
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """HTTPServer que atende cada requisicao em uma thread separada.
    Necessario para que POST /message nao bloqueie enquanto GET /events
    esta ativo — SSE mantem a conexao aberta indefinidamente."""
    daemon_threads = True
from pathlib import Path

from protocol import decode, encode

# ── Configuração ──────────────────────────────────────────────────────────────
SERVER_HOST = os.environ.get("SERVER_HOST", "localhost")
SERVER_PORT = int(os.environ.get("SERVER_PORT", 5000))
PROXY_PORT  = int(os.environ.get("PROXY_PORT", 8080))

# Em produção, passe algo como:
#   SERVER_HOSTS=server-a.example.com:5000,server-b.example.com:5000
SERVER_HOSTS = os.environ.get("SERVER_HOSTS", f"{SERVER_HOST}:{SERVER_PORT}")
BACKEND_SERVERS: list[tuple[str, int]] = []
for part in SERVER_HOSTS.split(","):
    host, sep, port_text = part.strip().partition(":")
    if not host:
        continue
    port = int(port_text) if sep and port_text.isdigit() else SERVER_PORT
    BACKEND_SERVERS.append((host, port))

FRONTEND_HTML = (Path(__file__).parent / "index.html").read_text(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [PROXY ] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("proxy")


# ── Sessão TCP (thread de recepção dedicada) ───────────────────────────────────

class TCPSession:
    """
    Representa a conexão TCP de um usuário com o servidor de chat.

    A thread de recepção (_recv_thread) fica bloqueada lendo o socket TCP
    e distribui os eventos para as filas SSE dos browsers conectados.
    """

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self._conn      = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._lock      = threading.Lock()
        self._closed    = threading.Event()

        # Uma fila por aba do browser conectada em /events
        self._queues: set[queue.Queue] = set()
        self._queues_lock = threading.Lock()

        # Thread dedicada à recepção — requisito acadêmico no cliente
        self._recv_thread = threading.Thread(
            target=self._recv_loop,
            name=f"recv-{session_id[:8]}",
            daemon=True,
        )

    def connect(self) -> None:
        last_exc = None
        for host, port in BACKEND_SERVERS:
            try:
                self._conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._conn.connect((host, port))
                self._conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self._recv_thread.start()
                log.info("TCP conectado para sessão %s -> %s:%d", self.session_id[:8], host, port)
                return
            except OSError as exc:
                last_exc = exc
                try:
                    self._conn.close()
                except OSError:
                    pass
        raise last_exc or OSError("Nenhum servidor TCP disponível")

    def send(self, **kwargs) -> None:
        """Envia um frame NDJSON para o servidor TCP."""
        with self._lock:
            self._conn.sendall(encode(kwargs))

    def subscribe(self) -> queue.Queue:
        """Adiciona uma fila SSE (uma por aba do browser)."""
        q: queue.Queue = queue.Queue(maxsize=64)
        with self._queues_lock:
            self._queues.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._queues_lock:
            self._queues.discard(q)

    def close(self) -> None:
        self._closed.set()
        try:
            self._conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._conn.close()
        except OSError:
            pass

    def is_closed(self) -> bool:
        return self._closed.is_set()

    def _recv_loop(self) -> None:
        """
        Thread dedicada à recepção — fica bloqueada lendo o socket TCP linha a linha.
        Cada frame recebido é distribuído para todas as filas SSE ativas.
        """
        reader = self._conn.makefile("rb")
        try:
            for raw_line in reader:
                if self._closed.is_set() or not raw_line:
                    break
                try:
                    msg = decode(raw_line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not msg:
                    continue
                with self._queues_lock:
                    targets = list(self._queues)
                for q in targets:
                    try:
                        q.put_nowait(msg)
                    except queue.Full:
                        pass
        except OSError:
            pass
        finally:
            self._closed.set()
            try:
                reader.close()
            except OSError:
                pass
            log.info("Recv loop encerrado para sessão %s", self.session_id[:8])


# ── Registro global de sessões ────────────────────────────────────────────────

_sessions: dict[str, TCPSession] = {}
_sessions_lock = threading.Lock()


def _get_session(sid: str) -> TCPSession | None:
    with _sessions_lock:
        return _sessions.get(sid)


def _remove_session(sid: str) -> None:
    with _sessions_lock:
        sess = _sessions.pop(sid, None)
    if sess:
        sess.close()


# ── Handler HTTP ──────────────────────────────────────────────────────────────

class ProxyHandler(BaseHTTPRequestHandler):
    """
    Rotas expostas ao browser:
      GET  /           → index.html
      POST /login      → abre TCP, envia login, aguarda welcome
      POST /message    → envia message via TCP
      GET  /events     → SSE stream de eventos
      POST /logout     → encerra sessão TCP
      GET  /health     → keep-alive / monitoramento
    """

    # ── Roteamento ────────────────────────────────────────────────────────────

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors_headers()
        self.end_headers()

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self._serve_html()
        elif self.path.startswith("/events"):
            self._handle_events()
        elif self.path.startswith("/users"):
            self._handle_users()
        elif self.path in ("/health", "/ping"):
            self._write_text("OK")
        else:
            self._write_json(404, {"error": "Not Found"})

    def do_POST(self):
        if self.path == "/login":
            self._handle_login()
        elif self.path == "/resume":
            self._handle_resume()
        elif self.path == "/message":
            self._handle_message()
        elif self.path == "/logout":
            self._handle_logout()
        else:
            self._write_json(404, {"error": "Not Found"})

    # ── Handlers ──────────────────────────────────────────────────────────────

    def _serve_html(self):
        body = FRONTEND_HTML.encode()
        self._send_headers(200, "text/html; charset=utf-8", len(body))
        self._write(body)

    def _handle_login(self):
        body = self._read_json()
        if body is None:
            return
        username = str(body.get("username", "")).strip()[:32]
        if not username:
            return self._write_json(400, {"error": "username inválido"})

        session_id = uuid.uuid4().hex
        sess = TCPSession(session_id)

        try:
            sess.connect()
        except OSError as e:
            return self._write_json(503, {"error": f"servidor indisponível: {e}"})

        # Registra fila ANTES de enviar para não perder a resposta
        q = sess.subscribe()
        try:
            sess.send(type="login", username=username)
            response = q.get(timeout=10)
        except (queue.Empty, TimeoutError):
            sess.close()
            return self._write_json(504, {"error": "timeout no login"})
        finally:
            sess.unsubscribe(q)

        if response.get("type") == "error":
            sess.close()
            return self._write_json(401, {"error": response.get("message", "erro no login")})

        with _sessions_lock:
            _sessions[session_id] = sess

        log.info("Login OK: '%s' (sessão %s)", username, session_id[:8])
        self._write_json(200, {
            "session_id": session_id,
            "username":   response.get("username", username),
            "history":    response.get("history", []),
            "users":      response.get("users", []),
        })

    def _handle_message(self):
        sid = self._get_sid()
        if not sid:
            return
        body = self._read_json()
        if body is None:
            return
        text = str(body.get("text", "")).strip()[:500]
        if not text:
            return self._write_json(400, {"error": "mensagem vazia"})

        sess = _get_session(sid)
        if not sess or sess.is_closed():
            sess, _ = self._resume_session(sid)
        if not sess:
            return self._write_json(401, {"error": "sessão inválida"})

        sess.send(type="message", text=text)
        self._write_json(200, {"ok": True})

    def _handle_events(self):
        """
        SSE — mantém a conexão HTTP aberta e envia eventos em tempo real.
        O browser usa EventSource('/events?sid=...') — HTTP puro, não WebSocket.
        """
        sid = None
        if "sid=" in self.path:
            sid = self.path.split("sid=")[-1].split("&")[0]
        if not sid:
            return self._write_json(400, {"error": "sid obrigatório"})

        sess = _get_session(sid)
        if not sess or sess.is_closed():
            sess, _ = self._resume_session(sid)
        if not sess:
            return self._write_json(401, {"error": "sessão inválida"})

        # Headers SSE
        self.send_response(200)
        self._set_cors_headers()
        self.send_header("Content-Type",      "text/event-stream")
        self.send_header("Cache-Control",     "no-cache")
        self.send_header("Connection",        "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        q = sess.subscribe()
        try:
            while not sess.is_closed():
                try:
                    event = q.get(timeout=20)
                except queue.Empty:
                    # Keepalive — evita timeout do browser/proxy reverso
                    self._write(b": keepalive\n\n")
                    continue

                typ = event.get("type")
                if typ in {"chat", "user_joined", "user_left", "welcome", "error"}:
                    data = json.dumps(event, ensure_ascii=False)
                    self._write(f"data: {data}\n\n".encode())

        except (BrokenPipeError, ConnectionResetError):
            pass  # browser fechou a aba — comportamento normal
        finally:
            sess.unsubscribe(q)

    def _handle_users(self):
        sid = self._get_sid()
        if not sid:
            return
        sess = _get_session(sid)
        if not sess or sess.is_closed():
            sess, _ = self._resume_session(sid)
        if not sess:
            return self._write_json(401, {"error": "sessão inválida"})
        # Lista usuários a partir das sessões ativas no proxy
        with _sessions_lock:
            count = len(_sessions)
        self._write_json(200, {"count": count})

    def _handle_resume(self):
        sid = self._get_sid()
        if not sid:
            return
        sess, response = self._resume_session(sid)
        if not sess:
            return self._write_json(401, {"error": "sessão inválida"})

        if response is None:
            username = backend.get_username(sid)
            if not username:
                return self._write_json(401, {"error": "sessão inválida"})
            response = {
                "type": "welcome",
                "session_id": sid,
                "username": username,
                "history": backend.get_history(),
                "users": backend.get_online_users(),
            }

        self._write_json(200, response)

    def _handle_logout(self):
        sid = self._get_sid()
        if sid:
            sess = _get_session(sid)
            if sess and not sess.is_closed():
                try:
                    sess.send(type="logout")
                except OSError:
                    pass
            _remove_session(sid)
            log.info("Logout: sessão %s", sid[:8])
        self._write_json(200, {"ok": True})

    # ── Primitivas de I/O ─────────────────────────────────────────────────────

    def _set_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Session-Id")

    def _send_headers(self, code: int, content_type: str, length: int) -> None:
        self.send_response(code)
        self._set_cors_headers()
        self.send_header("Content-Type",   content_type)
        self.send_header("Content-Length", str(length))
        self.end_headers()

    def _write(self, data: bytes) -> None:
        """Escreve bytes no socket — ignora BrokenPipe silenciosamente."""
        try:
            self.wfile.write(data)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # cliente fechou a conexão antes da resposta — normal em HTTP

    def _write_json(self, code: int, data: dict) -> None:
        body = json.dumps(data, ensure_ascii=False).encode()
        self._send_headers(code, "application/json", len(body))
        self._write(body)

    def _write_text(self, text: str) -> None:
        body = text.encode()
        self._send_headers(200, "text/plain", len(body))
        self._write(body)

    def _read_json(self) -> dict | None:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            self._write_json(400, {"error": "corpo vazio"})
            return None
        try:
            return json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            self._write_json(400, {"error": "JSON inválido"})
            return None

    def _get_sid(self) -> str | None:
        sid = self.headers.get("X-Session-Id")
        if not sid:
            self._write_json(401, {"error": "X-Session-Id obrigatório"})
        return sid

    def _resume_session(self, sid: str) -> tuple["TCPSession" | None, dict | None]:
        sess = _get_session(sid)
        if sess and not sess.is_closed():
            return sess, None

        try:
            sess = TCPSession(sid)
            sess.connect()
        except OSError:
            return None, None

        q = sess.subscribe()
        try:
            sess.send(type="resume", session_id=sid)
            response = q.get(timeout=10)
        except (queue.Empty, TimeoutError):
            sess.close()
            return None, None
        finally:
            sess.unsubscribe(q)

        if response.get("type") != "welcome":
            sess.close()
            return None, None

        with _sessions_lock:
            _sessions[sid] = sess
        log.info("Sessão %s retomada com sucesso.", sid[:8])
        return sess, response

    def log_message(self, fmt, *args):
        """Silencia logs de acesso HTTP no stdout (erros já aparecem via logging)."""
        pass

    def log_error(self, fmt, *args):
        """Silencia BrokenPipe e outros erros de conexão comuns."""
        msg = fmt % args
        # BrokenPipe é comportamento normal — browser fecha conexão cedo
        if "BrokenPipe" in msg or "ConnectionReset" in msg or "32" in msg:
            return
        log.warning("HTTP error: %s", msg)


# ── Ponto de entrada ──────────────────────────────────────────────────────────

def main() -> None:
    log.info("Proxy HTTP escutando em http://0.0.0.0:%d", PROXY_PORT)
    log.info("Conectando ao servidor TCP em %s:%d", SERVER_HOST, SERVER_PORT)
    srv = ThreadingHTTPServer(("0.0.0.0", PROXY_PORT), ProxyHandler)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log.info("Proxy encerrado.")


if __name__ == "__main__":
    main()
