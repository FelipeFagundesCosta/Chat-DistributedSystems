# ChatNet v2

Chat distribuído em tempo real. **Zero WebSocket** — browser usa `fetch` + `EventSource` (SSE).

## Arquitetura

```
Browser
  │  POST /login, /message, /logout   (fetch HTTP normal)
  │  GET  /events?sid=...             (EventSource — SSE, não WebSocket)
  ▼
proxy.py  (HTTP + TCP, porta 8080)
  │  socket TCP manual
  │  threading.Thread por sessão (thread de recepção dedicada)
  ▼
server.py  (TCP puro, porta 5000)
  │  socket + threading — uma thread por conexão
  │  NDJSON sobre TCP (JSON + \n, sem frames binários)
  ▼
Redis  (pub/sub entre instâncias + histórico + sessões)
```

### Por que SSE e não WebSocket?

SSE (`EventSource`) é HTTP puro com conexão mantida aberta — o servidor envia
linhas `data: {...}\n\n` e o browser lê em stream. Não há handshake especial,
não há frames binários, não há RFC 6455. O envio de mensagens usa `fetch POST`
normal. Resultado: zero WebSocket em qualquer camada do código JS.

### Threads implementadas manualmente

- **Servidor TCP**: `ClientSession(threading.Thread)` — uma thread por conexão
- **Proxy (cliente)**: `TCPSession._recv_thread` — thread dedicada à recepção por sessão
- **Redis pub/sub**: thread daemon dedicada ao subscriber

## Instalação

```bash
pip install -r requirements.txt
```

Redis local (Docker):
```bash
docker run -d -p 6379:6379 redis:7-alpine
```

## Execução local

```bash
# Terminal 1 — servidor de chat TCP
REDIS_URL=redis://localhost:6379 python server.py

# Terminal 2 — proxy HTTP (pode rodar múltiplos para escalar)
SERVER_HOST=localhost SERVER_PORT=5000 PROXY_PORT=8080 python proxy.py

# Abrir no browser
http://localhost:8080
```

## Múltiplas instâncias (tolerância a falhas)

```bash
# Dois servidores TCP — compartilham Redis
REDIS_URL=redis://... PORT=5000 python server.py
REDIS_URL=redis://... PORT=5001 python server.py

# Dois proxies — cada um aponta para um servidor
SERVER_PORT=5000 PROXY_PORT=8080 python proxy.py
SERVER_PORT=5001 PROXY_PORT=8081 python proxy.py
```

O Redis sincroniza mensagens entre instâncias via pub/sub.
Se um servidor cair, o proxy do outro continua atendendo normalmente.

## Deploy (Render + Redis Cloud)

| Arquivo      | Onde                  | Variáveis de ambiente                        |
|--------------|-----------------------|----------------------------------------------|
| server.py    | Render Web Service    | REDIS_URL, PORT                              |
| proxy.py     | Render Web Service    | SERVER_HOST, SERVER_PORT, PROXY_PORT         |
| index.html   | Cloudflare Pages      | (nenhuma — servido pelo proxy também)        |
| Redis        | Redis Cloud free tier | (URL usada pelos dois serviços acima)        |
