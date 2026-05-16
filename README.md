# ChatNet MVP

Chat em tempo real. Zero dependências externas. O browser é o cliente.

## Rodar localmente

```bash
# Terminal 1
python server_secondary.py

# Terminal 2
python server_primary.py

# Abrir no browser
http://localhost:5000
```

Testar failover: Ctrl+C no primário → aguardar ~6s → secundário assume.

## Deploy em nuvem

| Arquivo         | Onde hospedar       | Como                          |
|-----------------|---------------------|-------------------------------|
| index.html      | Cloudflare Pages    | Conectar repo Git → deploy automático |
| server_primary.py + ws_protocol.py  | Render Web Service #1 | Start: `python server_primary.py` |
| server_secondary.py + ws_protocol.py | Render Web Service #2 | Start: `python server_secondary.py` |

### Variáveis de ambiente no Render

**Servidor primário:**
```
PORT=5000
SECONDARY_URL=https://chatnet-secondary.onrender.com
```

**Servidor secundário:**
```
PORT=5001
```

### Após o deploy, editar index.html

Trocar as URLs dos servidores:
```javascript
const SERVERS = {
  primary:   'wss://chatnet-primary.onrender.com',
  secondary: 'wss://chatnet-secondary.onrender.com',
};
```

### Keep-alive (evita cold start no Render free tier)

Criar dois jobs em cron-job.org:
- `https://chatnet-primary.onrender.com/ping`   — a cada 10 minutos
- `https://chatnet-secondary.onrender.com/ping` — a cada 10 minutos
