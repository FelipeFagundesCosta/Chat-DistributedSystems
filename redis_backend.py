"""
redis_backend.py — Estado compartilhado entre instâncias via Redis
==================================================================
Responsável por:
  - Sessões de usuário (TTL de 5 minutos, renovável via ping)
  - Histórico das últimas N mensagens
  - Pub/sub para broadcast entre instâncias do servidor
"""

import json
import logging
import threading
from typing import Any, Callable

import redis

log = logging.getLogger(__name__)

HISTORY_KEY    = "chat:history"
SESSION_PREFIX = "chat:session:"
USER_PREFIX    = "chat:user:"
SESSION_TTL    = 300
HISTORY_MAX    = 100
PUBSUB_CHANNEL = "chat:events"


class RedisBackend:

    def __init__(self, url: str) -> None:
        # Guarda a URL explicitamente — usada pelo subscriber
        self._url = url
        self._r   = redis.Redis.from_url(url, decode_responses=True)

    def ping(self) -> None:
        self._r.ping()

    # ── Sessões ───────────────────────────────────────────────────────────────

    def claim_username(self, username: str, session_id: str) -> bool:
        user_key = f"{USER_PREFIX}{username}"
        existing = self._r.get(user_key)

        if existing is None:
            pipe = self._r.pipeline()
            pipe.setex(user_key, SESSION_TTL, session_id)
            pipe.setex(f"{SESSION_PREFIX}{session_id}", SESSION_TTL, username)
            pipe.execute()
            return True

        if existing == session_id:
            self.refresh_session(session_id)
            return True

        if not self._r.exists(f"{SESSION_PREFIX}{existing}"):
            pipe = self._r.pipeline()
            pipe.delete(f"{SESSION_PREFIX}{existing}")
            pipe.setex(user_key, SESSION_TTL, session_id)
            pipe.setex(f"{SESSION_PREFIX}{session_id}", SESSION_TTL, username)
            pipe.execute()
            return True

        return False

    def get_username(self, session_id: str) -> str | None:
        return self._r.get(f"{SESSION_PREFIX}{session_id}")

    def refresh_session(self, session_id: str) -> bool:
        key      = f"{SESSION_PREFIX}{session_id}"
        username = self._r.get(key)
        if not username:
            return False
        self._r.expire(key, SESSION_TTL)
        self._r.expire(f"{USER_PREFIX}{username}", SESSION_TTL)
        return True

    def remove_session(self, session_id: str) -> str | None:
        key      = f"{SESSION_PREFIX}{session_id}"
        username = self._r.get(key)
        if not username:
            return None
        pipe = self._r.pipeline()
        pipe.delete(key)
        pipe.delete(f"{USER_PREFIX}{username}")
        pipe.execute()
        return username

    def get_online_users(self) -> list[str]:
        keys = self._r.keys(f"{USER_PREFIX}*")
        return [k.removeprefix(USER_PREFIX) for k in keys]

    # ── Histórico ─────────────────────────────────────────────────────────────

    def append_history(self, entry: dict) -> None:
        pipe = self._r.pipeline()
        pipe.lpush(HISTORY_KEY, json.dumps(entry, ensure_ascii=False))
        pipe.ltrim(HISTORY_KEY, 0, HISTORY_MAX - 1)
        pipe.execute()

    def get_history(self) -> list[dict]:
        raw = self._r.lrange(HISTORY_KEY, 0, HISTORY_MAX - 1)
        result = []
        for item in reversed(raw):
            try:
                result.append(json.loads(item))
            except json.JSONDecodeError:
                pass
        return result

    # ── Pub/Sub ───────────────────────────────────────────────────────────────

    def publish(self, payload: dict) -> None:
        self._r.publish(PUBSUB_CHANNEL, json.dumps(payload, ensure_ascii=False))

    def start_subscriber(
        self,
        on_message: Callable[[dict], None],
        stop_event: threading.Event,
    ) -> threading.Thread:
        """
        Inicia thread dedicada que escuta o canal pub/sub Redis.
        Usa self._url diretamente — sem tentar reconstruir a URL do pool.
        """
        url = self._url  # captura antes de entrar na thread

        def _run():
            # Cria conexão Redis própria para o subscriber
            # (pub/sub bloqueia a conexão; não pode compartilhar com o cliente principal)
            client = redis.Redis.from_url(url, decode_responses=True)
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(PUBSUB_CHANNEL)
            log.info("[Redis] Inscrito no canal %s", PUBSUB_CHANNEL)
            try:
                for msg in pubsub.listen():
                    if stop_event.is_set():
                        break
                    if not isinstance(msg, dict) or msg.get("type") != "message":
                        continue
                    data = msg.get("data")
                    if not isinstance(data, str):
                        continue
                    try:
                        payload = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(payload, dict):
                        on_message(payload)
            finally:
                pubsub.close()
                client.close()

        t = threading.Thread(target=_run, name="redis-pubsub", daemon=True)
        t.start()
        return t
