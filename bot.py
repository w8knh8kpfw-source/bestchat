import os
import json
import logging
import re
import hashlib
from typing import Any

import httpx
import asyncpg
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("bestchat")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_SECRET_RAW = os.getenv("WEBHOOK_SECRET", "")
# Telegram accepts only A-Z, a-z, 0-9, underscore and hyphen.
# Hashing the configured value produces a stable, valid 64-character secret.
WEBHOOK_SECRET = hashlib.sha256(WEBHOOK_SECRET_RAW.encode("utf-8")).hexdigest() if WEBHOOK_SECRET_RAW else ""
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
pool: asyncpg.Pool | None = None
app = FastAPI(title="BestChat Business Archive", version="1.1.0")
SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_owner (id integer PRIMARY KEY CHECK (id=1), telegram_user_id bigint NOT NULL UNIQUE, enabled boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS business_messages (business_connection_id text NOT NULL, chat_id bigint NOT NULL, message_id bigint NOT NULL, sender text, chat_title text, text_content text NOT NULL DEFAULT '', caption_content text NOT NULL DEFAULT '', media jsonb NOT NULL DEFAULT '{}'::jsonb, message_date bigint, edited_at bigint, is_edited boolean NOT NULL DEFAULT false, saved_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (business_connection_id,chat_id,message_id));
CREATE TABLE IF NOT EXISTS message_edits (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, business_connection_id text NOT NULL, chat_id bigint NOT NULL, message_id bigint NOT NULL, old_text text NOT NULL DEFAULT '', new_text text NOT NULL DEFAULT '', edited_at bigint, recorded_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS deletion_events (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, business_connection_id text NOT NULL, chat_id bigint NOT NULL, message_id bigint NOT NULL, archived_text text NOT NULL DEFAULT '', sender text, chat_title text, detected_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS archived_media (business_connection_id text NOT NULL, chat_id bigint NOT NULL, message_id bigint NOT NULL, media_type text NOT NULL, file_name text, mime_type text, file_size bigint, file_bytes bytea NOT NULL, saved_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (business_connection_id,chat_id,message_id));
CREATE TABLE IF NOT EXISTS business_connections (business_connection_id text PRIMARY KEY, user_chat_id bigint, is_enabled boolean NOT NULL DEFAULT false, rights jsonb NOT NULL DEFAULT '{}'::jsonb, connected_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
"""
ALLOWED_TABLES = {"bot_owner","business_messages","message_edits","deletion_events","archived_media","business_connections"}

async def db(method: str, table: str, *, params=None, body=None, prefer=None):
    if pool is None or table not in ALLOWED_TABLES: raise RuntimeError("Database is not ready or table is invalid")
    async with pool.acquire() as conn:
        if method == "GET":
            params = params or {}; columns = params.get("select", "*")
            if not re.fullmatch(r"[a-zA-Z0-9_, *]+", columns): raise ValueError("Invalid select")
            where=[]; values=[]
            for key, raw in params.items():
                if key == "select": continue
                if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", key) or not str(raw).startswith("eq."): raise ValueError("Invalid filter")
                value=str(raw)[3:]; values.append(int(value) if key in {"id","chat_id","message_id"} and value.isdigit() else value); where.append(key + "=$" + str(len(values)))
            sql="SELECT " + columns + " FROM " + table + ((" WHERE " + " AND ".join(where)) if where else "")
            return [dict(row) for row in await conn.fetch(sql,*values)]
        if method == "POST":
            items=body if isinstance(body,list) else [body or {}]
            for item in items:
                if isinstance(item.get("media"), dict): item["media"] = json.dumps(item["media"])
                if isinstance(item.get("rights"), dict): item["rights"] = json.dumps(item["rights"])
                keys=list(item); vals=list(item.values())
                if not keys or any(not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", k) for k in keys): raise ValueError("Invalid insert")
                sql="INSERT INTO " + table + " (" + ", ".join(keys) + ") VALUES (" + ", ".join("$"+str(i) for i in range(1,len(vals)+1)) + ")"
                if prefer and "resolution=merge-duplicates" in prefer:
                    updates=[k+"=EXCLUDED."+k for k in keys if k not in {"business_connection_id","chat_id","message_id"}]
                    if table == "business_connections":
                        sql += " ON CONFLICT (business_connection_id) DO UPDATE SET " + ", ".join(updates)
                    else:
                        sql += " ON CONFLICT (business_connection_id,chat_id,message_id) DO UPDATE SET " + ", ".join(updates)
                elif prefer and "resolution=ignore-duplicates" in prefer: sql += " ON CONFLICT DO NOTHING"
                await conn.execute(sql,*vals)
            return None
        if method == "PATCH":
            data=body or {}; params=params or {}; vals=list(data.values())
            sets=[k+"=$"+str(i) for i,k in enumerate(data,1)]; where=[]
            for key,raw in params.items():
                if not str(raw).startswith("eq."): raise ValueError("Invalid filter")
                value=str(raw)[3:]; vals.append(int(value) if key in {"id","chat_id","message_id"} and value.isdigit() else value); where.append(key+"=$"+str(len(vals)))
            await conn.execute("UPDATE "+table+" SET "+", ".join(sets)+" WHERE "+" AND ".join(where),*vals)
            return None
        raise ValueError("Unsupported database method")

async def tg(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN is not configured")
    async with httpx.AsyncClient(timeout=25) as client:
        response = await client.post(f"{TELEGRAM_API}/{method}", json=payload)
        if response.status_code >= 400:
            log.error("Telegram API %s failed (%s): %s", method, response.status_code, response.text[:500])
            response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram API error: {data}")
        return data.get("result", {})


async def download_media(file_id: str):
    """Download ordinary media exposed to the Business bot; never bypass ephemeral/protected media."""
    try:
        info = await tg("getFile", {"file_id": file_id})
        file_path = info.get("file_path")
        file_size = int(info.get("file_size") or 0)
        if not file_path or file_size > 20 * 1024 * 1024:
            return None
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}")
            response.raise_for_status()
            return response.content
    except Exception:
        # Download URLs contain the bot token; never log exception details.
        log.warning("Ordinary media download failed; metadata remains archived.")
        return None


async def owner():
    rows = await db("GET", "bot_owner", params={"id": "eq.1", "select": "telegram_user_id,enabled"})
    return rows[0] if rows else None


async def notify_owner(text: str, *, require_enabled: bool = True):
    current = await owner()
    if current and current.get("telegram_user_id") and (current.get("enabled") or not require_enabled):
        await tg("sendMessage", {
            "chat_id": current["telegram_user_id"],
            "text": text[:3900],
            "disable_web_page_preview": True,
        })


async def handle_business_connection(connection: dict[str, Any]):
    connection_id = connection.get("id")
    if not connection_id:
        log.warning("Business connection update has no connection id")
        return
    is_enabled = bool(connection.get("is_enabled"))
    rights = connection.get("rights") or {}
    await db("POST", "business_connections", body={
        "business_connection_id": connection_id,
        "user_chat_id": connection.get("user_chat_id"),
        "is_enabled": is_enabled,
        "rights": rights,
    }, prefer="resolution=merge-duplicates,return=minimal")
    # A confirmed active Business connection arms the archive automatically.
    await db("PATCH", "bot_owner", params={"id":"eq.1"},
             body={"enabled": is_enabled}, prefer="return=minimal")
    readable_rights = ", ".join(k for k, v in rights.items() if v) or "права не указаны"
    state = "подключён" if is_enabled else "отключён"
    log.info("Business connection changed: enabled=%s rights=%s", is_enabled, readable_rights)
    await notify_owner(
        f"🔗 Telegram Business {state}.\n"
        f"Права: {readable_rights}\n"
        + ("✅ Архив автоматически включён." if is_enabled else "⏸ Архив остановлен."),
        require_enabled=False,
    )


def media_summary(message: dict[str, Any]) -> dict[str, Any]:
    # Store only Telegram file identifiers and metadata for ordinary media.
    # View-once / protected media is intentionally not downloaded or bypassed.
    for kind in ("photo", "video", "voice", "audio", "video_note", "document", "animation", "sticker"):
        value = message.get(kind)
        if value:
            item = value[-1] if kind == "photo" and isinstance(value, list) else value
            return {
                "type": kind,
                "file_id": item.get("file_id"),
                "file_unique_id": item.get("file_unique_id"),
                "mime_type": item.get("mime_type"),
                "file_name": item.get("file_name"),
                "duration": item.get("duration"),
                "file_size": item.get("file_size"),
            }
    return {}


def message_text(message: dict[str, Any]) -> str:
    return (message.get("text") or message.get("caption") or "").strip()


def sender_label(message: dict[str, Any]) -> str:
    user = message.get("from") or {}
    if user.get("username"):
        return "@" + user["username"]
    name = " ".join(x for x in (user.get("first_name"), user.get("last_name")) if x)
    return name or f"user:{user.get('id', 'unknown')}"


async def save_business_message(message: dict[str, Any], edited: bool = False):
    connection_id = message.get("business_connection_id")
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    if not connection_id or chat_id is None or message_id is None:
        return

    key = {
        "business_connection_id": connection_id,
        "chat_id": chat_id,
        "message_id": message_id,
    }
    existing = await db("GET", "business_messages", params={
        "business_connection_id": f"eq.{connection_id}",
        "chat_id": f"eq.{chat_id}",
        "message_id": f"eq.{message_id}",
        "select": "text_content,caption_content,sender,media",
    })
    text_now = message_text(message)
    caption_now = message.get("caption") or ""
    sender = sender_label(message)
    media = media_summary(message)
    old = existing[0] if existing else None

    if edited and old:
        await db("POST", "message_edits", body={
            **key,
            "old_text": old.get("text_content") or old.get("caption_content") or "",
            "new_text": text_now,
            "edited_at": message.get("edit_date"),
        }, prefer="return=minimal")

    payload = {
        **key,
        "sender": sender,
        "chat_title": chat.get("title") or chat.get("username") or str(chat_id),
        "text_content": text_now,
        "caption_content": caption_now,
        "media": media,
        "message_date": message.get("date"),
        "edited_at": message.get("edit_date"),
        "is_edited": edited or bool(message.get("edit_date")),
    }
    await db("POST", "business_messages", body=payload,
             prefer="resolution=merge-duplicates,return=minimal")

    if media.get("file_id"):
        file_bytes = await download_media(media["file_id"])
        if file_bytes is not None:
            await db("POST", "archived_media", body={
                **key,
                "media_type": media.get("type") or "file",
                "file_name": media.get("file_name"),
                "mime_type": media.get("mime_type"),
                "file_size": len(file_bytes),
                "file_bytes": file_bytes,
            }, prefer="resolution=merge-duplicates,return=minimal")

    if edited and old:
        await notify_owner(
            "✏️ Сообщение изменено\n"
            f"От: {sender}\n"
            f"Чат: {payload['chat_title']}\n\n"
            f"Было: {old.get('text_content') or old.get('caption_content') or '[без текста]'}\n"
            f"Стало: {text_now or '[без текста]'}"
        )


async def handle_deleted(update: dict[str, Any]):
    deleted = update.get("deleted_business_messages") or {}
    connection_id = deleted.get("business_connection_id")
    chat = deleted.get("chat") or {}
    chat_id = chat.get("id")
    if not connection_id or chat_id is None:
        return
    for message_id in deleted.get("message_ids", []):
        rows = await db("GET", "business_messages", params={
            "business_connection_id": f"eq.{connection_id}",
            "chat_id": f"eq.{chat_id}",
            "message_id": f"eq.{message_id}",
            "select": "sender,chat_title,text_content,caption_content,message_date",
        })
        saved = rows[0] if rows else None
        await db("POST", "deletion_events", body={
            "business_connection_id": connection_id,
            "chat_id": chat_id,
            "message_id": message_id,
            "archived_text": ((saved or {}).get("text_content") or
                              (saved or {}).get("caption_content") or ""),
            "sender": (saved or {}).get("sender"),
            "chat_title": (saved or {}).get("chat_title") or chat.get("title") or str(chat_id),
        }, prefer="return=minimal")
        if saved:
            await notify_owner(
                "🗑 Удалённое сообщение\n"
                f"От: {saved.get('sender') or 'неизвестно'}\n"
                f"Чат: {saved.get('chat_title')}\n\n"
                f"{saved.get('text_content') or saved.get('caption_content') or '[медиа или сообщение без текста]'}"
            )
        else:
            await notify_owner(
                "🗑 Сообщение удалено\n"
                f"Чат: {chat.get('title') or chat_id}\n"
                f"ID сообщения: {message_id}\n"
                "Текст не был доступен архиву до удаления."
            )


async def send_home(chat_id: int, text: str):
    await tg("sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": {"inline_keyboard": [
            [{"text": "🔌 Подключить Telegram Business", "callback_data": "connect"}],
            [{"text": "▶️ Включить архив", "callback_data": "enable"}, {"text": "⏸ Пауза", "callback_data": "pause"}],
            [{"text": "📊 Статус", "callback_data": "status"}, {"text": "❔ Помощь", "callback_data": "help"}]
        ]}
    })


async def handle_callback(query: dict[str, Any]):
    data = query.get("data") or ""
    user_id = (query.get("from") or {}).get("id")
    chat_id = ((query.get("message") or {}).get("chat") or {}).get("id")
    if query.get("id"):
        await tg("answerCallbackQuery", {"callback_query_id": query["id"]})
    current = await owner()
    if not current or current.get("telegram_user_id") != user_id:
        if chat_id:
            await tg("sendMessage", {"chat_id": chat_id, "text": "Это меню доступно только владельцу BestChat."})
        return
    if data == "connect":
        answer = ("🔌 ПОДКЛЮЧЕНИЕ TELEGRAM BUSINESS\n\n"
                  "1. Открой @BotFather и выбери BestChat.\n"
                  "2. Включи Business Mode / Secretary Mode в настройках бота.\n"
                  "3. Открой Настройки Telegram → Telegram Business → Чат-боты.\n"
                  "4. Выбери BestChat, укажи доступные чаты и подтверди подключение.\n\n"
                  "Без включённого Secretary Mode Telegram не разрешит подключение.")
    elif data == "enable":
        await db("PATCH", "bot_owner", params={"id":"eq.1"}, body={"enabled":True}, prefer="return=minimal")
        answer = "✅ Архив включён. Сохраняются только данные, которые Telegram передаёт подключённому бизнес-боту."
    elif data == "pause":
        await db("PATCH", "bot_owner", params={"id":"eq.1"}, body={"enabled":False}, prefer="return=minimal")
        answer = "⏸ Архив приостановлен."
    elif data == "status":
        answer = "📊 Архив: " + ("включён" if current.get("enabled") else "приостановлен") + "\nПодключение проверь в настройках Telegram Business."
    else:
        answer = ("❔ BestChat сохраняет обычные бизнес-сообщения, изменения, события удаления, "
                  "а также доступные обычные фото, видео, кружки и документы. "
                  "Одноразовые и защищённые медиа не восстанавливаются.")
    await send_home(chat_id, answer)


async def handle_start(message: dict[str, Any]):
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    if chat.get("type") != "private" or not user.get("id"):
        return
    await db("POST", "bot_owner", body={
        "id": 1,
        "telegram_user_id": user["id"],
        "enabled": False,
    }, prefer="resolution=ignore-duplicates,return=minimal")
    current = await owner()
    if current and current.get("telegram_user_id") == user["id"]:
        await send_home(chat["id"],
            "🚀 Добро пожаловать в BestChat!\n\n"
            "🔥 Возможности бота\n"
            "🗑 Архив событий удаления сообщений\n"
            "✏️ История изменений сообщений\n"
            "📸 Сохранение обычных фото и видео\n"
            "🎥 Поддержка видеосообщений (кружков)\n"
            "📎 Сохранение обычных документов и файлов\n\n"
            "Для начала подключи Telegram Business. Сначала включи Business Mode / "
            "Secretary Mode в @BotFather. Одноразовые и защищённые медиа не восстанавливаются.")
    else:
        await tg("sendMessage", {
            "chat_id": chat["id"],
            "text": "BestChat уже закреплён за другим владельцем. Если это ошибка, проверь настройки базы.",
        })


async def handle_command(message: dict[str, Any], command: str):
    user_id = (message.get("from") or {}).get("id")
    chat_id = (message.get("chat") or {}).get("id")
    current = await owner()
    if not current or current.get("telegram_user_id") != user_id:
        await tg("sendMessage", {"chat_id": chat_id, "text": "Эта команда доступна только владельцу BestChat."})
        return
    if command == "/enable":
        await db("PATCH", "bot_owner", params={"id": "eq.1"},
                 body={"enabled": True}, prefer="return=minimal")
        reply = "✅ Архив включён. Работают только чаты, разрешённые в Telegram Business."
    elif command == "/pause":
        await db("PATCH", "bot_owner", params={"id": "eq.1"},
                 body={"enabled": False}, prefer="return=minimal")
        reply = "⏸ Архив приостановлен."
    else:
        reply = "Статус: " + ("включён" if current.get("enabled") else "приостановлен")
    await tg("sendMessage", {"chat_id": chat_id, "text": reply})


@app.get("/")
async def health():
    return {"ok": True, "service": "bestchat", "mode": "telegram-business"}


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, x_telegram_bot_api_secret_token: str | None = Header(default=None)):
    if not WEBHOOK_SECRET or not x_telegram_bot_api_secret_token or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail="Invalid webhook secret")
    if x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")
    update = await request.json()

    if not BOT_TOKEN or pool is None:
        raise HTTPException(status_code=503, detail="Bot is not configured")
    try:
        event_type = next((key for key in (
            "business_connection", "business_message", "edited_business_message",
            "deleted_business_messages", "callback_query", "message"
        ) if key in update), "unknown")
        log.info("Telegram update received: %s", event_type)
        if update.get("callback_query"):
            await handle_callback(update["callback_query"])
        if update.get("business_connection"):
            await handle_business_connection(update["business_connection"])
        message = update.get("message")
        if message:
            text = (message.get("text") or "").strip()
            if text.startswith("/start"):
                await handle_start(message)
            elif text.split(" ", 1)[0] in ("/enable", "/pause", "/status"):
                await handle_command(message, text.split(" ", 1)[0])
        current = await owner()
        if current and current.get("enabled"):
            if update.get("business_message"):
                await save_business_message(update["business_message"])
            elif update.get("edited_business_message"):
                await save_business_message(update["edited_business_message"], edited=True)
            elif update.get("deleted_business_messages"):
                await handle_deleted(update)
    except Exception:
        log.exception("Failed to process Telegram update")
        raise HTTPException(status_code=500, detail="Update processing failed")
    return JSONResponse({"ok": True})


@app.on_event("startup")
async def configure_webhook():
    global pool
    if DATABASE_URL:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, command_timeout=30)
        async with pool.acquire() as conn: await conn.execute(SCHEMA)
        log.info("PostgreSQL connected; schema ready")
    else: log.warning("DATABASE_URL is missing")
    public_url=os.getenv("RENDER_EXTERNAL_URL","").rstrip("/")
    if public_url and BOT_TOKEN and WEBHOOK_SECRET and pool is not None:
        try:
            result = await tg("setWebhook", {
                "url": public_url + "/telegram/webhook",
                "secret_token": WEBHOOK_SECRET,
                "allowed_updates": ["message", "callback_query", "business_connection", "business_message", "edited_business_message", "deleted_business_messages"],
                "drop_pending_updates": False,
            })
            log.info("Telegram webhook configured: %s", result)
        except httpx.HTTPStatusError as exc:
            # Do not log the exception traceback: httpx includes the bot-token URL in it.
            log.error("Webhook registration failed with HTTP %s. App remains online.", exc.response.status_code)
        except Exception:
            log.exception("Webhook registration failed unexpectedly. App remains online.")
    else:
        log.warning("Add DATABASE_URL, BOT_TOKEN and WEBHOOK_SECRET to enable the bot")
