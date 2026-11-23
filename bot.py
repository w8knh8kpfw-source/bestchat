import os
import json
import logging
import re
from typing import Any

import httpx
import asyncpg
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("bestchat")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
pool: asyncpg.Pool | None = None
app = FastAPI(title="BestChat Business Archive", version="1.1.0")
SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_owner (id integer PRIMARY KEY CHECK (id=1), telegram_user_id bigint NOT NULL UNIQUE, enabled boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS business_messages (business_connection_id text NOT NULL, chat_id bigint NOT NULL, message_id bigint NOT NULL, sender text, chat_title text, text_content text NOT NULL DEFAULT '', caption_content text NOT NULL DEFAULT '', media jsonb NOT NULL DEFAULT '{}'::jsonb, message_date bigint, edited_at bigint, is_edited boolean NOT NULL DEFAULT false, saved_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (business_connection_id,chat_id,message_id));
CREATE TABLE IF NOT EXISTS message_edits (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, business_connection_id text NOT NULL, chat_id bigint NOT NULL, message_id bigint NOT NULL, old_text text NOT NULL DEFAULT '', new_text text NOT NULL DEFAULT '', edited_at bigint, recorded_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS deletion_events (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, business_connection_id text NOT NULL, chat_id bigint NOT NULL, message_id bigint NOT NULL, archived_text text NOT NULL DEFAULT '', sender text, chat_title text, detected_at timestamptz NOT NULL DEFAULT now());
"""
ALLOWED_TABLES = {"bot_owner","business_messages","message_edits","deletion_events"}

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
                keys=list(item); vals=list(item.values())
                if not keys or any(not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", k) for k in keys): raise ValueError("Invalid insert")
                sql="INSERT INTO " + table + " (" + ", ".join(keys) + ") VALUES (" + ", ".join("$"+str(i) for i in range(1,len(vals)+1)) + ")"
                if prefer and "resolution=merge-duplicates" in prefer:
                    updates=[k+"=EXCLUDED."+k for k in keys if k not in {"business_connection_id","chat_id","message_id"}]
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
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram API error: {data}")
        return data.get("result", {})


async def db(method: str, table: str, *, params=None, body=None, prefer=None):
    headers = dict(DB_HEADERS)
    if prefer:
        headers["Prefer"] = prefer
    async with httpx.AsyncClient(timeout=25) as client:
        response = await client.request(
            method,
            f"{SUPABASE_URL}/rest/v1/{table}",
            params=params,
            json=body,
            headers=headers,
        )
        if response.status_code >= 400:
            log.error("Supabase %s %s: %s", method, table, response.text[:1000])
            response.raise_for_status()
        return response.json() if response.content else None


async def owner():
    rows = await db("GET", "bot_owner", params={"id": "eq.1", "select": "telegram_user_id,enabled"})
    return rows[0] if rows else None


async def notify_owner(text: str):
    current = await owner()
    if current and current.get("enabled") and current.get("telegram_user_id"):
        await tg("sendMessage", {
            "chat_id": current["telegram_user_id"],
            "text": text[:3900],
            "disable_web_page_preview": True,
        })


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
        await tg("sendMessage", {
            "chat_id": chat["id"],
            "text": (
                "BestChat подключён.\n\n"
                "Бот архивирует только сообщения из чатов, которые ты явно разрешил "
                "в Telegram Business, и только после включения архива. "
                "Сохраняются полученные обычные текстовые сообщения, изменения и "
                "события удаления. Для удалённого сообщения показывается только "
                "то, что бот успел получить и сохранить ранее.\n\n"
                "Одноразовые и защищённые медиа бот не перехватывает. "
                "Файлы обычных медиа пока не скачиваются.\n\n"
                "Команды: /enable — включить архив, /pause — остановить, /status — статус."
            ),
        })
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
        await tg("setWebhook", {"url":public_url+"/telegram/webhook","secret_token":WEBHOOK_SECRET,"allowed_updates":["message","business_connection","business_message","edited_business_message","deleted_business_messages"],"drop_pending_updates":False})
        log.info("Telegram webhook configured")
    else: log.warning("Add DATABASE_URL, BOT_TOKEN and WEBHOOK_SECRET to enable the bot")
