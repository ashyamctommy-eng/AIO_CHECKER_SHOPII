import asyncio
import json
import logging
import os
import random
import re
import secrets
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Awaitable, List, Optional, Tuple

import aiohttp
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    TelegramObject,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

# ── Configuration ───────────────────────────────────────────────────
BOT_TOKEN  = os.getenv("BOT_TOKEN", "9172966238:AAGv6OZsLaO_BwabWunPGlvxgGHpnUDs4Xg")
MONGO_URI  = os.getenv("MONGO_URI", "")  # e.g. "mongodb+srv://user:pass@cluster.mongodb.net/"
OWNER_ID   = int(os.getenv("OWNER_ID", "5541778617"))  # Replace with your Telegram User ID

CACHE_FILE = "service_cache.json"
PROXY_FILE = "proxies.txt"
JSON_DB    = "auth_db.json"

_M = "https://morethanpanel.com"
_T = "https://tgpanel.org"

USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 15; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.88 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.6533.103 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Redmi Note 13 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.122 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
]

SERVICES: List[Tuple[str, str, str, str, str]] = [
    ("Instagram Likes",     _M, "instagram-likes",     "50",  "26664"),
    ("Instagram Views",     _M, "instagram-views",     "100", ""),
    ("Instagram Followers", _M, "instagram-followers", "20",  ""),
    ("TikTok Likes",        _M, "tiktok-likes",        "50",  ""),
    ("TikTok Views",        _M, "tiktok-views",        "100", ""),
    ("TikTok Followers",    _M, "tiktok-followers",    "20",  ""),
    ("Facebook Likes",      _M, "facebook-likes",      "50",  ""),
    ("Facebook Followers",  _M, "facebook-followers",  "50",  ""),
    ("YouTube Likes",       _M, "youtube-likes",       "50",  ""),
    ("YouTube Subscribers", _M, "youtube-subscribers", "20",  ""),
    ("Telegram Reactions",  _T, "telegram-reaction",   "50",  "22176"),
    ("Telegram Views",      _T, "telegram-view",       "50",  ""),
    ("Telegram Members",    _T, "telegram-members",    "50",  ""),
]

PLATFORMS: Dict[str, Tuple[str, List[int]]] = {
    "ig": ("📸 Instagram", [0, 1, 2]),
    "tt": ("🎵 TikTok",    [3, 4, 5]),
    "fb": ("📘 Facebook",  [6, 7]),
    "yt": ("▶️ YouTube",   [8, 9]),
    "tg": ("✈️ Telegram",  [10, 11, 12]),
}

# ── Hybrid Database Engine (MongoDB + JSON Fallback) ────────────────
class AuthDB:
    def __init__(self, mongo_uri: str, json_path: str):
        self.use_mongo = False
        self.json_path = json_path
        self.lock = threading.Lock()

        if mongo_uri and MONGO_AVAILABLE:
            try:
                self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
                self.client.admin.command("ping")
                self.db = self.client["smm_booster_db"]
                self.users = self.db["users"]
                self.keys = self.db["keys"]
                self.use_mongo = True
                logging.info("Connected to MongoDB Atlas successfully.")
            except Exception as e:
                logging.warning(f"MongoDB connection failed ({e}). Falling back to local JSON.")

        if not self.use_mongo:
            self._init_json()

    def _init_json(self):
        if not os.path.exists(self.json_path):
            with open(self.json_path, "w") as f:
                json.dump({"users": {}, "keys": {}}, f, indent=2)

    def _read_json(self) -> dict:
        with self.lock:
            try:
                with open(self.json_path, "r") as f:
                    return json.load(f)
            except Exception:
                return {"users": {}, "keys": {}}

    def _write_json(self, data: dict):
        with self.lock:
            with open(self.json_path, "w") as f:
                json.dump(data, f, indent=2)

    def get_user(self, user_id: int, username: str = "") -> dict:
        now = int(time.time())
        default_role = "OWNER" if user_id == OWNER_ID else "FREE"
        default_exp = 9999999999 if user_id == OWNER_ID else 0

        if self.use_mongo:
            u = self.users.find_one({"user_id": user_id})
            if not u:
                u = {
                    "user_id": user_id,
                    "username": username,
                    "role": default_role,
                    "expires_at": default_exp,
                    "joined_at": now,
                }
                self.users.insert_one(u)
            elif user_id == OWNER_ID and u.get("role") != "OWNER":
                self.users.update_one({"user_id": user_id}, {"$set": {"role": "OWNER", "expires_at": default_exp}})
                u["role"] = "OWNER"
            return u
        else:
            data = self._read_json()
            uid_str = str(user_id)
            if uid_str not in data["users"]:
                data["users"][uid_str] = {
                    "user_id": user_id,
                    "username": username,
                    "role": default_role,
                    "expires_at": default_exp,
                    "joined_at": now,
                }
                self._write_json(data)
            elif user_id == OWNER_ID and data["users"][uid_str].get("role") != "OWNER":
                data["users"][uid_str]["role"] = "OWNER"
                data["users"][uid_str]["expires_at"] = default_exp
                self._write_json(data)
            return data["users"][uid_str]

    def set_role(self, user_id: int, role: str, duration_sec: int = 0):
        exp = int(time.time()) + duration_sec if duration_sec > 0 else (9999999999 if role in ("OWNER", "ADMIN") else 0)
        if self.use_mongo:
            self.users.update_one(
                {"user_id": user_id},
                {"$set": {"role": role, "expires_at": exp}},
                upsert=True,
            )
        else:
            data = self._read_json()
            uid_str = str(user_id)
            u = data["users"].get(uid_str, {"user_id": user_id, "joined_at": int(time.time())})
            u["role"] = role
            u["expires_at"] = exp
            data["users"][uid_str] = u
            self._write_json(data)

    def create_key(self, duration_sec: int, label: str, created_by: int) -> str:
        code = f"BOOST-{secrets.token_hex(3).upper()}-{secrets.token_hex(3).upper()}"
        key_doc = {
            "code": code,
            "duration_sec": duration_sec,
            "label": label,
            "created_by": created_by,
            "created_at": int(time.time()),
        }
        if self.use_mongo:
            self.keys.insert_one(key_doc)
        else:
            data = self._read_json()
            data["keys"][code] = key_doc
            self._write_json(data)
        return code

    def redeem_key(self, user_id: int, code: str) -> Tuple[bool, str]:
        code = code.strip().upper()
        now = int(time.time())

        if self.use_mongo:
            key_doc = self.keys.find_one_and_delete({"code": code})
        else:
            data = self._read_json()
            key_doc = data["keys"].pop(code, None)
            if key_doc:
                self._write_json(data)

        if not key_doc:
            return False, "Invalid or already redeemed key."

        u = self.get_user(user_id)
        current_exp = u.get("expires_at", 0)
        base_time = max(now, current_exp) if u.get("role") == "PREMIUM" else now
        new_exp = base_time + key_doc["duration_sec"]

        new_role = u["role"] if u["role"] in ("OWNER", "ADMIN") else "PREMIUM"
        if self.use_mongo:
            self.users.update_one(
                {"user_id": user_id},
                {"$set": {"role": new_role, "expires_at": new_exp}},
            )
        else:
            data = self._read_json()
            data["users"][str(user_id)]["role"] = new_role
            data["users"][str(user_id)]["expires_at"] = new_exp
            self._write_json(data)

        exp_str = datetime.fromtimestamp(new_exp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return True, f"Key redeemed ({key_doc['label']})! Valid until: {exp_str}"

    def cleanup_expired(self) -> int:
        now = int(time.time())
        if self.use_mongo:
            res = self.users.update_many(
                {"role": "PREMIUM", "expires_at": {"$lt": now}},
                {"$set": {"role": "FREE", "expires_at": 0}},
            )
            return res.modified_count
        else:
            data = self._read_json()
            count = 0
            for uid, u in data["users"].items():
                if u.get("role") == "PREMIUM" and u.get("expires_at", 0) < now:
                    u["role"] = "FREE"
                    u["expires_at"] = 0
                    count += 1
            if count > 0:
                self._write_json(data)
            return count

db = AuthDB(MONGO_URI, JSON_DB)

# ── Background Expiry Monitor Thread ────────────────────────────────
def start_expiry_daemon():
    def _worker():
        while True:
            try:
                expired_count = db.cleanup_expired()
                if expired_count:
                    logging.info(f"[ExpiryDaemon] Downgraded {expired_count} expired PREMIUM user(s).")
            except Exception as e:
                logging.error(f"[ExpiryDaemon] Error: {e}")
            time.sleep(60)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()

# ── Duration Parser Helper ──────────────────────────────────────────
def parse_duration(dur_str: str) -> Optional[Tuple[int, str]]:
    """Parses strings like '30m', '12h', '7d' into (seconds, label)."""
    m = re.match(r"^(\d+)([mhd])$", dur_str.strip().lower())
    if not m:
        return None
    val, unit = int(m.group(1)), m.group(2)
    mult = {"m": (60, "Minute(s)"), "h": (3600, "Hour(s)"), "d": (86400, "Day(s)")}[unit]
    return val * mult[0], f"{val} {mult[1]}"

def is_authorized(user_doc: dict) -> bool:
    role = user_doc.get("role", "FREE")
    if role in ("OWNER", "ADMIN"):
        return True
    if role == "PREMIUM" and user_doc.get("expires_at", 0) > int(time.time()):
        return True
    return False

# ── Aiogram Auth Middleware ─────────────────────────────────────────
class RoleAuthMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)

        user_doc = db.get_user(user.id, user.username or "")
        # Real-time expiry check before handling event
        if user_doc.get("role") == "PREMIUM" and user_doc.get("expires_at", 0) <= int(time.time()):
            db.set_role(user.id, "FREE", 0)
            user_doc["role"] = "FREE"
            user_doc["expires_at"] = 0

        data["user_doc"] = user_doc
        return await handler(event, data)

# ── State & File Helpers ────────────────────────────────────────────
class OrderState(StatesGroup):
    waiting_for_link = State()
    waiting_for_key = State()

def load_cache() -> Dict[str, str]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    cache = {slug: pid for _, _, slug, _, pid in SERVICES if pid}
    save_cache(cache)
    return cache

def save_cache(cache: Dict[str, str]) -> None:
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception:
        pass

def load_proxies() -> List[str]:
    if not os.path.exists(PROXY_FILE):
        return []
    with open(PROXY_FILE, "r") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return [p if "://" in p else f"http://{p}" for p in lines]

def normalize_link(link: str) -> str:
    link = link.strip()
    if link.lower().startswith("https://"):
        link = "https://" + link[8:]
    if ("instagram.com/" in link or "t.me/" in link) and "?" in link:
        link = link.split("?")[0]
    return link

# ── Async Network Engine ────────────────────────────────────────────
async def fetch_service_id(
    session: aiohttp.ClientSession, site: str, slug: str, proxy: Optional[str] = None
) -> Optional[str]:
    url = f"{site}/free-{slug}"
    try:
        async with session.get(url, proxy=proxy, timeout=aiohttp.ClientTimeout(total=12)) as resp:
            txt = await resp.text()
            for pat in (
                r'name=["\']provider_service_id["\'][^>]*value=["\'](\d+)["\']',
                r'value=["\'](\d+)["\'][^>]*name=["\']provider_service_id["\']',
                r'"provider_service_id"\s*:\s*"?(\d+)"?',
            ):
                m = re.search(pat, txt)
                if m:
                    return m.group(1)
    except Exception:
        pass
    return None

async def execute_order_async(
    idx: int, link: str, cache: Dict[str, str], proxies: List[str], max_retries: int = 3
) -> Tuple[bool, str]:
    name, site, slug, qty, default_pid = SERVICES[idx]
    path = f"{site}/free-{slug}"
    api = "https://free.socialfruit.co/api/mtp" if site == _M else "https://test.socialfruit.co/api/gateway"
    pid = cache.get(slug) or default_pid
    attempts = max_retries if proxies else 1

    for attempt in range(1, attempts + 1):
        proxy = random.choice(proxies) if proxies else None
        ua = random.choice(USER_AGENTS)

        async with aiohttp.ClientSession(headers={"User-Agent": ua}) as session:
            if not pid:
                pid = await fetch_service_id(session, site, slug, proxy)
                if pid:
                    cache[slug] = pid
                    save_cache(cache)

            if not pid:
                return False, "Service ID could not be scraped from panel."

            headers = {
                "origin": site,
                "referer": path,
                "x-requested-with": "mark.via.gp",
                "accept": "application/json, text/plain, */*",
            }
            payload = {
                "link": link,
                "quantity": qty,
                "provider_service_id": pid,
                "username": "guest",
            }

            try:
                async with session.post(
                    api, headers=headers, json=payload, proxy=proxy, timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    data = await resp.json(content_type=None)
            except Exception as e:
                data = {"status": "error", "message": f"Connection Error ({type(e).__name__})"}

            if isinstance(data, dict) and data.get("status") == "success":
                return True, data.get("message", "Order queued successfully!")

            err_msg = str(data.get("message", "IP Limit Reached / Rejected")) if isinstance(data, dict) else "Invalid Response"
            if attempt == attempts:
                return False, err_msg
            await asyncio.sleep(0.5)

    return False, "Max retries exceeded."

# ── Inline Keyboards ────────────────────────────────────────────────
def main_menu_kb(user_doc: dict) -> InlineKeyboardMarkup:
    if not is_authorized(user_doc):
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔑 Redeem Subscription Key", callback_data="action:redeem_prompt")],
                [InlineKeyboardButton(text="👤 My Account Status", callback_data="action:profile")],
            ]
        )

    builder = InlineKeyboardBuilder()
    for key, (label, _) in PLATFORMS.items():
        builder.button(text=label, callback_data=f"plat:{key}")
    builder.adjust(2, 2, 1)
    builder.row(
        InlineKeyboardButton(text="👤 My Plan", callback_data="action:profile"),
        InlineKeyboardButton(text="🔎 Refresh IDs", callback_data="action:scan"),
    )
    return builder.as_markup()

def platform_menu_kb(plat_key: str, cache: Dict[str, str]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    _, indices = PLATFORMS[plat_key]
    for idx in indices:
        name, _, slug, qty, default_pid = SERVICES[idx]
        short_name = name.split(" ", 1)[1]
        has_id = "✅" if (cache.get(slug) or default_pid) else "⚡"
        builder.button(text=f"{has_id} {short_name} (+{qty})", callback_data=f"svc:{idx}")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="⬅️ Back to Main Menu", callback_data="action:home"))
    return builder.as_markup()

def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="action:home")]]
    )

# ── Bot Handlers ────────────────────────────────────────────────────
router = Router()

def get_dashboard_text(user_doc: dict, proxies: List[str], cache: Dict[str, str]) -> str:
    role = user_doc.get("role", "FREE")
    exp = user_doc.get("expires_at", 0)
    if role in ("OWNER", "ADMIN"):
        exp_str = "Lifetime"
    elif role == "PREMIUM" and exp > 0:
        rem_hours = max(0, (exp - int(time.time())) // 3600)
        exp_str = f"{rem_hours}h remaining"
    else:
        exp_str = "Expired / None"

    db_mode = "MongoDB Atlas" if db.use_mongo else "Local JSON"
    auth_banner = (
        "👇 <i>Select a platform below to boost:</i>"
        if is_authorized(user_doc)
        else "🔒 <b>Access Locked:</b> Redeem a key via <code>/redeem KEY</code> or click below."
    )

    return (
        "🚀 <b>ALL-IN-ONE SOCIAL BOOSTER BOT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Role:</b> <code>{role}</code> ({exp_str})\n"
        f"🗄 <b>Storage:</b> <code>{db_mode}</code>\n"
        f"🌐 <b>Proxies:</b> <code>{len(proxies)}</code> │ 🗂 <b>IDs:</b> <code>{len(cache)}/{len(SERVICES)}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{auth_banner}"
    )

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, user_doc: dict):
    await state.clear()
    await message.answer(
        get_dashboard_text(user_doc, load_proxies(), load_cache()),
        reply_markup=main_menu_kb(user_doc),
    )

# ── Key & Admin Commands ────────────────────────────────────────────
@router.message(Command("genkey"))
async def cmd_genkey(message: Message, command: CommandObject, user_doc: dict):
    if user_doc.get("role") not in ("OWNER", "ADMIN"):
        await message.answer("⛔ <b>Access Denied:</b> Only OWNER or ADMIN can generate keys.")
        return

    args = (command.args or "").split()
    dur_str = args[0] if args else "7d"
    count = min(int(args[1]), 20) if len(args) > 1 and args[1].isdigit() else 1

    parsed = parse_duration(dur_str)
    if not parsed:
        await message.answer(
            "⚠️ <b>Usage:</b> <code>/genkey [duration] [count]</code>\n"
            "Examples: <code>/genkey 30m</code>, <code>/genkey 24h</code>, <code>/genkey 7d 5</code>"
        )
        return

    dur_sec, label = parsed
    keys = [f"<code>{db.create_key(dur_sec, label, message.from_user.id)}</code>" for _ in range(count)]
    await message.answer(
        f"🔑 <b>Generated {count} Key(s) ({label}):</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(keys)
    )

@router.message(Command("redeem"))
async def cmd_redeem(message: Message, command: CommandObject, user_doc: dict):
    if not command.args:
        await message.answer("⚠️ <b>Usage:</b> <code>/redeem BOOST-XXXXXX-XXXXXX</code>")
        return
    ok, msg = db.redeem_key(message.from_user.id, command.args)
    updated_doc = db.get_user(message.from_user.id)
    icon = "✅" if ok else "❌"
    await message.answer(f"{icon} <b>{msg}</b>", reply_markup=main_menu_kb(updated_doc))

@router.message(Command("promote"))
async def cmd_promote(message: Message, command: CommandObject, user_doc: dict):
    if user_doc.get("role") != "OWNER":
        await message.answer("⛔ <b>Only the OWNER can promote Admins.</b>")
        return
    if not command.args or not command.args.strip().isdigit():
        await message.answer("⚠️ <b>Usage:</b> <code>/promote [user_id]</code>")
        return
    target_id = int(command.args.strip())
    db.set_role(target_id, "ADMIN")
    await message.answer(f"✅ User <code>{target_id}</code> promoted to <b>ADMIN</b>.")

@router.message(Command("revoke"))
async def cmd_revoke(message: Message, command: CommandObject, user_doc: dict):
    if user_doc.get("role") not in ("OWNER", "ADMIN"):
        return
    if not command.args or not command.args.strip().isdigit():
        await message.answer("⚠️ <b>Usage:</b> <code>/revoke [user_id]</code>")
        return
    target_id = int(command.args.strip())
    if target_id == OWNER_ID:
        await message.answer("❌ Cannot revoke OWNER.")
        return
    db.set_role(target_id, "FREE", 0)
    await message.answer(f"✅ User <code>{target_id}</code> downgraded to <b>FREE</b>.")

# ── Callback Handlers ───────────────────────────────────────────────
@router.callback_query(F.data == "action:home")
async def cb_home(call: CallbackQuery, state: FSMContext, user_doc: dict):
    await state.clear()
    await call.message.edit_text(
        get_dashboard_text(user_doc, load_proxies(), load_cache()),
        reply_markup=main_menu_kb(user_doc),
    )
    await call.answer()

@router.callback_query(F.data == "action:profile")
async def cb_profile(call: CallbackQuery, user_doc: dict):
    exp = user_doc.get("expires_at", 0)
    exp_str = (
        "Lifetime"
        if user_doc["role"] in ("OWNER", "ADMIN")
        else (datetime.fromtimestamp(exp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if exp > 0 else "None")
    )
    text = (
        "👤 <b>USER PROFILE</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <b>User ID:</b> <code>{user_doc['user_id']}</code>\n"
        f"🎖 <b>Role:</b> <code>{user_doc['role']}</code>\n"
        f"⏳ <b>Expires:</b> <code>{exp_str}</code>"
    )
    await call.message.edit_text(text, reply_markup=cancel_kb())
    await call.answer()

@router.callback_query(F.data == "action:redeem_prompt")
async def cb_redeem_prompt(call: CallbackQuery, state: FSMContext):
    await state.set_state(OrderState.waiting_for_key)
    await call.message.edit_text(
        "🔑 <b>Send your subscription key now:</b>\n"
        "<i>Format:</i> <code>BOOST-XXXXXX-XXXXXX</code>",
        reply_markup=cancel_kb(),
    )
    await call.answer()

@router.message(OrderState.waiting_for_key, F.text)
async def process_key_input(message: Message, state: FSMContext):
    await state.clear()
    ok, msg = db.redeem_key(message.from_user.id, message.text)
    updated_doc = db.get_user(message.from_user.id)
    icon = "✅" if ok else "❌"
    await message.answer(f"{icon} <b>{msg}</b>", reply_markup=main_menu_kb(updated_doc))

@router.callback_query(F.data.startswith("plat:"))
async def cb_platform(call: CallbackQuery, user_doc: dict):
    if not is_authorized(user_doc):
        await call.answer("🔒 Active subscription required!", show_alert=True)
        return
    plat_key = call.data.split(":")[1]
    label, _ = PLATFORMS[plat_key]
    await call.message.edit_text(
        f"<b>{label} Services</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSelect the service package you want to push:",
        reply_markup=platform_menu_kb(plat_key, load_cache()),
    )
    await call.answer()

@router.callback_query(F.data.startswith("svc:"))
async def cb_service(call: CallbackQuery, state: FSMContext, user_doc: dict):
    if not is_authorized(user_doc):
        await call.answer("🔒 Active subscription required!", show_alert=True)
        return
    idx = int(call.data.split(":")[1])
    name, _, _, qty, _ = SERVICES[idx]
    await state.update_data(service_idx=idx)
    await state.set_state(OrderState.waiting_for_link)
    await call.message.edit_text(
        f"🎯 <b>Selected:</b> {name} (<code>+{qty}</code>)\n━━━━━━━━━━━━━━━━━━━━━━\n🔗 <b>Send the target Post or Profile link now:</b>",
        reply_markup=cancel_kb(),
    )
    await call.answer()

@router.message(OrderState.waiting_for_link, F.text)
async def process_link(message: Message, state: FSMContext, user_doc: dict):
    if not is_authorized(user_doc):
        await state.clear()
        await message.answer("🔒 <b>Your subscription has expired.</b>", reply_markup=main_menu_kb(user_doc))
        return

    data = await state.get_data()
    idx = data.get("service_idx")
    await state.clear()

    link = normalize_link(message.text)
    if not link.startswith("http"):
        await message.answer("⚠️ <b>Invalid URL!</b>", reply_markup=main_menu_kb(user_doc))
        return

    name, _, _, qty, _ = SERVICES[idx]
    status_msg = await message.answer(
        f"⏳ <b>Submitting Order...</b>\n• <b>Service:</b> {name} (+{qty})\n• <b>Target:</b> <code>{link}</code>",
        disable_web_page_preview=True,
    )

    ok, resp_msg = await execute_order_async(idx, link, load_cache(), load_proxies())
    result_text = (
        f"✅ <b>ORDER PUSHED SUCCESSFULLY!</b>\n━━━━━━━━━━━━━━━━━━━━━━\n📦 <b>Service:</b> {name} (+{qty})\n🔗 <b>Link:</b> <code>{link}</code>"
        if ok
        else f"❌ <b>ORDER FAILED</b>\n━━━━━━━━━━━━━━━━━━━━━━\n📦 <b>Service:</b> {name}\n⚠️ <b>Reason:</b> <code>{resp_msg}</code>"
    )
    await status_msg.edit_text(result_text, reply_markup=main_menu_kb(user_doc), disable_web_page_preview=True)

@router.callback_query(F.data == "action:scan")
async def cb_scan_ids(call: CallbackQuery, user_doc: dict):
    if not is_authorized(user_doc):
        await call.answer("🔒 Active subscription required!", show_alert=True)
        return
    await call.answer("Scanning all endpoints...")
    await call.message.edit_text("🔎 <b>Scanning all 13 endpoints concurrently...</b>")

    cache = load_cache()
    proxies = load_proxies()

    async def _probe(idx: int):
        name, site, slug, _, default_pid = SERVICES[idx]
        proxy = random.choice(proxies) if proxies else None
        async with aiohttp.ClientSession(headers={"User-Agent": random.choice(USER_AGENTS)}) as session:
            found = await fetch_service_id(session, site, slug, proxy) or default_pid
            return slug, name, found

    results = await asyncio.gather(*(_probe(i) for i in range(len(SERVICES))))
    lines = ["🔎 <b>Service ID Scan Results:</b>\n━━━━━━━━━━━━━━━━━━━━━━"]
    for slug, name, found in results:
        if found:
            cache[slug] = found
            lines.append(f"✅ <b>{name}:</b> <code>{found}</code>")
        else:
            lines.append(f"❌ <b>{name}:</b> <code>Not Found</code>")

    save_cache(cache)
    await call.message.edit_text("\n".join(lines), reply_markup=main_menu_kb(user_doc))

# ── Entry Point ─────────────────────────────────────────────────────
async def main():
    logging.basicConfig(level=logging.INFO)
    start_expiry_daemon()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.message.middleware(RoleAuthMiddleware())
    dp.callback_query.middleware(RoleAuthMiddleware())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
