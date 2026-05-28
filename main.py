# v230: Telegram admin buttons respect independent paymentStatus/moneyStatus and never leave
# cards stuck on "Ставлю действие в единую очередь…" after the server already accepted the action.
RECENTLY_PUBLISHED_PAYMENT_IDS = {}
RECENTLY_PUBLISHED_TTL_SECONDS = 10 * 60
PAYMENT_MESSAGE_CACHE = {}
PENDING_PROGRESS_BY_PAYMENT_ID = {}

import os
import re
import html
import asyncio
from types import SimpleNamespace
from datetime import datetime
import time
from typing import Any, Dict, List, Optional

import requests

SESSION = requests.Session()
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# v167: cache binding after first successful check/bind; support flat and nested Apps Script API responses.
# v161: speed layer — longer Railway cache, stale-while-revalidate, item prewarm, faster request endpoint.
# v160: delete admin card too when a request is canceled/archived from the website; preserve all known Telegram message IDs.
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
# v182: заявки "По счету" дублируются Татьяне как view-only копия.
# Можно переопределить через Railway env TATYANA_CHAT_ID, но по умолчанию используем рабочий ID.
TATYANA_CHAT_ID = int(os.getenv("TATYANA_CHAT_ID", "1896781134"))
APPS_SCRIPT_URL = os.getenv("APPS_SCRIPT_URL")
BOT_API_SECRET = os.getenv("BOT_API_SECRET")
POLL_SITE_REQUESTS_SECONDS = int(os.getenv("POLL_SITE_REQUESTS_SECONDS", "20"))
BOT_POLL_BATCH_LIMIT = int(os.getenv("BOT_POLL_BATCH_LIMIT", "5"))

(
    BIND_PHONE,
    BIND_PIN,
    CHOOSE_MONTH,
    CHOOSE_EVENT,
    CHOOSE_ITEM,
    EXTRA_POSITION_NAME,
    AMOUNT,
    PAYMENT_METHOD,
    CARD_NUMBER,
    COMMENT,
) = range(10)

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [["Новая заявка"], ["Мои заявки", "Привязать аккаунт"], ["Отменить"]],
    resize_keyboard=True,
    one_time_keyboard=False,
)

PAYMENT_METHODS = ["По счету", "На карту", "Нал"]
EXTRA_ITEM_ORDER = -1
FLOW_CACHE = {}
FLOW_CACHE_TTL_SECONDS = 900
ITEM_CACHE = {}
ITEM_CACHE_TTL_SECONDS = 900
FLOW_REFRESH_IN_PROGRESS = set()
ITEM_REFRESH_IN_PROGRESS = set()
FLOW_CLEANUP_KEY = "payment_flow_message_ids"
BOUND_USER_CACHE = {}
BOUND_USER_CACHE_TTL_SECONDS = 24 * 60 * 60




def require_env() -> None:
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not ADMIN_CHAT_ID:
        missing.append("ADMIN_CHAT_ID")
    if not APPS_SCRIPT_URL:
        missing.append("APPS_SCRIPT_URL")
    if not BOT_API_SECRET:
        missing.append("BOT_API_SECRET")
    if missing:
        raise RuntimeError("Не заданы переменные окружения: " + ", ".join(missing))


def api(action: str, data: Optional[Dict[str, Any]] = None, timeout: int = 120) -> Dict[str, Any]:
    payload = {
        "secret": BOT_API_SECRET,
        "action": action,
        "data": data or {},
    }
    response = SESSION.post(APPS_SCRIPT_URL, json=payload, timeout=timeout)
    response.raise_for_status()
    result = response.json()
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "Ошибка Apps Script API")

    # Apps Script versions before v166 returned fields flat: {ok:true, user:...}.
    # v166 briefly returned {ok:true, result:{...}}. Support both shapes so the bot
    # does not lose binding state after deploy/version mismatches.
    nested = result.get("result")
    if isinstance(nested, dict):
        return nested
    if nested is not None and len(result.keys()) <= 2:
        return {"result": nested}
    return result


async def api_retry(action: str, data: Optional[Dict[str, Any]] = None, attempts: int = 3, delay: float = 2.0) -> Dict[str, Any]:
    last_error = None
    for attempt in range(max(1, attempts)):
        try:
            return api(action, data)
        except Exception as err:
            last_error = err
            if attempt < attempts - 1:
                await asyncio.sleep(delay)
    raise last_error


async def api_async(action: str, data: Optional[Dict[str, Any]] = None, timeout: int = 120) -> Dict[str, Any]:
    # requests блокирующий; уносим его в отдельный поток, чтобы бот не замирал целиком.
    return await asyncio.to_thread(api, action, data, timeout)


async def api_async_try(actions, data: Optional[Dict[str, Any]] = None, timeout: int = 120) -> Dict[str, Any]:
    """Try a list of Apps Script actions in order. Useful during staged deploys."""
    last_error = None
    for action in actions:
        try:
            return await api_async(action, data, timeout=timeout)
        except Exception as err:
            last_error = err
            text = str(err)
            if "Неизвестное действие" not in text and "Unknown" not in text and action != actions[-1]:
                # For real server errors, still allow fallback only if this was a new optional action.
                continue
    raise last_error


def flow_cache_key(telegram_id: Any) -> str:
    return str(telegram_id or '').strip()


def get_local_flow_cache(telegram_id: Any) -> Optional[Dict[str, Any]]:
    key = flow_cache_key(telegram_id)
    item = FLOW_CACHE.get(key)
    if not item:
        return None
    if time.time() - float(item.get('ts', 0)) > FLOW_CACHE_TTL_SECONDS:
        FLOW_CACHE.pop(key, None)
        return None
    return item.get('data')


def set_local_flow_cache(telegram_id: Any, data: Dict[str, Any]) -> None:
    key = flow_cache_key(telegram_id)
    if not key:
        return
    FLOW_CACHE[key] = {'ts': time.time(), 'data': data or {}}


async def _refresh_flow_cache_background(telegram_id: Any) -> None:
    key = flow_cache_key(telegram_id)
    if not key or key in FLOW_REFRESH_IN_PROGRESS:
        return
    FLOW_REFRESH_IN_PROGRESS.add(key)
    try:
        try:
            result = await api_async('list_flow_fast', {'telegramId': telegram_id}, timeout=45)
        except Exception as err:
            err_text = str(err)
            if 'list_flow_fast' not in err_text and 'Неизвестное действие' not in err_text:
                raise
            result = await api_async('list_flow', {'telegramId': telegram_id}, timeout=90)
        set_local_flow_cache(telegram_id, result)
    except Exception as err:
        print(f"flow background refresh failed for {telegram_id}: {err}")
    finally:
        FLOW_REFRESH_IN_PROGRESS.discard(key)


def schedule_flow_refresh(telegram_id: Any) -> None:
    try:
        asyncio.create_task(_refresh_flow_cache_background(telegram_id))
    except RuntimeError:
        pass


async def load_flow_data(telegram_id: Any, prefer_cache: bool = True, refresh_if_cached: bool = True) -> Dict[str, Any]:
    if prefer_cache:
        cached = get_local_flow_cache(telegram_id)
        if cached:
            if refresh_if_cached:
                schedule_flow_refresh(telegram_id)
            return dict(cached, localCached=True)

    try:
        result = await api_async('list_flow_fast', {'telegramId': telegram_id}, timeout=60)
    except Exception as err:
        # Если Apps Script ещё не обновлён/не redeploy и не знает новый endpoint,
        # откатываемся на старое действие, чтобы бот не падал у менеджера.
        err_text = str(err)
        if 'list_flow_fast' not in err_text and 'Неизвестное действие' not in err_text:
            raise
        result = await api_async('list_flow', {'telegramId': telegram_id}, timeout=120)

    set_local_flow_cache(telegram_id, result)
    return result


def item_cache_key(telegram_id: Any, event_id: Any) -> str:
    return f"{telegram_id}:{event_id}"

def get_local_item_cache(telegram_id: Any, event_id: Any) -> Optional[Dict[str, Any]]:
    key = item_cache_key(telegram_id, event_id)
    item = ITEM_CACHE.get(key)
    if not item:
        return None
    if time.time() - float(item.get('ts', 0)) > ITEM_CACHE_TTL_SECONDS:
        ITEM_CACHE.pop(key, None)
        return None
    return item.get('data')

def set_local_item_cache(telegram_id: Any, event_id: Any, data: Dict[str, Any]) -> None:
    key = item_cache_key(telegram_id, event_id)
    if not event_id:
        return
    ITEM_CACHE[key] = {'ts': time.time(), 'data': data or {}}

def clear_local_item_cache(telegram_id: Any, event_id: Any = None) -> None:
    if event_id:
        ITEM_CACHE.pop(item_cache_key(telegram_id, event_id), None)
        return
    prefix = f"{telegram_id}:"
    for key in list(ITEM_CACHE.keys()):
        if key.startswith(prefix):
            ITEM_CACHE.pop(key, None)

async def _refresh_item_cache_background(telegram_id: Any, event_id: Any) -> None:
    key = item_cache_key(telegram_id, event_id)
    if not event_id or key in ITEM_REFRESH_IN_PROGRESS:
        return
    ITEM_REFRESH_IN_PROGRESS.add(key)
    try:
        try:
            result = await api_async("list_items_ultra", {"telegramId": telegram_id, "eventId": event_id}, timeout=30)
        except Exception as err:
            err_text = str(err)
            if 'list_items_ultra' not in err_text and 'Неизвестное действие' not in err_text:
                raise
            result = await api_async("list_items_fast", {"telegramId": telegram_id, "eventId": event_id}, timeout=60)
        set_local_item_cache(telegram_id, event_id, result)
    except Exception as err:
        print(f"items background refresh failed for {telegram_id}/{event_id}: {err}")
    finally:
        ITEM_REFRESH_IN_PROGRESS.discard(key)


def schedule_items_refresh(telegram_id: Any, event_id: Any) -> None:
    try:
        asyncio.create_task(_refresh_item_cache_background(telegram_id, event_id))
    except RuntimeError:
        pass


async def prewarm_items_for_events(telegram_id: Any, events: List[Dict[str, Any]], limit: int = 6) -> None:
    # Прогреваем позиции для ближайших карточек месяца в фоне. Это делает следующий клик почти мгновенным.
    count = 0
    for event in events or []:
        event_id = event.get("eventId")
        if not event_id or get_local_item_cache(telegram_id, event_id):
            continue
        count += 1
        schedule_items_refresh(telegram_id, event_id)
        if count >= limit:
            break
        await asyncio.sleep(0.08)


async def load_items_data(telegram_id: Any, event_id: Any, prefer_cache: bool = True, refresh_if_cached: bool = True) -> Dict[str, Any]:
    if prefer_cache:
        cached = get_local_item_cache(telegram_id, event_id)
        if cached:
            if refresh_if_cached:
                schedule_items_refresh(telegram_id, event_id)
            return dict(cached, localCached=True)
    try:
        result = await api_async("list_items_ultra", {"telegramId": telegram_id, "eventId": event_id}, timeout=35)
    except Exception as err:
        err_text = str(err)
        if 'list_items_ultra' not in err_text and 'Неизвестное действие' not in err_text:
            raise
        result = await api_async("list_items_fast", {"telegramId": telegram_id, "eventId": event_id}, timeout=90)
    set_local_item_cache(telegram_id, event_id, result)
    return result



async def mark_notified_retry(payment_id: Any, admin_message_id: Any = None, manager_message_id: Any = None, tatyana_message_id: Any = None) -> bool:
    if not payment_id:
        return False
    payload = {"paymentId": payment_id}
    if admin_message_id is not None:
        payload["adminMessageId"] = admin_message_id or ""
    if manager_message_id is not None:
        payload["managerMessageId"] = manager_message_id or ""
    if tatyana_message_id is not None:
        payload["tatyanaMessageId"] = tatyana_message_id or ""
    try:
        await api_retry(
            "mark_notified",
            payload,
            attempts=4,
            delay=2.5,
        )
        return True
    except Exception as err:
        print(f"mark_notified failed for {payment_id}: {err}")
        return False


async def mark_tatyana_notified_retry(payment_id: Any, tatyana_message_id: Any) -> bool:
    if not payment_id or not tatyana_message_id:
        return False
    try:
        await api_retry(
            "mark_tatyana_notified",
            {"paymentId": payment_id, "tatyanaMessageId": tatyana_message_id},
            attempts=3,
            delay=2.0,
        )
        return True
    except Exception as err:
        print(f"mark_tatyana_notified failed for {payment_id}: {err}")
        return False


async def safe_edit_text(message, text: str, reply_markup=None, parse_mode: Optional[str] = None) -> None:
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception:
        pass


async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: Any, message_id: Any) -> bool:
    if not chat_id or not message_id:
        return False
    try:
        await context.bot.delete_message(chat_id=int(chat_id), message_id=int(message_id))
        return True
    except Exception:
        return False


def reset_payment_flow_cleanup(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data[FLOW_CLEANUP_KEY] = []


def track_flow_message_id(context: ContextTypes.DEFAULT_TYPE, message_id: Any) -> None:
    if not message_id:
        return
    ids = context.user_data.setdefault(FLOW_CLEANUP_KEY, [])
    try:
        mid = int(message_id)
    except Exception:
        return
    if mid not in ids:
        ids.append(mid)


def track_update_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        track_flow_message_id(context, update.message.message_id)
    elif update.callback_query and update.callback_query.message:
        track_flow_message_id(context, update.callback_query.message.message_id)


def track_bot_message(context: ContextTypes.DEFAULT_TYPE, message) -> None:
    if message:
        track_flow_message_id(context, getattr(message, "message_id", None))


async def cleanup_payment_flow_messages(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: Any,
    keep_message_ids: Optional[List[Any]] = None,
) -> None:
    keep = set()
    for item in keep_message_ids or []:
        try:
            keep.add(int(item))
        except Exception:
            pass
    ids = list(context.user_data.get(FLOW_CLEANUP_KEY, []) or [])
    context.user_data[FLOW_CLEANUP_KEY] = []
    for message_id in ids:
        if int(message_id) in keep:
            continue
        await safe_delete_message(context, chat_id, message_id)


def is_cashbox_archived(request: Dict[str, Any]) -> bool:
    """Telegram should remove terminal/archived request cards from chats."""
    effective = effective_payment_status_from_request(request or {}) if 'effective_payment_status_from_request' in globals() else (request.get("status") or "")
    return effective in ["Деньги в кассе", "Отменено", "Отклонено"]


def remember_payment_messages(payment_id: Any, admin_message_id: Any = None, manager_message_id: Any = None, manager_telegram_id: Any = None, tatyana_message_id: Any = None) -> None:
    if not payment_id:
        return
    key = str(payment_id)
    item = PAYMENT_MESSAGE_CACHE.get(key, {})

    # v160: keep not only the last message_id, but the full known history.
    # If a request was accidentally sent twice earlier, cancellation/archiving must
    # remove every known admin/manager card, not just the latest one.
    def add_message_ids(field: str, list_field: str, value: Any) -> None:
        if not value:
            return
        ids = item.setdefault(list_field, [])
        added = []
        iterable = value if isinstance(value, (list, tuple, set)) else [value]
        for raw in iterable:
            try:
                message_int = int(raw)
            except Exception:
                continue
            if message_int and message_int not in ids:
                ids.append(message_int)
            if message_int:
                added.append(message_int)
        if added:
            item[field] = added[-1]

    add_message_ids("admin_message_id", "admin_message_ids", admin_message_id)
    add_message_ids("manager_message_id", "manager_message_ids", manager_message_id)
    if manager_telegram_id:
        item["manager_telegram_id"] = manager_telegram_id
    add_message_ids("tatyana_message_id", "tatyana_message_ids", tatyana_message_id)
    item["updated_at"] = time.time()
    PAYMENT_MESSAGE_CACHE[key] = item


def remember_progress_message(payment_id: Any, chat_id: Any, message_id: Any) -> None:
    if payment_id and chat_id and message_id:
        PENDING_PROGRESS_BY_PAYMENT_ID[str(payment_id)] = {"chat_id": chat_id, "message_id": message_id, "updated_at": time.time()}


async def remove_progress_message(context: ContextTypes.DEFAULT_TYPE, payment_id: Any) -> None:
    item = PENDING_PROGRESS_BY_PAYMENT_ID.pop(str(payment_id or ""), None)
    if item:
        await safe_delete_message(context, item.get("chat_id"), item.get("message_id"))


def unique_int_ids(*values) -> List[int]:
    result: List[int] = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            iterable = value
        else:
            iterable = [value]
        for item in iterable:
            try:
                n = int(item)
            except Exception:
                continue
            if n and n not in result:
                result.append(n)
    return result


async def remove_payment_messages_by_id(context: ContextTypes.DEFAULT_TYPE, payment_id: Any, manager_telegram_id: Any = None) -> None:
    key = str(payment_id or "")
    item = PAYMENT_MESSAGE_CACHE.pop(key, {})

    # v157/v160: after Railway redeploys or slow create flows, the in-memory cache can be empty.
    # Fetch message IDs from Apps Script so manager/site cancellation also removes the admin card.
    request = None
    try:
        request = await fetch_payment_request(payment_id, timeout=20)
    except Exception:
        request = None

    admin_ids = unique_int_ids(
        item.get("admin_message_ids"),
        item.get("admin_message_id"),
        (request or {}).get("telegramAdminMessageId"),
    )
    manager_ids = unique_int_ids(
        item.get("manager_message_ids"),
        item.get("manager_message_id"),
        (request or {}).get("telegramManagerMessageId"),
    )
    tatyana_ids = unique_int_ids(
        item.get("tatyana_message_ids"),
        item.get("tatyana_message_id"),
        (request or {}).get("telegramTatyanaMessageId"),
    )
    manager_tg = manager_telegram_id or item.get("manager_telegram_id") or (request or {}).get("telegramId")

    if request and should_notify_tatyana(request):
        await sync_tatyana_payment_message(context, request, "🧾 Заявка по счету обновлена", tatyana_ids)

    for admin_id in admin_ids:
        await safe_delete_message(context, ADMIN_CHAT_ID, admin_id)
    if manager_tg:
        for manager_msg_id in manager_ids:
            await safe_delete_message(context, manager_tg, manager_msg_id)
    await remove_progress_message(context, payment_id)


async def remove_archived_payment_messages(
    context: ContextTypes.DEFAULT_TYPE,
    request: Dict[str, Any],
    admin_message_id: Any = None,
    manager_message_id: Any = None,
    manager_telegram_id: Any = None,
) -> bool:
    payment_id = request.get("paymentId")
    cached = PAYMENT_MESSAGE_CACHE.pop(str(payment_id or ""), {})

    # v207: for direct bot/admin actions the request already contains message IDs.
    # Fetching full request here made "Деньги в кассе" feel frozen during high load.
    # For website/status polling we still fetch only if essential IDs are missing.
    full_request = None
    if not (request.get("telegramAdminMessageId") or admin_message_id) and not (request.get("telegramManagerMessageId") or manager_message_id):
        try:
            full_request = await fetch_payment_request(payment_id, timeout=12)
        except Exception:
            full_request = None
    full_request = full_request or {}

    admin_ids = unique_int_ids(
        admin_message_id,
        request.get("telegramAdminMessageId"),
        full_request.get("telegramAdminMessageId"),
        cached.get("admin_message_id"),
        cached.get("admin_message_ids"),
    )
    manager_ids = unique_int_ids(
        manager_message_id,
        request.get("telegramManagerMessageId"),
        full_request.get("telegramManagerMessageId"),
        cached.get("manager_message_id"),
        cached.get("manager_message_ids"),
    )
    tatyana_ids = unique_int_ids(
        request.get("telegramTatyanaMessageId"),
        full_request.get("telegramTatyanaMessageId"),
        cached.get("tatyana_message_id"),
        cached.get("tatyana_message_ids"),
    )
    manager_tg = manager_telegram_id or request.get("telegramId") or full_request.get("telegramId") or cached.get("manager_telegram_id")

    effective_request = dict(full_request or {})
    effective_request.update({k: v for k, v in (request or {}).items() if v not in (None, "")})
    tatyana_ok = True
    if should_notify_tatyana(effective_request):
        tatyana_ids_after = await sync_tatyana_payment_message(context, effective_request, "🧾 Заявка по счету обновлена", tatyana_ids)
        tatyana_ok = bool(tatyana_ids_after)

    for admin_id in admin_ids:
        await safe_delete_message(context, ADMIN_CHAT_ID, admin_id)
    if manager_tg:
        for manager_msg_id in manager_ids:
            await safe_delete_message(context, manager_tg, manager_msg_id)
    await remove_progress_message(context, payment_id)
    return tatyana_ok


async def show_processing_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    if update.callback_query and update.callback_query.message:
        await update.callback_query.answer(text.replace("…", ""))
        await safe_edit_text(update.callback_query.message, f"⏳ {text}")
        return update.callback_query.message
    if update.message:
        return await update.message.reply_text(f"⏳ {text}")
    return None


def money_number(raw: str) -> Optional[int]:
    text = (raw or "").lower().strip()
    text = text.replace("₸", "").replace("kzt", "").replace("тенге", "").replace("тг", "")
    text = text.replace("\u00a0", " ").strip()
    if re.fullmatch(r"\d+", text):
        digits = text
    elif re.fullmatch(r"\d{1,3}([ .,]\d{3})+", text):
        digits = re.sub(r"[ .,]", "", text)
    else:
        return None
    amount = int(digits)
    return amount if amount > 0 else None


def fmt_money(value: Any) -> str:
    try:
        n = int(round(float(value or 0)))
    except Exception:
        n = 0
    return f"{n:,}".replace(",", " ") + " ₸"


def esc(value: Any) -> str:
    return html.escape(str(value or ""))


def format_card_number_for_telegram(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        return ""
    if len(digits) == 16:
        return " ".join(digits[i:i + 4] for i in range(0, 16, 4))
    return str(value or "").strip()


def short(text: str, limit: int = 42) -> str:
    value = str(text or "").strip()
    return value if len(value) <= limit else value[: limit - 1] + "…"


def month_keyboard(months: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(months), 2):
        rows.append([InlineKeyboardButton(m, callback_data=f"month:{i + j}") for j, m in enumerate(months[i:i + 2])])
    return InlineKeyboardMarkup(rows)


def events_keyboard(events: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = []
    for idx, event in enumerate(events):
        label = f"{event.get('eventDate', '')} · {short(event.get('customerName') or event.get('eventName'), 34)}"
        rows.append([InlineKeyboardButton(label, callback_data=f"event:{idx}")])
    rows.append([InlineKeyboardButton("← Назад к месяцам", callback_data="back:months")])
    return InlineKeyboardMarkup(rows)


def items_keyboard(items: List[Dict[str, Any]], allow_extra: bool) -> InlineKeyboardMarkup:
    rows = []
    for item in items:
        order = int(item.get("itemOrder") or 0)
        paid = fmt_money(item.get("paidAmount", 0))
        base_amount = fmt_money(item.get("budgetAmount", 0))
        amount_label = "21%" if item.get("isManagerSalary") or item.get("_isManagerSalary") else "факт"
        label = f"{short(item.get('positionName') or item.get('contractorName'), 28)} · {amount_label} {base_amount} · оплачено {paid}"
        rows.append([InlineKeyboardButton(label, callback_data=f"item:{order}")])
    if allow_extra:
        rows.append([InlineKeyboardButton("+ Добавить позицию", callback_data=f"item:{EXTRA_ITEM_ORDER}")])
    rows.append([InlineKeyboardButton("← Назад к мероприятиям", callback_data="back:events")])
    return InlineKeyboardMarkup(rows)


def payment_method_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(m, callback_data=f"paymethod:{m}")] for m in PAYMENT_METHODS])


def is_new_payment_status(status: Any) -> bool:
    return str(status or "").strip() in ["Новая", "На оплату"]


def effective_payment_status_for_actions(
    status: Any,
    payment_status: Any = "",
    money_status: Any = "",
) -> str:
    """v230: independent payment columns are the source of truth for Telegram buttons.

    Apps Script may keep the public legacy `status` as "Новая/На оплату" while the
    new independent column `paymentStatus` is already "Оплачено". Earlier bot code
    looked only at `status`, so admin cards could stay forever on
    "Ставлю действие в единую очередь…" even though the website already showed paid.
    """
    public_status = str(status or "").strip()
    pay_status = str(payment_status or "").strip()
    cash_status = str(money_status or "").strip()

    if public_status in ["Отменено", "Отклонено"]:
        return public_status
    if cash_status == "Деньги в кассе" or public_status == "Деньги в кассе":
        return "Деньги в кассе"
    if pay_status == "Оплачено" or public_status == "Оплачено":
        return "Оплачено"
    if pay_status in ["Новая", "На оплату"]:
        return pay_status
    return public_status or "Новая"


def effective_payment_status_from_request(request: Optional[Dict[str, Any]]) -> str:
    request = request or {}
    return effective_payment_status_for_actions(
        request.get("status"),
        request.get("paymentStatus"),
        request.get("moneyStatus"),
    )


def admin_keyboard(payment_id: str, status: str, payment_status: Any = "", money_status: Any = "") -> Optional[InlineKeyboardMarkup]:
    effective_status = effective_payment_status_for_actions(status, payment_status, money_status)
    if is_new_payment_status(effective_status):
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Оплачено", callback_data=f"admin:paid:{payment_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"admin:reject:{payment_id}"),
            ]
        ])
    if effective_status == "Оплачено":
        return InlineKeyboardMarkup([[InlineKeyboardButton("💰 Деньги в кассе", callback_data=f"admin:cashin:{payment_id}")]])
    return None


def manager_keyboard(payment_id: str, status: str) -> Optional[InlineKeyboardMarkup]:
    # Apps Script/site may return the initial payment status either as "Новая"
    # or as the UI label "На оплату". Both are active and cancelable.
    if is_new_payment_status(status):
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отменить заявку", callback_data=f"manager:cancel:{payment_id}")]])
    return None


def payment_status_label(status: str, payment_status: str = "") -> str:
    normalized = payment_status or status or "Новая"
    if normalized == "Новая":
        return "🕒 На оплату"
    if normalized == "Оплачено":
        return "✅ Оплачено"
    if normalized == "Деньги в кассе":
        return "✅ Оплачено"
    if normalized in ["Отменено", "Отклонено"]:
        return "❌ Отменено" if normalized == "Отменено" else "❌ Отклонено"
    return normalized


def money_status_label(status: str, money_status: str = "") -> str:
    normalized = money_status or status or "Новая"
    if normalized == "Деньги в кассе":
        return "✅ Деньги в кассе"
    if normalized in ["Отменено", "Отклонено"]:
        return ""
    return "💰 Ждем деньги"


def is_active_request_for_manager(request: Dict[str, Any]) -> bool:
    status = request.get("status") or "Новая"
    return status in ["Новая", "Оплачено"]


def payment_text(request: Dict[str, Any], title: str = "🧾 Заявка на оплату") -> str:
    status = request.get("status") or "Новая"
    payment_status = payment_status_label(status, request.get("paymentStatus") or "")
    money_status = money_status_label(status, request.get("moneyStatus") or "")
    card = request.get("cardNumber") or ""
    card_display = format_card_number_for_telegram(card)
    card_line = f"\nКарта: <code>{esc(card_display)}</code>" if card_display else ""
    extra_comment = request.get("managerComment") or ""
    comment_line = f"\nКомментарий: {esc(extra_comment)}" if extra_comment else ""
    money_line = f"\nСтатус денег: <b>{esc(money_status)}</b>" if money_status else ""
    return (
        f"{esc(title)}\n\n"
        f"№: <b>{esc(request.get('paymentId'))}</b>\n"
        f"Менеджер: <b>{esc(request.get('managerName'))}</b>\n"
        f"Заказчик: {esc(request.get('customerName'))}\n"
        f"Мероприятие: {esc(request.get('eventName'))}\n"
        f"Дата: {esc(request.get('eventDate'))}\n\n"
        f"Позиция: <b>{esc(request.get('positionName'))}</b>\n"
        f"Подрядчик: {esc(request.get('contractorName'))}\n"
        f"Способ оплаты: {esc(request.get('requestPaymentType'))}{card_line}\n"
        f"Сумма заявки: <b>{fmt_money(request.get('requestAmount'))}</b>{comment_line}\n\n"
        f"Статус оплаты: <b>{esc(payment_status)}</b>"
        f"{money_line}"
    )

def is_invoice_payment_request(request: Dict[str, Any]) -> bool:
    method = str((request or {}).get("requestPaymentType") or (request or {}).get("paymentType") or "").strip().lower()
    return "счет" in method or "счёт" in method


def should_notify_tatyana(request: Dict[str, Any]) -> bool:
    return bool(TATYANA_CHAT_ID and is_invoice_payment_request(request or {}))


async def edit_tatyana_payment_message(context: ContextTypes.DEFAULT_TYPE, message_id: Any, request: Dict[str, Any], title: str = "🧾 Заявка по счету обновлена") -> bool:
    if not TATYANA_CHAT_ID or not message_id:
        return False
    try:
        await context.bot.edit_message_text(
            chat_id=TATYANA_CHAT_ID,
            message_id=int(message_id),
            text=payment_text(request, title),
            parse_mode="HTML",
            reply_markup=None,
        )
        return True
    except Exception as err:
        print(f"tatyana edit failed for {request.get('paymentId')}: {err}")
        return False


async def sync_tatyana_payment_message(
    context: ContextTypes.DEFAULT_TYPE,
    request: Dict[str, Any],
    title: str = "🧾 Заявка по счету обновлена",
    known_message_ids: Optional[List[Any]] = None,
) -> List[int]:
    """Create/update Tatiana's view-only copy for invoice requests.

    Unlike manager/admin cards, Tatiana's card is never deleted when the request
    reaches "Деньги в кассе"; it is updated to the final status and remains in history.
    """
    request = request or {}
    payment_id = request.get("paymentId")
    if not should_notify_tatyana(request):
        return []

    cached = PAYMENT_MESSAGE_CACHE.get(str(payment_id or ""), {})
    ids = unique_int_ids(
        known_message_ids,
        request.get("telegramTatyanaMessageId"),
        cached.get("tatyana_message_id"),
        cached.get("tatyana_message_ids"),
    )

    edited_ids: List[int] = []
    for message_id in ids:
        if await edit_tatyana_payment_message(context, message_id, request, title):
            edited_ids.append(int(message_id))

    if edited_ids:
        for message_id in edited_ids:
            remember_payment_messages(payment_id, tatyana_message_id=message_id)
            await mark_tatyana_notified_retry(payment_id, message_id)
        return edited_ids

    try:
        msg = await context.bot.send_message(
            chat_id=TATYANA_CHAT_ID,
            text=payment_text(request, title),
            parse_mode="HTML",
            reply_markup=None,
        )
        remember_payment_messages(payment_id, tatyana_message_id=msg.message_id)
        await mark_tatyana_notified_retry(payment_id, msg.message_id)
        return [int(msg.message_id)]
    except Exception as err:
        print(f"tatyana send failed for {payment_id}: {err}")
        return []


def get_cached_bound_user(telegram_id: Any, context: Optional[ContextTypes.DEFAULT_TYPE] = None) -> Optional[Dict[str, Any]]:
    if context is not None:
        cached = context.user_data.get("bound_user")
        if cached:
            return cached

    key = str(telegram_id or "").strip()
    item = BOUND_USER_CACHE.get(key)
    if not item:
        return None
    if time.time() - float(item.get("ts", 0)) > BOUND_USER_CACHE_TTL_SECONDS:
        BOUND_USER_CACHE.pop(key, None)
        return None
    user = item.get("user")
    if user and context is not None:
        context.user_data["bound_user"] = user
    return user


def set_cached_bound_user(telegram_id: Any, user: Optional[Dict[str, Any]], context: Optional[ContextTypes.DEFAULT_TYPE] = None) -> None:
    if not user:
        return
    key = str(telegram_id or "").strip()
    if key:
        BOUND_USER_CACHE[key] = {"ts": time.time(), "user": user}
    if context is not None:
        context.user_data["bound_user"] = user


async def ensure_bound(update: Update, context: ContextTypes.DEFAULT_TYPE, force_remote: bool = False) -> Optional[Dict[str, Any]]:
    """Return bound manager for this Telegram account.

    Проверка в Apps Script делается только если пользователя нет в локальном кэше.
    После успешной привязки/проверки бот больше не должен дёргать me_fast на каждый клик.
    """
    telegram_id = update.effective_user.id
    if not force_remote:
        cached = get_cached_bound_user(telegram_id, context)
        if cached:
            return cached

    try:
        result = await api_async("me_fast", {"telegramId": telegram_id}, timeout=10)
        user = result.get("user")
        if user:
            set_cached_bound_user(telegram_id, user, context)
        return user
    except Exception as err:
        print(f"fast binding check failed for {telegram_id}: {err}")
        return None



async def admin_backfill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force-repair admin chat cards. Does not touch manager/Tatiana delivery."""
    if update.message is None:
        return
    if not (update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID)):
        await update.message.reply_text("Эта команда доступна только в админском чате.")
        return
    limit = 25
    if context.args:
        try:
            limit = max(1, min(int(context.args[0]), 60))
        except Exception:
            limit = 25
    progress = await update.message.reply_text(f"🔄 v243: восстанавливаю админские карточки, лимит {limit}…")
    sent = edited = saved = failed = 0
    details = []
    try:
        result = await asyncio.wait_for(api_async("list_admin_backfill_candidates", {"limit": limit}, timeout=14), timeout=16)
        requests_list = result.get("requests", []) or []
        if not requests_list:
            await progress.edit_text("✅ v243: сервер не вернул заявок для восстановления админских карточек.")
            return
        for request in requests_list:
            payment_id = request.get("paymentId")
            admin_msg_id = str(request.get("telegramAdminMessageId") or "").strip()
            ok_existing = False
            if admin_msg_id:
                ok_existing = await edit_payment_message(
                    context,
                    ADMIN_CHAT_ID,
                    admin_msg_id,
                    request,
                    "🧾 Заявка обновлена / восстановлена",
                    is_admin=True,
                )
                if ok_existing:
                    edited += 1
            if not ok_existing:
                try:
                    msg = await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=ADMIN_CHAT_ID,
                            text=payment_text(request, "🧾 Заявка восстановлена в админский чат"),
                            parse_mode="HTML",
                            reply_markup=admin_keyboard(
                                payment_id,
                                request.get("status"),
                                request.get("paymentStatus"),
                                request.get("moneyStatus"),
                            ),
                            read_timeout=8,
                            write_timeout=8,
                            connect_timeout=8,
                        ),
                        timeout=10,
                    )
                    admin_msg_id = str(msg.message_id)
                    sent += 1
                except Exception as err:
                    failed += 1
                    details.append(f"{payment_id}: admin send failed: {err}")
                    continue
            try:
                await asyncio.wait_for(api_async("mark_notified", {
                    "paymentId": payment_id,
                    "adminMessageId": admin_msg_id,
                    "managerMessageId": str(request.get("telegramManagerMessageId") or ""),
                    "tatyanaMessageId": str(request.get("telegramTatyanaMessageId") or ""),
                }, timeout=10), timeout=12)
                saved += 1
            except Exception as err:
                failed += 1
                details.append(f"{payment_id}: save admin id failed: {err}")
        tail = ("\n\nОшибки:\n" + "\n".join(details[:8])) if details else ""
        await progress.edit_text(
            "✅ v243 admin_backfill завершён.\n"
            f"Получено от сервера: {len(requests_list)}\n"
            f"Обновлено старых карточек: {edited}\n"
            f"Отправлено новых админских карточек: {sent}\n"
            f"Сохранено Admin Message ID: {saved}\n"
            f"Ошибок: {failed}" + tail
        )
    except Exception as err:
        try:
            await progress.edit_text(f"⚠️ v243 admin_backfill упал: {err}")
        except Exception:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"⚠️ v243 admin_backfill упал: {err}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return ConversationHandler.END

    # v241: админский чат не должен попадать в менеджерскую проверку me_fast.
    if update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID):
        await update.message.reply_text(
            "✅ Админский чат активен.\n/health — диагностика\n/poll_once — дослать недостающие карточки",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    telegram_id = update.effective_user.id
    cached = get_cached_bound_user(telegram_id, context)
    if cached:
        await update.message.reply_text(
            f"✅ Аккаунт найден: {cached.get('name', 'менеджер')}\nГлавное меню:",
            reply_markup=MAIN_KEYBOARD,
        )
        return ConversationHandler.END

    progress_msg = await update.message.reply_text("⏳ Проверяю аккаунт…")

    try:
        user = await ensure_bound(update, context, force_remote=True)
        if user:
            schedule_flow_refresh(telegram_id)
            await safe_edit_text(progress_msg, f"✅ Аккаунт найден: {user.get('name', 'менеджер')}")
            await update.message.reply_text("Главное меню:", reply_markup=MAIN_KEYBOARD)
            return ConversationHandler.END

        await safe_edit_text(progress_msg, "Telegram пока не привязан к аккаунту менеджера.")
        await update.message.reply_text(
            "Нажми «Привязать аккаунт» и введи телефон + PIN от сайта.",
            reply_markup=MAIN_KEYBOARD,
        )
        return ConversationHandler.END
    except Exception as err:
        await safe_edit_text(progress_msg, "⚠️ Не удалось быстро проверить аккаунт.")
        await update.message.reply_text(
            f"Проверка зависла или не ответила: {err}\n\n"
            "Попробуй ещё раз /start или нажми «Привязать аккаунт».",
            reply_markup=MAIN_KEYBOARD,
        )
        return ConversationHandler.END


def normalize_kz_phone_for_bot(raw: str) -> Optional[str]:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) != 11 or not digits.startswith("7"):
        return None
    return f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"


async def bind_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return ConversationHandler.END
    # This step is deliberately instant: no Apps Script calls here.
    context.user_data.clear()
    await update.message.reply_text(
        "Введи телефон как на сайте в формате +7 (___) ___-__-__:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return BIND_PHONE


async def bind_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    formatted_phone = normalize_kz_phone_for_bot(update.message.text.strip())
    if not formatted_phone:
        await update.message.reply_text(
            "Телефон нужен в формате +7 (___) ___-__-__.\n"
            "Например: +7 (701) 123-45-67"
        )
        return BIND_PHONE
    context.user_data["bind_phone"] = formatted_phone
    await update.message.reply_text(f"Телефон: {formatted_phone}\nТеперь введи PIN от сайта:")
    return BIND_PIN


async def bind_pin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    progress_msg = await update.message.reply_text("⏳ Проверяю аккаунт…")
    try:
        result = await api_async("bind_user_fast", {
            "telegramId": tg_user.id,
            "username": tg_user.username or "",
            "fullName": tg_user.full_name or "",
            "phone": context.user_data.get("bind_phone"),
            "pin": update.message.text.strip(),
        }, timeout=25)
        user = result.get("user")
        if not user:
            raise RuntimeError("Сайт подтвердил привязку, но не вернул данные менеджера. Обнови Apps Script deployment и попробуй ещё раз.")
        set_cached_bound_user(tg_user.id, user, context)
        schedule_flow_refresh(tg_user.id)
        await safe_edit_text(progress_msg, f"✅ Аккаунт привязан: {user.get('name', 'менеджер')}.")
        await update.message.reply_text("Главное меню:", reply_markup=MAIN_KEYBOARD)
        return ConversationHandler.END
    except Exception as err:
        await safe_edit_text(progress_msg, "⚠️ Не получилось привязать аккаунт.")
        await update.message.reply_text(f"Не получилось привязать аккаунт: {err}\n\nНажми «Привязать аккаунт» и попробуй ещё раз.", reply_markup=MAIN_KEYBOARD)
        return ConversationHandler.END


async def new_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return ConversationHandler.END
    reset_payment_flow_cleanup(context)
    track_update_message(update, context)

    user = get_cached_bound_user(update.effective_user.id, context)
    status_msg = None
    if not user:
        status_msg = await update.message.reply_text("⏳ Проверяю аккаунт…")
        track_bot_message(context, status_msg)
        user = await ensure_bound(update, context, force_remote=True)

    if not user:
        if status_msg:
            await safe_edit_text(status_msg, "Аккаунт Telegram ещё не привязан к сайту.")
        await update.message.reply_text("Нажми «Привязать аккаунт» и введи телефон + PIN от сайта.", reply_markup=MAIN_KEYBOARD)
        return ConversationHandler.END

    try:
        if status_msg:
            await safe_edit_text(status_msg, "⏳ Загружаю мероприятия…")
        else:
            status_msg = await update.message.reply_text("⏳ Загружаю мероприятия…")
            track_bot_message(context, status_msg)
        result = await load_flow_data(update.effective_user.id, prefer_cache=True)
        months = result.get("months", [])
        events_by_month = result.get("eventsByMonth", {})
        if not months:
            await status_msg.edit_text("У тебя пока нет мероприятий в базе.")
            await update.message.reply_text("Главное меню:", reply_markup=MAIN_KEYBOARD)
            return ConversationHandler.END
        context.user_data["months"] = months
        context.user_data["events_by_month"] = events_by_month
        await status_msg.edit_text("Выбери месяц мероприятия:", reply_markup=month_keyboard(months))
        return CHOOSE_MONTH
    except Exception as err:
        await update.message.reply_text(
            f"Не удалось загрузить мероприятия: {err}\n\n"
            "Попробуй нажать «Новая заявка» ещё раз. Если кэш уже успел сохраниться, "
            "следующий заход откроется быстрее.",
            reply_markup=MAIN_KEYBOARD,
        )
        return ConversationHandler.END



async def choose_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_update_message(update, context)
    query = update.callback_query
    await query.answer("Открываю месяц…")
    if query.data == "back:months":
        return await new_request_from_query(query, context)
    idx = int(query.data.replace("month:", ""))
    months = context.user_data.get("months", [])
    month = months[idx]
    context.user_data["selected_month"] = month
    events = (context.user_data.get("events_by_month") or {}).get(month, [])
    context.user_data["events"] = events
    if events:
        asyncio.create_task(prewarm_items_for_events(query.from_user.id, events, limit=8))
    if not events:
        await query.edit_message_text("В этом месяце мероприятий нет.")
        return ConversationHandler.END
    await query.edit_message_text(f"Месяц: {month}\nВыбери мероприятие:", reply_markup=events_keyboard(events))
    return CHOOSE_EVENT


async def new_request_from_query(query, context: ContextTypes.DEFAULT_TYPE):
    months = context.user_data.get("months", [])
    if not months:
        await safe_edit_text(query.message, "⏳ Загружаю мероприятия…")
        result = await load_flow_data(query.from_user.id, prefer_cache=True)
        months = result.get("months", [])
        context.user_data["months"] = months
        context.user_data["events_by_month"] = result.get("eventsByMonth", {})
    await query.edit_message_text("Выбери месяц мероприятия:", reply_markup=month_keyboard(months))
    return CHOOSE_MONTH


async def choose_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_update_message(update, context)
    query = update.callback_query
    await query.answer("Загружаю позиции…")
    if query.data == "back:months":
        return await new_request_from_query(query, context)
    idx = int(query.data.replace("event:", ""))
    event = context.user_data.get("events", [])[idx]
    context.user_data["selected_event"] = event
    await safe_edit_text(
        query.message,
        f"{event.get('customerName')} · {event.get('eventName')}\n"
        f"Дата: {event.get('eventDate')}\n\n⏳ Загружаю позиции…"
    )
    result = await load_items_data(query.from_user.id, event.get("eventId"), prefer_cache=True)
    context.user_data["items"] = result.get("overview", [])
    context.user_data["allow_extra"] = bool(result.get("allowExtraPosition"))
    await query.edit_message_text(
        f"{event.get('customerName')} · {event.get('eventName')}\n"
        f"Дата: {event.get('eventDate')}\n\nВыбери позицию для оплаты:",
        reply_markup=items_keyboard(context.user_data["items"], context.user_data["allow_extra"]),
    )
    return CHOOSE_ITEM


async def choose_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_update_message(update, context)
    query = update.callback_query
    await query.answer("Открываю позицию…")
    if query.data == "back:events":
        events = context.user_data.get("events", [])
        await query.edit_message_text("Выбери мероприятие:", reply_markup=events_keyboard(events))
        return CHOOSE_EVENT
    order = int(query.data.replace("item:", ""))
    context.user_data["item_order"] = order
    if order == EXTRA_ITEM_ORDER:
        await query.edit_message_text("Введи название новой позиции-допрасхода:")
        return EXTRA_POSITION_NAME
    item = next((x for x in context.user_data.get("items", []) if int(x.get("itemOrder") or 0) == order), None)
    context.user_data["selected_item"] = item or {}
    await query.edit_message_text(f"Позиция: {item.get('positionName') if item else order}\n\nВведи сумму заявки:")
    return AMOUNT


async def extra_position_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_update_message(update, context)
    name = update.message.text.strip()
    if not name:
        err_msg = await update.message.reply_text("Название не должно быть пустым. Введи название новой позиции:")
        track_bot_message(context, err_msg)
        return EXTRA_POSITION_NAME
    context.user_data["new_position_name"] = name
    prompt_msg = await update.message.reply_text("Теперь введи сумму заявки:")
    track_bot_message(context, prompt_msg)
    return AMOUNT


async def get_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_update_message(update, context)
    amount = money_number(update.message.text)
    if not amount:
        err_msg = await update.message.reply_text("Сумма должна быть числом. Например: 250000 или 250 000")
        track_bot_message(context, err_msg)
        return AMOUNT
    context.user_data["amount"] = amount
    prompt_msg = await update.message.reply_text("Выбери способ оплаты:", reply_markup=payment_method_keyboard())
    track_bot_message(context, prompt_msg)
    return PAYMENT_METHOD


async def choose_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_update_message(update, context)
    query = update.callback_query
    await query.answer("Выбран способ оплаты")
    method = query.data.replace("paymethod:", "")
    context.user_data["payment_method"] = method
    if method == "На карту":
        await query.edit_message_text("Введи номер карты. Ровно 16 цифр, можно с пробелами:")
        return CARD_NUMBER
    await query.edit_message_text("Комментарий к заявке? Если комментария нет — напиши «-».")
    return COMMENT


async def get_card_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_update_message(update, context)
    digits = re.sub(r"\D", "", update.message.text or "")
    if len(digits) != 16:
        err_msg = await update.message.reply_text("Номер карты должен содержать ровно 16 цифр. Попробуй ещё раз:")
        track_bot_message(context, err_msg)
        return CARD_NUMBER
    context.user_data["card_number"] = digits
    prompt_msg = await update.message.reply_text("Комментарий к заявке? Если комментария нет — напиши «-».")
    track_bot_message(context, prompt_msg)
    return COMMENT


async def recover_created_request_after_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE, payload: Dict[str, Any], attempts: int = 8) -> Optional[Dict[str, Any]]:
    """После timeout у Apps Script заявка часто уже успевает записаться.
    Не создаём дубль: несколько раз ищем свежую заявку менеджера по данным текущей формы.
    """
    event_id = str(payload.get("eventId") or "")
    amount = int(payload.get("amount") or 0)
    payment_method = str(payload.get("paymentType") or "")
    item_order = int(payload.get("itemOrder") or 0)
    new_position = str(payload.get("newPositionName") or "").strip().lower()

    for _ in range(max(1, attempts)):
        await asyncio.sleep(3)
        try:
            result = await api_async("list_my_requests", {"telegramId": update.effective_user.id}, timeout=45)
            candidates = result.get("requests", []) or []
            for req in candidates:
                req_event = str(req.get("eventId") or "")
                req_amount = int(float(req.get("requestAmount") or 0))
                req_method = str(req.get("requestPaymentType") or "")
                req_order = int(float(req.get("itemOrder") or req.get("itemOrderNumber") or 0))
                req_pos = str(req.get("positionName") or "").strip().lower()
                status = str(req.get("status") or "")

                if req_event != event_id:
                    continue
                if req_amount != amount:
                    continue
                if payment_method and req_method != payment_method:
                    continue
                if status not in ["Новая", "Оплачено"]:
                    continue

                # Для обычной позиции сверяем номер. Для доппозиции (-1) после записи номер уже новый,
                # поэтому сверяем название новой позиции, если оно есть.
                if item_order != EXTRA_ITEM_ORDER and req_order != item_order:
                    continue
                if item_order == EXTRA_ITEM_ORDER and new_position and req_pos != new_position:
                    continue
                return req
        except Exception as err:
            print(f"recover_created_request_after_timeout error: {err}")
            continue
    return None



def remember_recently_published(payment_id: Any) -> None:
    if not payment_id:
        return
    RECENTLY_PUBLISHED_PAYMENT_IDS[str(payment_id)] = time.time()


def is_recently_published(payment_id: Any) -> bool:
    if not payment_id:
        return False
    now = time.time()
    # Cheap cleanup.
    for pid, ts in list(RECENTLY_PUBLISHED_PAYMENT_IDS.items()):
        if now - ts > RECENTLY_PUBLISHED_TTL_SECONDS:
            RECENTLY_PUBLISHED_PAYMENT_IDS.pop(pid, None)
    ts = RECENTLY_PUBLISHED_PAYMENT_IDS.get(str(payment_id))
    return bool(ts and now - ts <= RECENTLY_PUBLISHED_TTL_SECONDS)


async def publish_created_request_cards(update: Update, context: ContextTypes.DEFAULT_TYPE, request: Dict[str, Any], title: str = "🧾 Заявка создана", progress_msg=None) -> None:
    """Publish final request cards.

    v156: while a request is still being sent/registered, the manager must not see
    the cancel button. We first send the final card without inline buttons, save the
    Telegram message IDs in Apps Script, and only after that attach "Отменить заявку".
    This prevents parallel cancel clicks during slow Google/App Script responses.
    """
    remember_recently_published(request.get("paymentId"))
    payment_id = request.get("paymentId")

    manager_msg = await update.message.reply_html(
        payment_text(request, title),
        reply_markup=None,
    )
    admin_msg = await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=payment_text(request, "🧾 Новая заявка на оплату"),
        parse_mode="HTML",
        reply_markup=admin_keyboard(payment_id, request.get("status")),
    )

    tatyana_msg_id = ""
    if should_notify_tatyana(request):
        tatyana_ids = await sync_tatyana_payment_message(context, request, "🧾 Новая заявка по счету")
        tatyana_msg_id = tatyana_ids[0] if tatyana_ids else ""

    remember_payment_messages(payment_id, admin_msg.message_id, manager_msg.message_id, update.effective_user.id, tatyana_msg_id)
    if progress_msg:
        remember_progress_message(payment_id, update.effective_chat.id, progress_msg.message_id)

    # v159: do not wait for slow Apps Script message-id persistence before showing
    # the manager cancel button. The request already exists, final cards are sent,
    # and the local cache is enough for immediate cancel handling. Message IDs are
    # saved in the background; if Google is slow, the UI still behaves correctly.
    keyboard = manager_keyboard(payment_id, request.get("status") or "На оплату")
    if keyboard:
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=update.effective_user.id,
                message_id=manager_msg.message_id,
                reply_markup=keyboard,
            )
        except Exception as err:
            print(f"manager cancel keyboard attach failed for {payment_id}: {err}")
    else:
        print(f"manager cancel keyboard not attached for {payment_id}: status={request.get('status')!r}")

    async def _save_message_ids_background():
        try:
            await mark_notified_retry(
                payment_id,
                admin_message_id=admin_msg.message_id,
                manager_message_id=manager_msg.message_id,
                tatyana_message_id=tatyana_msg_id,
            )
        except Exception as err:
            print(f"mark_notified background failed for {payment_id}: {err}")

    asyncio.create_task(_save_message_ids_background())


async def get_comment_and_submit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    track_update_message(update, context)
    comment = (update.message.text or "").strip()
    if comment == "-":
        comment = ""
    context.user_data["comment"] = comment
    progress_msg = await update.message.reply_text("⏳ Отправляю заявку…")
    track_bot_message(context, progress_msg)

    event = context.user_data.get("selected_event", {})
    payload = {
        "eventId": event.get("eventId"),
        "itemOrder": context.user_data.get("item_order"),
        "amount": context.user_data.get("amount"),
        "paymentType": context.user_data.get("payment_method"),
        "cardNumber": context.user_data.get("card_number", ""),
        "comment": comment,
        "newPositionName": context.user_data.get("new_position_name", ""),
    }

    try:
        try:
            result = await api_async("create_request_v161", {"telegramId": update.effective_user.id, "payload": payload}, timeout=35)
        except Exception as instant_err:
            err_text = str(instant_err)
            if 'create_request_v161' not in err_text and 'Неизвестное действие' not in err_text:
                raise
            result = await api_async("create_request_instant", {"telegramId": update.effective_user.id, "payload": payload}, timeout=60)
        request = result.get("request", {})
        if payload.get("itemOrder") == EXTRA_ITEM_ORDER:
            clear_local_item_cache(update.effective_user.id, payload.get("eventId"))
            schedule_flow_refresh(update.effective_user.id)
        await publish_created_request_cards(update, context, request, progress_msg=progress_msg)
        await remove_progress_message(context, request.get("paymentId"))
        await cleanup_payment_flow_messages(context, update.effective_chat.id)

    except requests.exceptions.Timeout:
        # Важный кейс: Apps Script мог уже записать заявку, но не успел вернуть ответ Telegram-боту.
        # Не пугаем менеджера ошибкой и не предлагаем отправлять дубль.
        await safe_edit_text(progress_msg, "⏳ Google долго отвечает. Проверяю, записалась ли заявка…")
        recovered = await recover_created_request_after_timeout(update, context, payload)
        if recovered:
            try:
                await publish_created_request_cards(update, context, recovered, "🧾 Заявка создана", progress_msg=progress_msg)
            except Exception as err:
                print(f"publish recovered request error: {err}")
            await remove_progress_message(context, recovered.get("paymentId"))
            await cleanup_payment_flow_messages(context, update.effective_chat.id)
        else:
            await safe_edit_text(progress_msg, "⏳ Не получил подтверждение от Google, но заявка могла записаться.")
            await update.message.reply_text(
                "Не отправляй повторно прямо сейчас. Открой «Мои заявки» через минуту: если заявка там есть, значит всё прошло.",
                reply_markup=MAIN_KEYBOARD,
            )

    except Exception as err:
        await safe_edit_text(progress_msg, "⚠️ Не удалось отправить заявку.")
        await update.message.reply_text(f"Не удалось отправить заявку: {err}", reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END


async def my_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    user = get_cached_bound_user(update.effective_user.id, context)
    if not user:
        check_msg = await update.message.reply_text("⏳ Проверяю аккаунт…")
        user = await ensure_bound(update, context, force_remote=True)
        if not user:
            await safe_edit_text(check_msg, "Аккаунт Telegram ещё не привязан к сайту.")
            await update.message.reply_text("Сначала привяжи аккаунт кнопкой «Привязать аккаунт».")
            return
        await safe_delete_message(context, update.effective_chat.id, check_msg.message_id)
    progress_msg = await update.message.reply_text("⏳ Загружаю заявки…")
    try:
        result = await api_async("list_my_requests", {"telegramId": update.effective_user.id}, timeout=45)
        requests_list = [r for r in result.get("requests", []) if is_active_request_for_manager(r)][:30]
        if not requests_list:
            await safe_edit_text(progress_msg, "Заявок пока нет.")
            await update.message.reply_text("Главное меню:", reply_markup=MAIN_KEYBOARD)
            return
        await safe_edit_text(progress_msg, f"Найдено активных заявок: {len(requests_list)}")
        for req in requests_list:
            await update.message.reply_html(payment_text(req), reply_markup=manager_keyboard(req.get("paymentId"), req.get("status")))
    except Exception as err:
        await safe_edit_text(progress_msg, "⚠️ Не удалось загрузить заявки.")
        await update.message.reply_text(f"Не удалось загрузить заявки: {err}")


async def fetch_payment_request(payment_id: Any, timeout: int = 25) -> Optional[Dict[str, Any]]:
    try:
        result = await api_async("get_request", {"paymentId": payment_id}, timeout=timeout)
        return result.get("request") or None
    except Exception as err:
        print(f"fetch_payment_request failed for {payment_id}: {err}")
        return None


def admin_status_reached(
    expected_status: str,
    actual_status: str,
    payment_status: Any = "",
    money_status: Any = "",
) -> bool:
    effective = effective_payment_status_for_actions(actual_status, payment_status, money_status)
    expected = str(expected_status or "")
    if expected == "Оплачено":
        return effective in ["Оплачено", "Деньги в кассе"]
    if expected == "Деньги в кассе":
        return effective == "Деньги в кассе"
    if expected == "Отклонено":
        return effective == "Отклонено"
    return effective == expected


async def set_manager_status_processing(context: ContextTypes.DEFAULT_TYPE, request: Optional[Dict[str, Any]]) -> None:
    if not request:
        return
    manager_tg = request.get("telegramId")
    manager_msg_id = request.get("telegramManagerMessageId")
    payment_id = request.get("paymentId")
    if not manager_tg or not manager_msg_id:
        return
    remember_payment_messages(payment_id, request.get("telegramAdminMessageId"), manager_msg_id, manager_tg)
    try:
        await context.bot.edit_message_text(
            chat_id=int(manager_tg),
            message_id=int(manager_msg_id),
            text=payment_text(request, "⏳ Обновляю статус заявки") + "\n\n⏳ Обновляю статус…",
            parse_mode="HTML",
            reply_markup=None,
        )
    except Exception as err:
        print(f"set_manager_status_processing failed for {payment_id}: {err}")


async def recover_admin_update_after_timeout(payment_id: Any, expected_status: str, attempts: int = 8) -> Optional[Dict[str, Any]]:
    for _ in range(max(1, attempts)):
        await asyncio.sleep(3)
        request = await fetch_payment_request(payment_id, timeout=30)
        if request and admin_status_reached(
            expected_status,
            request.get("status"),
            request.get("paymentStatus"),
            request.get("moneyStatus"),
        ):
            return request
    return None


async def apply_admin_status_result(
    context: ContextTypes.DEFAULT_TYPE,
    admin_message,
    payment_id: Any,
    request: Dict[str, Any],
    processed_status: Optional[str] = None,
) -> None:
    request = request or {}
    remember_payment_messages(
        payment_id,
        request.get("telegramAdminMessageId") or getattr(admin_message, "message_id", None),
        request.get("telegramManagerMessageId"),
        request.get("telegramId"),
        request.get("telegramTatyanaMessageId"),
    )

    # Финально закрытые заявки убираем из Telegram-чата: на сайте они уже в архиве.
    if is_cashbox_archived(request):
        await remove_archived_payment_messages(
            context,
            request,
            admin_message_id=getattr(admin_message, "message_id", None),
            manager_message_id=request.get("telegramManagerMessageId"),
            manager_telegram_id=request.get("telegramId"),
        )
    else:
        try:
            await admin_message.edit_text(
                payment_text(request, "🧾 Заявка обновлена"),
                parse_mode="HTML",
                reply_markup=admin_keyboard(
                    payment_id,
                    request.get("status"),
                    request.get("paymentStatus"),
                    request.get("moneyStatus"),
                ),
            )
        except Exception:
            pass

        manager_tg = request.get("telegramId")
        manager_msg_id = request.get("telegramManagerMessageId")
        if manager_tg and manager_msg_id:
            edited = await edit_payment_message(
                context,
                int(manager_tg),
                manager_msg_id,
                request,
                "🔔 Статус заявки обновлён",
                is_admin=False,
            )
            if not edited:
                await context.bot.send_message(
                    chat_id=int(manager_tg),
                    text=payment_text(request, "🔔 Статус заявки обновлён"),
                    parse_mode="HTML",
                )
        elif manager_tg:
            await context.bot.send_message(
                chat_id=int(manager_tg),
                text=payment_text(request, "🔔 Статус заявки обновлён"),
                parse_mode="HTML",
            )

        await sync_tatyana_payment_message(
            context,
            request,
            "🧾 Заявка по счету обновлена",
            unique_int_ids(request.get("telegramTatyanaMessageId")),
        )

    async def _mark_status_synced_background():
        try:
            await api_async("mark_status_synced", {"paymentId": payment_id, "status": processed_status or effective_payment_status_from_request(request)}, timeout=30)
        except Exception as err:
            print(f"mark_status_synced background failed for {payment_id}: {err}")

    try:
        asyncio.create_task(_mark_status_synced_background())
    except RuntimeError:
        pass


async def handle_admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Обновляю статус…")
    if query.from_user.id != ADMIN_CHAT_ID:
        await query.answer("Эта кнопка только для админа.", show_alert=True)
        return

    _, action, payment_id = query.data.split(":", 2)
    status = {"paid": "Оплачено", "reject": "Отклонено", "cashin": "Деньги в кассе"}[action]
    old_text = query.message.text_html or query.message.text or ""

    # v207: не делаем предварительный get_request перед сменой статуса.
    # При большом потоке заявок этот лишний запрос блокировал кнопку админа на 20+ секунд.
    await safe_edit_text(query.message, old_text + "\n\n⏳ Ставлю действие в единую очередь…", parse_mode="HTML")

    try:
        result = await api_async_try(
            ["queue_payment_status_action", "admin_update_fast", "admin_update"],
            {"paymentId": payment_id, "action": status, "status": status, "comment": "Telegram"},
            timeout=45,
        )
        request = result.get("request", {}) or {}

        # v230: queue endpoints can return before the visible legacy `status` catches up.
        # If we already have the independent column, render immediately. Otherwise do a
        # very short verification, then still remove the intermediate text instead of
        # leaving the admin card stuck forever.
        if not request or not admin_status_reached(
            status,
            request.get("status"),
            request.get("paymentStatus"),
            request.get("moneyStatus"),
        ):
            verified = await recover_admin_update_after_timeout(payment_id, status, attempts=2)
            if verified:
                request = verified

        if request:
            if status == "Оплачено" and not admin_status_reached(
                status,
                request.get("status"),
                request.get("paymentStatus"),
                request.get("moneyStatus"),
            ):
                request = dict(request)
                request["paymentStatus"] = "Оплачено"
            elif status == "Деньги в кассе" and not admin_status_reached(
                status,
                request.get("status"),
                request.get("paymentStatus"),
                request.get("moneyStatus"),
            ):
                request = dict(request)
                request["moneyStatus"] = "Деньги в кассе"
            elif status == "Отклонено" and not admin_status_reached(
                status,
                request.get("status"),
                request.get("paymentStatus"),
                request.get("moneyStatus"),
            ):
                request = dict(request)
                request["status"] = "Отклонено"
            await apply_admin_status_result(context, query.message, payment_id, request, processed_status=status)
        else:
            await safe_edit_text(
                query.message,
                old_text + "\n\n✅ Действие поставлено в очередь. Обновлю карточку фоновой синхронизацией.",
                parse_mode="HTML",
                reply_markup=None,
            )
    except Exception as err:
        # Apps Script мог успеть записать статус, но не успеть вернуть ответ.
        # Проверяем факт изменения, но не держим кнопку в вечном "ничего не происходит".
        recovered_request = await recover_admin_update_after_timeout(payment_id, status, attempts=3)
        if recovered_request:
            await apply_admin_status_result(context, query.message, payment_id, recovered_request, processed_status=status)
            return

        rollback_request = await fetch_payment_request(payment_id, timeout=20)
        if rollback_request:
            await apply_admin_status_result(context, query.message, payment_id, rollback_request, processed_status=rollback_request.get("status"))
        else:
            await safe_edit_text(query.message, old_text + f"\n\n⚠️ Не удалось обновить статус: {esc(err)}", parse_mode="HTML")
        await query.answer(str(err), show_alert=True)


async def recover_cancel_after_timeout(telegram_id: Any, payment_id: Any, attempts: int = 6) -> bool:
    """После timeout отмена часто уже прошла в Apps Script.
    Проверяем активные заявки: если заявка исчезла из активного списка или получила статус Отменено/Отклонено — считаем отмену успешной.
    """
    payment_id = str(payment_id or '')
    for _ in range(max(1, attempts)):
        await asyncio.sleep(3)
        try:
            result = await api_async("list_my_requests", {"telegramId": telegram_id}, timeout=45)
            requests_list = result.get("requests", []) or []
            found = None
            for req in requests_list:
                if str(req.get("paymentId") or '') == payment_id:
                    found = req
                    break
            if found is None:
                # На сервере list_my_requests не возвращает архивные заявки.
                # Если заявка пропала из активных — значит отмена/закрытие уже применилось.
                return True
            status = str(found.get("status") or '')
            if status in ["Отменено", "Отклонено", "Деньги в кассе"]:
                return True
        except Exception as err:
            print(f"recover_cancel_after_timeout error: {err}")
            continue
    return False


async def handle_manager_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Отменяю заявку…")
    _, action, payment_id = query.data.split(":", 2)
    if action != "cancel":
        return
    try:
        await query.edit_message_text("⏳ Отменяю заявку…")
        await api_async("cancel_request", {"telegramId": query.from_user.id, "paymentId": payment_id}, timeout=90)
        await remove_payment_messages_by_id(context, payment_id, manager_telegram_id=query.from_user.id)
        await safe_delete_message(context, query.message.chat_id, query.message.message_id)
        await context.bot.send_message(chat_id=query.from_user.id, text="Заявка отменена.", reply_markup=MAIN_KEYBOARD)
    except requests.exceptions.Timeout:
        await query.edit_message_text("⏳ Google долго отвечает. Проверяю, отменилась ли заявка…")
        recovered = await recover_cancel_after_timeout(query.from_user.id, payment_id)
        if recovered:
            await remove_payment_messages_by_id(context, payment_id, manager_telegram_id=query.from_user.id)
            await safe_delete_message(context, query.message.chat_id, query.message.message_id)
            await context.bot.send_message(chat_id=query.from_user.id, text="Заявка отменена.", reply_markup=MAIN_KEYBOARD)
        else:
            await query.edit_message_text(
                "Не получил подтверждение от Google, но отмена могла пройти. "
                "Открой «Мои заявки» через минуту. Если заявки нет — она отменена."
            )
    except Exception as err:
        await query.edit_message_text(f"Не удалось отменить заявку: {err}")


async def edit_payment_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: Any, request: Dict[str, Any], title: str, is_admin: bool = False) -> bool:
    if not chat_id or not message_id:
        return False
    try:
        await context.bot.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(message_id),
            text=payment_text(request, title),
            parse_mode="HTML",
            reply_markup=admin_keyboard(
                request.get("paymentId"),
                request.get("status"),
                request.get("paymentStatus"),
                request.get("moneyStatus"),
            ) if is_admin else manager_keyboard(request.get("paymentId"), request.get("status")),
        )
        return True
    except Exception:
        return False


async def poll_status_updates(context: ContextTypes.DEFAULT_TYPE):
    try:
        # v207: Apps Script опрашиваем через api_async, иначе фоновой polling блокирует весь event loop
        # и кнопки Telegram выглядят так, будто бот умер с открытыми глазами.
        result = await api_async("list_status_updates", {}, timeout=45)
        for request in (result.get("requests", []) or [])[:BOT_POLL_BATCH_LIMIT]:
            payment_id = request.get("paymentId")
            cached = PAYMENT_MESSAGE_CACHE.get(str(payment_id or ""), {})
            admin_ids = unique_int_ids(request.get("telegramAdminMessageId"), cached.get("admin_message_id"), cached.get("admin_message_ids"))
            manager_ids = unique_int_ids(request.get("telegramManagerMessageId"), cached.get("manager_message_id"), cached.get("manager_message_ids"))
            tatyana_ids = unique_int_ids(request.get("telegramTatyanaMessageId"), cached.get("tatyana_message_id"), cached.get("tatyana_message_ids"))
            manager_tg = request.get("telegramId") or cached.get("manager_telegram_id")

            processed_ok = False
            if is_cashbox_archived(request):
                processed_ok = bool(await remove_archived_payment_messages(
                    context,
                    request,
                    admin_message_id=admin_ids,
                    manager_message_id=manager_ids,
                    manager_telegram_id=manager_tg,
                ))
            else:
                remember_payment_messages(payment_id, admin_ids, manager_ids, manager_tg, tatyana_ids)
                admin_edited = True
                if admin_ids:
                    admin_edited = False
                    for aid in admin_ids:
                        admin_edited = (await edit_payment_message(context, ADMIN_CHAT_ID, aid, request, "🧾 Заявка обновлена", is_admin=True)) or admin_edited

                manager_edited = True
                if manager_tg and manager_ids:
                    manager_edited = False
                    for mid in manager_ids:
                        manager_edited = (await edit_payment_message(context, int(manager_tg), mid, request, "🔔 Статус заявки обновлён", is_admin=False)) or manager_edited

                tatyana_ids_after = await sync_tatyana_payment_message(context, request, "🧾 Заявка по счету обновлена", tatyana_ids)
                tatyana_ok = (not should_notify_tatyana(request)) or bool(tatyana_ids_after)
                processed_ok = admin_edited and manager_edited and tatyana_ok

            if processed_ok:
                try:
                    await api_async("mark_status_synced", {"paymentId": payment_id, "status": effective_payment_status_from_request(request)}, timeout=30)
                except Exception as err:
                    print(f"mark_status_synced failed for {payment_id}: {err}")
            else:
                print(f"status sync not confirmed for {payment_id}: admin_ids={admin_ids}, manager_ids={manager_ids}")
    except Exception as err:
        print(f"poll_status_updates error: {err}")


async def poll_site_requests(context: ContextTypes.DEFAULT_TYPE):
    try:
        # v207: не блокируем Telegram callbacks синхронным requests.post.
        result = await api_async("list_unnotified", {}, timeout=45)
        for request in (result.get("requests", []) or [])[:BOT_POLL_BATCH_LIMIT]:
            payment_id = request.get("paymentId")
            if is_recently_published(payment_id):
                continue
            admin_msg = await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=payment_text(request, "🧾 Новая заявка с сайта"),
                parse_mode="HTML",
                reply_markup=admin_keyboard(
                    payment_id,
                    request.get("status"),
                    request.get("paymentStatus"),
                    request.get("moneyStatus"),
                ),
            )
            manager_msg_id = ""
            manager_tg = request.get("telegramId")
            if manager_tg:
                try:
                    manager_msg = await context.bot.send_message(
                        chat_id=int(manager_tg),
                        text=payment_text(request, "🧾 Заявка создана на сайте"),
                        parse_mode="HTML",
                        reply_markup=manager_keyboard(payment_id, request.get("status")),
                    )
                    manager_msg_id = manager_msg.message_id
                except Exception:
                    manager_msg_id = ""
            tatyana_msg_id = ""
            if should_notify_tatyana(request):
                tatyana_ids = await sync_tatyana_payment_message(context, request, "🧾 Новая заявка по счету")
                tatyana_msg_id = tatyana_ids[0] if tatyana_ids else ""

            remember_recently_published(payment_id)
            remember_payment_messages(payment_id, admin_msg.message_id, manager_msg_id, manager_tg, tatyana_msg_id)

            async def _save_ids(pid=payment_id, aid=admin_msg.message_id, mid=manager_msg_id, tid=tatyana_msg_id):
                await mark_notified_retry(pid, admin_message_id=aid, manager_message_id=mid, tatyana_message_id=tid)
            try:
                asyncio.create_task(_save_ids())
            except RuntimeError:
                pass
    except Exception as err:
        print(f"poll_site_requests error: {err}")


async def bot_background_loop(application, worker, name: str, first: int, interval: int):
    await asyncio.sleep(max(0, first))
    context = SimpleNamespace(bot=application.bot)
    while True:
        try:
            await worker(context)
        except Exception as err:
            print(f"{name} background error: {err}")
        await asyncio.sleep(max(5, interval))


async def post_init(application):
    # Не зависим от optional JobQueue. Railway часто ставит python-telegram-bot без [job-queue],
    # из-за этого фоновые обновления статусов раньше могли вообще не запускаться.
    application.create_task(
        bot_background_loop(application, poll_site_requests, "poll_site_requests", 10, POLL_SITE_REQUESTS_SECONDS)
    )
    application.create_task(
        bot_background_loop(application, poll_status_updates, "poll_status_updates", 15, POLL_SITE_REQUESTS_SECONDS)
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        track_update_message(update, context)
        await cleanup_payment_flow_messages(context, update.effective_chat.id)
        await update.message.reply_text("Действие отменено.", reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END







# ===== v241 STARTUP SAFETY HELPERS =====
# v240 referenced these helpers but did not define them in this physical file.
# That causes a startup NameError before polling begins, so /start and /health never answer.
async def notify_admin_v238(application, text: str):
    try:
        if ADMIN_CHAT_ID:
            await application.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=text,
                read_timeout=8,
                write_timeout=8,
                connect_timeout=8,
            )
    except Exception as err:
        print(f"admin startup notify failed: {err}")


async def error_handler_v238(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = getattr(context, "error", None)
    print(f"telegram handler error: {err}")
    try:
        if ADMIN_CHAT_ID:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"⚠️ Ошибка Telegram-бота v241: {err}",
                read_timeout=8,
                write_timeout=8,
                connect_timeout=8,
            )
    except Exception as notify_err:
        print(f"telegram error notify failed: {notify_err}")

# ===== v240 ADMIN DELIVERY BACKFILL =====
# Если заявка уже ушла менеджеру, но не ушла админу, не считаем её полностью опубликованной.
# Публикуем только недостающие карточки и сохраняем уже существующие message_id.

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    if update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID):
        text = (
            "✅ v241 жив.\n"
            f"ADMIN_CHAT_ID: {ADMIN_CHAT_ID}\n"
            f"APPS_SCRIPT_URL: {'есть' if APPS_SCRIPT_URL else 'нет'}\n"
            f"BOT_API_SECRET: {'есть' if BOT_API_SECRET else 'нет'}\n"
            f"POLL_SITE_REQUESTS_SECONDS: {POLL_SITE_REQUESTS_SECONDS}\n"
            f"BOT_POLL_BATCH_LIMIT: {BOT_POLL_BATCH_LIMIT}"
        )
        try:
            diag = await api_async("debug_polling", {}, timeout=18)
            text += (
                "\n\nApps Script debug_polling:"
                f"\nsource: {diag.get('sourceVersion')}"
                f"\npaymentRows: {diag.get('paymentRows')}"
                f"\nactiveSiteRows: {diag.get('activeSiteRows')}"
                f"\nmissingAdmin: {diag.get('missingAdmin')}"
                f"\nmissingManager: {diag.get('missingManager')}"
                f"\nactiveSiteIncompleteTelegram: {diag.get('activeSiteIncompleteTelegram')}"
                f"\ncandidates: {diag.get('candidates')}"
            )
            sample = diag.get("sample") or []
            if sample:
                lines = []
                for item in sample[:6]:
                    lines.append(
                        "• {pid} · {mgr} · {date} · {status}/{pstatus} · admin:{admin} manager:{manager}".format(
                            pid=item.get("paymentId") or "",
                            mgr=item.get("manager") or "",
                            date=item.get("eventDate") or "",
                            status=item.get("status") or "",
                            pstatus=item.get("paymentStatus") or "",
                            admin="есть" if item.get("adminMsg") else "нет",
                            manager="есть" if item.get("managerMsg") else "нет",
                        )
                    )
                text += "\n\nБлижайшие неполные карточки:\n" + "\n".join(lines)
        except Exception as err:
            text += f"\n\n⚠️ debug_polling упал: {err}"
        await update.message.reply_text(text, reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text("Бот работает.", reply_markup=MAIN_KEYBOARD)


async def poll_site_requests(context: ContextTypes.DEFAULT_TYPE):
    try:
        result = await api_async("list_unnotified", {}, timeout=20)
        requests_list = (result.get("requests", []) or [])[:BOT_POLL_BATCH_LIMIT]
        if not requests_list:
            return
        print(f"v240 poll_site_requests: {len(requests_list)} incomplete request card(s)")
        for request in requests_list:
            payment_id = request.get("paymentId")
            if not payment_id:
                continue

            existing_admin_id = str(request.get("telegramAdminMessageId") or "").strip()
            existing_manager_id = str(request.get("telegramManagerMessageId") or "").strip()
            existing_tatyana_id = str(request.get("telegramTatyanaMessageId") or "").strip()

            # Если карточка ещё нигде не публиковалась и мы только что её отправляли этим процессом,
            # не дублируем. Но если админу не хватает карточки — не блокируем backfill.
            if is_recently_published(payment_id) and existing_admin_id:
                continue

            admin_msg_id = existing_admin_id
            manager_msg_id = existing_manager_id
            tatyana_msg_id = existing_tatyana_id
            manager_tg = request.get("telegramId")

            if not admin_msg_id:
                try:
                    admin_msg = await context.bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=payment_text(request, "🧾 Новая заявка с сайта"),
                        parse_mode="HTML",
                        reply_markup=admin_keyboard(
                            payment_id,
                            request.get("status"),
                            request.get("paymentStatus"),
                            request.get("moneyStatus"),
                        ),
                        read_timeout=10,
                        write_timeout=10,
                        connect_timeout=10,
                    )
                    admin_msg_id = str(admin_msg.message_id)
                except Exception as err:
                    print(f"v240 admin send failed for {payment_id}: {err}")

            if manager_tg and not manager_msg_id:
                try:
                    manager_msg = await context.bot.send_message(
                        chat_id=int(manager_tg),
                        text=payment_text(request, "🧾 Заявка создана на сайте"),
                        parse_mode="HTML",
                        reply_markup=manager_keyboard(payment_id, request.get("status")),
                        read_timeout=10,
                        write_timeout=10,
                        connect_timeout=10,
                    )
                    manager_msg_id = str(manager_msg.message_id)
                except Exception as err:
                    print(f"v240 manager send failed for {payment_id}: {err}")

            if should_notify_tatyana(request) and not tatyana_msg_id:
                try:
                    tatyana_ids = await sync_tatyana_payment_message(context, request, "🧾 Новая заявка по счету")
                    tatyana_msg_id = str(tatyana_ids[0]) if tatyana_ids else ""
                except Exception as err:
                    print(f"v240 tatyana send failed for {payment_id}: {err}")

            if admin_msg_id or manager_msg_id or tatyana_msg_id:
                remember_recently_published(payment_id)
                remember_payment_messages(payment_id, admin_msg_id, manager_msg_id, manager_tg, tatyana_msg_id)
                try:
                    await api_async("mark_notified", {
                        "paymentId": payment_id,
                        "adminMessageId": admin_msg_id,
                        "managerMessageId": manager_msg_id,
                        "tatyanaMessageId": tatyana_msg_id,
                    }, timeout=20)
                except Exception as err:
                    print(f"v240 mark_notified failed for {payment_id}: {err}")
            else:
                print(f"v240 no messages sent for {payment_id}; will retry later")
    except Exception as err:
        print(f"v240 poll_site_requests error: {err}")
        try:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"⚠️ v240 ошибка подтяжки заявок: {err}")
        except Exception:
            pass


async def poll_once(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    if not (update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID)):
        await update.message.reply_text("Эта команда доступна только в админском чате.")
        return

    progress = await update.message.reply_text("🔄 v240: проверяю недостающие карточки Telegram…")
    try:
        result = await api_async("list_unnotified", {}, timeout=18)
        requests_list = (result.get("requests", []) or [])[:BOT_POLL_BATCH_LIMIT]
        if not requests_list:
            diag_text = ""
            try:
                diag = await api_async("debug_polling", {}, timeout=18)
                diag_text = (
                    f"\n\nДиагностика:"
                    f"\nmissingAdmin: {diag.get('missingAdmin')}"
                    f"\nmissingManager: {diag.get('missingManager')}"
                    f"\nactiveSiteIncompleteTelegram: {diag.get('activeSiteIncompleteTelegram')}"
                    f"\ncandidates: {diag.get('candidates')}"
                    f"\nsource: {diag.get('sourceVersion')}"
                )
            except Exception as diag_err:
                diag_text = f"\n\n⚠️ debug_polling тоже упал: {diag_err}"
            await progress.edit_text(f"Готово: недостающих карточек не найдено.{diag_text}")
            return

        sent_admin = sent_manager = sent_tatyana = marked = failed = skipped = 0
        lines = []
        for request in requests_list:
            payment_id = request.get("paymentId")
            if not payment_id:
                continue
            admin_msg_id = str(request.get("telegramAdminMessageId") or "").strip()
            manager_msg_id = str(request.get("telegramManagerMessageId") or "").strip()
            tatyana_msg_id = str(request.get("telegramTatyanaMessageId") or "").strip()
            manager_tg = request.get("telegramId")

            if admin_msg_id:
                skipped += 1
            else:
                try:
                    admin_msg = await context.bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=payment_text(request, "🧾 Новая заявка с сайта"),
                        parse_mode="HTML",
                        reply_markup=admin_keyboard(payment_id, request.get("status"), request.get("paymentStatus"), request.get("moneyStatus")),
                        read_timeout=8, write_timeout=8, connect_timeout=8,
                    )
                    admin_msg_id = str(admin_msg.message_id)
                    sent_admin += 1
                except Exception as err:
                    failed += 1
                    lines.append(f"{payment_id}: admin send error: {err}")

            if manager_tg and not manager_msg_id:
                try:
                    manager_msg = await context.bot.send_message(
                        chat_id=int(manager_tg),
                        text=payment_text(request, "🧾 Заявка создана на сайте"),
                        parse_mode="HTML",
                        reply_markup=manager_keyboard(payment_id, request.get("status")),
                        read_timeout=8, write_timeout=8, connect_timeout=8,
                    )
                    manager_msg_id = str(manager_msg.message_id)
                    sent_manager += 1
                except Exception as err:
                    failed += 1
                    lines.append(f"{payment_id}: manager send error: {err}")

            if should_notify_tatyana(request) and not tatyana_msg_id:
                try:
                    ids = await sync_tatyana_payment_message(context, request, "🧾 Новая заявка по счету")
                    tatyana_msg_id = str(ids[0]) if ids else ""
                    if tatyana_msg_id:
                        sent_tatyana += 1
                except Exception as err:
                    failed += 1
                    lines.append(f"{payment_id}: tatyana send error: {err}")

            if admin_msg_id or manager_msg_id or tatyana_msg_id:
                remember_recently_published(payment_id)
                remember_payment_messages(payment_id, admin_msg_id, manager_msg_id, manager_tg, tatyana_msg_id)
                try:
                    await api_async("mark_notified", {
                        "paymentId": payment_id,
                        "adminMessageId": admin_msg_id,
                        "managerMessageId": manager_msg_id,
                        "tatyanaMessageId": tatyana_msg_id,
                    }, timeout=18)
                    marked += 1
                except Exception as err:
                    failed += 1
                    lines.append(f"{payment_id}: mark_notified error: {err}")

        detail = ""
        if lines:
            detail = "\n\nОшибки:\n" + "\n".join(lines[:8])
        await progress.edit_text(
            f"✅ v240 poll_once завершён.\n"
            f"Получено от сервера: {len(requests_list)}\n"
            f"Админу дослано: {sent_admin}\n"
            f"Менеджерам дослано: {sent_manager}\n"
            f"Татьяне дослано: {sent_tatyana}\n"
            f"Уже были admin-карточки: {skipped}\n"
            f"Записано message_id: {marked}\n"
            f"Ошибок: {failed}" + detail
        )
    except Exception as err:
        try:
            await progress.edit_text(f"⚠️ v240 poll_once упал: {err}")
        except Exception:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"⚠️ v240 poll_once упал: {err}")


async def post_init(application):
    await notify_admin_v238(application, "✅ Contrast Finance Bot v241 запущен. /health — диагностика, /poll_once — дослать недостающие карточки.")
    application.create_task(bot_background_loop(application, poll_site_requests, "poll_site_requests", 3, POLL_SITE_REQUESTS_SECONDS))
    application.create_task(bot_background_loop(application, poll_status_updates, "poll_status_updates", 8, POLL_SITE_REQUESTS_SECONDS))


# ===== v242 ADMIN DELIVERY RESTORE / NO REFRESH BUTTON =====
# Emergency stabilization after v232-v241: remove any blocking refresh behavior from the critical path.
# Bot polling must deliver admin cards first, then manager cards, and never wait on Tatiana/card repairs before admin delivery.

async def _send_tg_with_timeout_v242(coro, timeout: int = 12):
    return await asyncio.wait_for(coro, timeout=timeout)

async def _save_notified_ids_v242(payment_id: Any, admin_msg_id: Any = '', manager_msg_id: Any = '', tatyana_msg_id: Any = '') -> bool:
    try:
        await asyncio.wait_for(
            api_async("mark_notified", {
                "paymentId": payment_id,
                "adminMessageId": str(admin_msg_id or ''),
                "managerMessageId": str(manager_msg_id or ''),
                "tatyanaMessageId": str(tatyana_msg_id or ''),
            }, timeout=10),
            timeout=12,
        )
        return True
    except Exception as err:
        print(f"v242 mark_notified failed for {payment_id}: {err}")
        return False

async def _send_admin_card_if_needed_v242(context: ContextTypes.DEFAULT_TYPE, request: Dict[str, Any]) -> str:
    payment_id = request.get("paymentId")
    existing_admin_id = str(request.get("telegramAdminMessageId") or "").strip()
    if existing_admin_id:
        return existing_admin_id
    admin_msg = await _send_tg_with_timeout_v242(
        context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=payment_text(request, "🧾 Новая заявка с сайта"),
            parse_mode="HTML",
            reply_markup=admin_keyboard(
                payment_id,
                request.get("status"),
                request.get("paymentStatus"),
                request.get("moneyStatus"),
            ),
            read_timeout=10,
            write_timeout=10,
            connect_timeout=10,
        ),
        timeout=14,
    )
    return str(admin_msg.message_id)

async def _send_manager_card_if_needed_v242(context: ContextTypes.DEFAULT_TYPE, request: Dict[str, Any]) -> str:
    payment_id = request.get("paymentId")
    existing_manager_id = str(request.get("telegramManagerMessageId") or "").strip()
    if existing_manager_id:
        return existing_manager_id
    manager_tg = request.get("telegramId")
    if not manager_tg:
        return ""
    manager_msg = await _send_tg_with_timeout_v242(
        context.bot.send_message(
            chat_id=int(manager_tg),
            text=payment_text(request, "🧾 Заявка создана на сайте"),
            parse_mode="HTML",
            reply_markup=manager_keyboard(payment_id, request.get("status")),
            read_timeout=10,
            write_timeout=10,
            connect_timeout=10,
        ),
        timeout=14,
    )
    return str(manager_msg.message_id)

async def _send_tatyana_background_v242(context: ContextTypes.DEFAULT_TYPE, request: Dict[str, Any]) -> str:
    # Tatiana delivery is useful, but it must never block admin/manager delivery.
    try:
        if not should_notify_tatyana(request):
            return str(request.get("telegramTatyanaMessageId") or "")
        if str(request.get("telegramTatyanaMessageId") or "").strip():
            return str(request.get("telegramTatyanaMessageId") or "").strip()
        ids = await asyncio.wait_for(sync_tatyana_payment_message(context, request, "🧾 Новая заявка по счету"), timeout=12)
        return str(ids[0]) if ids else ""
    except Exception as err:
        print(f"v242 tatyana skipped for {request.get('paymentId')}: {err}")
        return str(request.get("telegramTatyanaMessageId") or "")

async def _deliver_one_request_v242(context: ContextTypes.DEFAULT_TYPE, request: Dict[str, Any], allow_manager: bool = True, allow_tatyana: bool = False) -> Dict[str, Any]:
    payment_id = request.get("paymentId")
    report = {"paymentId": payment_id, "admin": "", "manager": "", "tatyana": "", "saved": False, "errors": []}
    if not payment_id:
        report["errors"].append("empty paymentId")
        return report

    admin_msg_id = str(request.get("telegramAdminMessageId") or "").strip()
    manager_msg_id = str(request.get("telegramManagerMessageId") or "").strip()
    tatyana_msg_id = str(request.get("telegramTatyanaMessageId") or "").strip()
    manager_tg = request.get("telegramId")

    # Critical rule: admin first. If admin send fails, do not let manager delivery hide the missing admin card.
    try:
        admin_msg_id = await _send_admin_card_if_needed_v242(context, request)
        report["admin"] = "sent_or_exists"
    except Exception as err:
        report["errors"].append(f"admin send: {err}")
        print(f"v242 admin send failed for {payment_id}: {err}")

    if allow_manager and manager_tg:
        try:
            manager_msg_id = await _send_manager_card_if_needed_v242(context, request)
            report["manager"] = "sent_or_exists" if manager_msg_id else "no_manager_id"
        except Exception as err:
            report["errors"].append(f"manager send: {err}")
            print(f"v242 manager send failed for {payment_id}: {err}")

    # Do not block on Tatiana in background polling; optional only for manual one-off.
    if allow_tatyana:
        tatyana_msg_id = await _send_tatyana_background_v242(context, request)
        report["tatyana"] = "sent_or_exists" if tatyana_msg_id else "skipped"

    if admin_msg_id or manager_msg_id or tatyana_msg_id:
        remember_recently_published(payment_id)
        remember_payment_messages(payment_id, admin_msg_id, manager_msg_id, manager_tg, tatyana_msg_id)
        report["saved"] = await _save_notified_ids_v242(payment_id, admin_msg_id, manager_msg_id, tatyana_msg_id)
    return report

async def poll_site_requests(context: ContextTypes.DEFAULT_TYPE):
    try:
        result = await asyncio.wait_for(api_async("list_unnotified", {}, timeout=12), timeout=14)
        requests_list = (result.get("requests", []) or [])[:BOT_POLL_BATCH_LIMIT]
        if not requests_list:
            return
        print(f"v242 poll_site_requests: {len(requests_list)} incomplete Telegram delivery candidate(s)")
        for request in requests_list:
            payment_id = request.get("paymentId")
            # If admin card is missing, never suppress by local recent cache.
            existing_admin_id = str(request.get("telegramAdminMessageId") or "").strip()
            if existing_admin_id and is_recently_published(payment_id):
                continue
            report = await _deliver_one_request_v242(context, request, allow_manager=True, allow_tatyana=False)
            if report.get("errors"):
                print(f"v242 delivery report errors: {report}")
    except Exception as err:
        print(f"v242 poll_site_requests error: {err}")

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    if update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID):
        text = (
            "✅ v243 жив. Админская доставка включена. /admin_backfill N — восстановить админские карточки.\n"
            f"ADMIN_CHAT_ID: {ADMIN_CHAT_ID}\n"
            f"APPS_SCRIPT_URL: {'есть' if APPS_SCRIPT_URL else 'нет'}\n"
            f"BOT_API_SECRET: {'есть' if BOT_API_SECRET else 'нет'}\n"
            f"POLL_SITE_REQUESTS_SECONDS: {POLL_SITE_REQUESTS_SECONDS}\n"
            f"BOT_POLL_BATCH_LIMIT: {BOT_POLL_BATCH_LIMIT}"
        )
        try:
            diag = await asyncio.wait_for(api_async("debug_polling", {}, timeout=12), timeout=14)
            text += (
                "\n\nApps Script debug_polling:"
                f"\nsource: {diag.get('sourceVersion')}"
                f"\npaymentRows: {diag.get('paymentRows')}"
                f"\nactiveSiteRows: {diag.get('activeSiteRows')}"
                f"\nmissingAdmin: {diag.get('missingAdmin')}"
                f"\nmissingManager: {diag.get('missingManager')}"
                f"\nactiveSiteIncompleteTelegram: {diag.get('activeSiteIncompleteTelegram')}"
                f"\ncandidates: {diag.get('candidates')}"
            )
            sample = diag.get("sample") or []
            if sample:
                lines = []
                for item in sample[:6]:
                    lines.append(
                        "• {pid} · {mgr} · {date} · {status}/{pstatus} · admin:{admin} manager:{manager}".format(
                            pid=item.get("paymentId") or "",
                            mgr=item.get("manager") or "",
                            date=item.get("eventDate") or "",
                            status=item.get("status") or "",
                            pstatus=item.get("paymentStatus") or "",
                            admin="есть" if item.get("adminMsg") else "нет",
                            manager="есть" if item.get("managerMsg") else "нет",
                        )
                    )
                text += "\n\nБлижайшие неполные карточки:\n" + "\n".join(lines)
        except Exception as err:
            text += f"\n\n⚠️ debug_polling упал: {err}"
        await update.message.reply_text(text, reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text("Бот работает.", reply_markup=MAIN_KEYBOARD)

async def poll_once(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    if not (update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID)):
        await update.message.reply_text("Эта команда доступна только в админском чате.")
        return
    progress = await update.message.reply_text("🔄 v242: делаю один короткий проход доставки админских карточек…")
    sent_admin = sent_manager = saved = failed = 0
    lines = []
    try:
        result = await asyncio.wait_for(api_async("list_unnotified", {}, timeout=12), timeout=14)
        requests_list = (result.get("requests", []) or [])[:BOT_POLL_BATCH_LIMIT]
        for request in requests_list:
            report = await _deliver_one_request_v242(context, request, allow_manager=True, allow_tatyana=False)
            if report.get("admin"):
                sent_admin += 1
            if report.get("manager"):
                sent_manager += 1
            if report.get("saved"):
                saved += 1
            if report.get("errors"):
                failed += 1
                lines.append(str(report.get("paymentId")) + ": " + "; ".join(report.get("errors") or []))
        detail = ("\n\nОшибки:\n" + "\n".join(lines[:6])) if lines else ""
        await progress.edit_text(
            "✅ v242 poll_once завершён.\n"
            f"Получено от сервера: {len(requests_list)}\n"
            f"Админские карточки обработаны: {sent_admin}\n"
            f"Менеджерские карточки обработаны: {sent_manager}\n"
            f"Message ID сохранены: {saved}\n"
            f"Ошибок: {failed}" + detail
        )
    except Exception as err:
        try:
            await progress.edit_text(f"⚠️ v242 poll_once упал: {err}")
        except Exception:
            try:
                await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"⚠️ v242 poll_once упал: {err}")
            except Exception:
                pass


async def admin_backfill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force-repair admin chat cards. Does not touch manager/Tatiana delivery."""
    if update.message is None:
        return
    if not (update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID)):
        await update.message.reply_text("Эта команда доступна только в админском чате.")
        return
    limit = 25
    if context.args:
        try:
            limit = max(1, min(int(context.args[0]), 60))
        except Exception:
            limit = 25
    progress = await update.message.reply_text(f"🔄 v243: восстанавливаю админские карточки, лимит {limit}…")
    sent = edited = saved = failed = 0
    details = []
    try:
        result = await asyncio.wait_for(api_async("list_admin_backfill_candidates", {"limit": limit}, timeout=14), timeout=16)
        requests_list = result.get("requests", []) or []
        if not requests_list:
            await progress.edit_text("✅ v243: сервер не вернул заявок для восстановления админских карточек.")
            return
        for request in requests_list:
            payment_id = request.get("paymentId")
            admin_msg_id = str(request.get("telegramAdminMessageId") or "").strip()
            ok_existing = False
            if admin_msg_id:
                ok_existing = await edit_payment_message(
                    context,
                    ADMIN_CHAT_ID,
                    admin_msg_id,
                    request,
                    "🧾 Заявка обновлена / восстановлена",
                    is_admin=True,
                )
                if ok_existing:
                    edited += 1
            if not ok_existing:
                try:
                    msg = await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=ADMIN_CHAT_ID,
                            text=payment_text(request, "🧾 Заявка восстановлена в админский чат"),
                            parse_mode="HTML",
                            reply_markup=admin_keyboard(
                                payment_id,
                                request.get("status"),
                                request.get("paymentStatus"),
                                request.get("moneyStatus"),
                            ),
                            read_timeout=8,
                            write_timeout=8,
                            connect_timeout=8,
                        ),
                        timeout=10,
                    )
                    admin_msg_id = str(msg.message_id)
                    sent += 1
                except Exception as err:
                    failed += 1
                    details.append(f"{payment_id}: admin send failed: {err}")
                    continue
            try:
                await asyncio.wait_for(api_async("mark_notified", {
                    "paymentId": payment_id,
                    "adminMessageId": admin_msg_id,
                    "managerMessageId": str(request.get("telegramManagerMessageId") or ""),
                    "tatyanaMessageId": str(request.get("telegramTatyanaMessageId") or ""),
                }, timeout=10), timeout=12)
                saved += 1
            except Exception as err:
                failed += 1
                details.append(f"{payment_id}: save admin id failed: {err}")
        tail = ("\n\nОшибки:\n" + "\n".join(details[:8])) if details else ""
        await progress.edit_text(
            "✅ v243 admin_backfill завершён.\n"
            f"Получено от сервера: {len(requests_list)}\n"
            f"Обновлено старых карточек: {edited}\n"
            f"Отправлено новых админских карточек: {sent}\n"
            f"Сохранено Admin Message ID: {saved}\n"
            f"Ошибок: {failed}" + tail
        )
    except Exception as err:
        try:
            await progress.edit_text(f"⚠️ v243 admin_backfill упал: {err}")
        except Exception:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"⚠️ v243 admin_backfill упал: {err}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return ConversationHandler.END
    if update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID):
        await update.message.reply_text(
            "✅ Админский чат активен.\n/health — диагностика\n/poll_once — один короткий проход доставки\n/admin_backfill 25 — восстановить последние админские карточки",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END
    telegram_id = update.effective_user.id
    cached = get_cached_bound_user(telegram_id, context)
    if cached:
        await update.message.reply_text(
            f"✅ Аккаунт найден: {cached.get('name', 'менеджер')}\nГлавное меню:",
            reply_markup=MAIN_KEYBOARD,
        )
        return ConversationHandler.END
    await update.message.reply_text("Проверяю аккаунт…")
    user = await get_bound_user_fast(telegram_id, context)
    if user:
        await update.message.reply_text(
            f"✅ Аккаунт найден: {user.get('name', 'менеджер')}\nГлавное меню:",
            reply_markup=MAIN_KEYBOARD,
        )
        return ConversationHandler.END
    await update.message.reply_text(
        "Нужно привязать Telegram к аккаунту менеджера. Нажмите «Привязать аккаунт».",
        reply_markup=MAIN_KEYBOARD,
    )
    return ConversationHandler.END

async def post_init(application):
    await notify_admin_v238(application, "✅ Contrast Finance Bot v243 запущен. /health — диагностика, /poll_once — один проход, /admin_backfill 25 — восстановить админские карточки.")
    application.create_task(bot_background_loop(application, poll_site_requests, "poll_site_requests_v242", 3, POLL_SITE_REQUESTS_SECONDS))
    application.create_task(bot_background_loop(application, poll_status_updates, "poll_status_updates", 8, POLL_SITE_REQUESTS_SECONDS))



# v246 — lightweight health and forced admin-card resend.
async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    if update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID):
        text = (
            "✅ v246 жив. Админская доставка включена. /admin_last N — переотправить последние N карточек админу.\n"
            f"ADMIN_CHAT_ID: {ADMIN_CHAT_ID}\n"
            f"APPS_SCRIPT_URL: {'есть' if APPS_SCRIPT_URL else 'нет'}\n"
            f"BOT_API_SECRET: {'есть' if BOT_API_SECRET else 'нет'}\n"
            f"POLL_SITE_REQUESTS_SECONDS: {POLL_SITE_REQUESTS_SECONDS}\n"
            f"BOT_POLL_BATCH_LIMIT: {BOT_POLL_BATCH_LIMIT}"
        )
        try:
            diag = await asyncio.wait_for(api_async("debug_polling_fast", {}, timeout=10), timeout=12)
            text += (
                "\n\nApps Script debug_polling_fast:"
                f"\nsource: {diag.get('sourceVersion')}"
                f"\npaymentRows: {diag.get('paymentRows')}"
                f"\nactiveSiteRows: {diag.get('activeSiteRows')}"
                f"\nmissingAdmin: {diag.get('missingAdmin')}"
                f"\nwithAdminMessageId: {diag.get('withAdminMessageId')}"
                f"\nmissingManager: {diag.get('missingManager')}"
                f"\nactiveSiteIncompleteTelegram: {diag.get('activeSiteIncompleteTelegram')}"
                f"\ncandidates: {diag.get('candidates')}"
                f"\nadminRecentResendAvailable: {diag.get('adminRecentResendAvailable')}"
            )
            note = diag.get("note")
            if note:
                text += f"\n\n{note}"
            sample = diag.get("sample") or []
            if sample:
                lines = []
                for item in sample[:6]:
                    lines.append(
                        "• {pid} · {mgr} · {date} · {status}/{pstatus} · admin:{admin} manager:{manager}".format(
                            pid=item.get("paymentId") or "",
                            mgr=item.get("manager") or "",
                            date=item.get("eventDate") or "",
                            status=item.get("status") or "",
                            pstatus=item.get("paymentStatus") or "",
                            admin="есть" if item.get("adminMsg") else "нет",
                            manager="есть" if item.get("managerMsg") else "нет",
                        )
                    )
                text += "\n\nПоследние активные заявки для /admin_last:\n" + "\n".join(lines)
        except Exception as err:
            text += f"\n\n⚠️ debug_polling_fast упал: {err}"
        await update.message.reply_text(text, reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text("Бот работает.", reply_markup=MAIN_KEYBOARD)

async def admin_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force-send last active site requests to admin chat as NEW admin cards.
    This intentionally ignores existing/stale Telegram Admin Message IDs.
    It does not send or duplicate manager/Tatiana cards.
    """
    if update.message is None:
        return
    if not (update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID)):
        await update.message.reply_text("Эта команда доступна только в админском чате.")
        return
    limit = 10
    if context.args:
        try:
            limit = max(1, min(int(context.args[0]), 50))
        except Exception:
            limit = 10
    progress = await update.message.reply_text(f"🔄 v246: переотправляю последние {limit} активных заявок админу…")
    sent = saved = failed = 0
    details = []
    try:
        result = await asyncio.wait_for(api_async("list_admin_recent_for_resend", {"limit": limit}, timeout=10), timeout=12)
        requests_list = result.get("requests", []) or []
        if not requests_list:
            await progress.edit_text("✅ v246: сервер не вернул активных заявок для переотправки админу.")
            return
        for request in requests_list:
            payment_id = request.get("paymentId")
            try:
                msg = await asyncio.wait_for(
                    context.bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=payment_text(request, "🧾 Заявка в админский чат"),
                        parse_mode="HTML",
                        reply_markup=admin_keyboard(
                            payment_id,
                            request.get("status"),
                            request.get("paymentStatus"),
                            request.get("moneyStatus"),
                        ),
                        read_timeout=8,
                        write_timeout=8,
                        connect_timeout=8,
                    ),
                    timeout=10,
                )
                sent += 1
                try:
                    await asyncio.wait_for(api_async("mark_notified", {
                        "paymentId": payment_id,
                        "adminMessageId": str(msg.message_id),
                        "managerMessageId": str(request.get("telegramManagerMessageId") or ""),
                        "tatyanaMessageId": str(request.get("telegramTatyanaMessageId") or ""),
                    }, timeout=8), timeout=10)
                    saved += 1
                except Exception as save_err:
                    failed += 1
                    details.append(f"{payment_id}: отправлено, но Admin Message ID не сохранился: {save_err}")
            except Exception as send_err:
                failed += 1
                details.append(f"{payment_id}: не отправлено админу: {send_err}")
        tail = ("\n\nОшибки:\n" + "\n".join(details[:8])) if details else ""
        await progress.edit_text(
            "✅ v246 admin_last завершён.\n"
            f"Получено от сервера: {len(requests_list)}\n"
            f"Отправлено новых админских карточек: {sent}\n"
            f"Сохранено новых Admin Message ID: {saved}\n"
            f"Ошибок: {failed}" + tail
        )
    except Exception as err:
        try:
            await progress.edit_text(f"⚠️ v246 admin_last упал: {err}")
        except Exception:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"⚠️ v246 admin_last упал: {err}")

async def admin_backfill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # v246: old backfill tried to edit stale message IDs and could appear stuck.
    # Keep the command name, but route it to the safer forced resend.
    await admin_last(update, context)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return ConversationHandler.END
    if update.effective_chat and int(update.effective_chat.id) == int(ADMIN_CHAT_ID):
        await update.message.reply_text(
            "✅ Админский чат активен.\n/health — лёгкая диагностика\n/poll_once — один короткий проход доставки\n/admin_last 10 — переотправить последние карточки админу",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END
    telegram_id = update.effective_user.id
    cached = get_cached_bound_user(telegram_id, context)
    if cached:
        await update.message.reply_text(
            f"✅ Аккаунт найден: {cached.get('name', 'менеджер')}\nГлавное меню:",
            reply_markup=MAIN_KEYBOARD,
        )
        return ConversationHandler.END
    await update.message.reply_text("Проверяю аккаунт…")
    user = await get_bound_user_fast(telegram_id, context)
    if user:
        await update.message.reply_text(
            f"✅ Аккаунт найден: {user.get('name', 'менеджер')}\nГлавное меню:",
            reply_markup=MAIN_KEYBOARD,
        )
        return ConversationHandler.END
    await update.message.reply_text(
        "Нужно привязать Telegram к аккаунту менеджера. Нажмите «Привязать аккаунт».",
        reply_markup=MAIN_KEYBOARD,
    )
    return ConversationHandler.END

async def post_init(application):
    await notify_admin_v238(application, "✅ Contrast Finance Bot v246 запущен. /health — лёгкая диагностика, /admin_last 10 — переотправить карточки админу.")
    application.create_task(bot_background_loop(application, poll_site_requests, "poll_site_requests_v242", 3, POLL_SITE_REQUESTS_SECONDS))
    application.create_task(bot_background_loop(application, poll_status_updates, "poll_status_updates", 8, POLL_SITE_REQUESTS_SECONDS))

def main():
    require_env()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    bind_conversation = ConversationHandler(
        entry_points=[CommandHandler("start", start), MessageHandler(filters.Regex("^(Привязать аккаунт|Зарегистрировать пользователя)$"), bind_start)],
        states={
            BIND_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, bind_phone)],
            BIND_PIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, bind_pin)],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex("^Отменить$"), cancel),
        ],
    )

    request_conversation = ConversationHandler(
        entry_points=[CommandHandler("new", new_request), MessageHandler(filters.Regex("^Новая заявка$"), new_request)],
        states={
            CHOOSE_MONTH: [CallbackQueryHandler(choose_month, pattern="^(month:|back:months)")],
            CHOOSE_EVENT: [CallbackQueryHandler(choose_event, pattern="^(event:|back:months)")],
            CHOOSE_ITEM: [CallbackQueryHandler(choose_item, pattern="^(item:|back:events)")],
            EXTRA_POSITION_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, extra_position_name)],
            AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_amount)],
            PAYMENT_METHOD: [CallbackQueryHandler(choose_payment_method, pattern="^paymethod:")],
            CARD_NUMBER: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_card_number)],
            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_comment_and_submit)],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("cancel", cancel),
            MessageHandler(filters.Regex("^Отменить$"), cancel),
            MessageHandler(filters.Regex("^(Привязать аккаунт|Зарегистрировать пользователя)$"), bind_start),
        ],
    )

    app.add_handler(bind_conversation)
    app.add_handler(request_conversation)
    app.add_handler(MessageHandler(filters.Regex("^Мои заявки$"), my_requests))
    app.add_handler(MessageHandler(filters.Regex("^Отменить$"), cancel))
    app.add_handler(CallbackQueryHandler(handle_admin_action, pattern="^admin:"))
    app.add_handler(CallbackQueryHandler(handle_manager_action, pattern="^manager:"))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("poll_once", poll_once))
    app.add_handler(CommandHandler("admin_backfill", admin_backfill))
    app.add_handler(CommandHandler("admin_last", admin_last))
    app.add_error_handler(error_handler_v238)

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
