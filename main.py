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
# stable direct status: Telegram admin buttons use direct admin_update, not queued preview, so Sheets status is written before card changes.
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
    """Telegram should remove terminal/archived request cards from admin/manager chats.

    Treat `moneyStatus = Деньги в кассе` as final even if the legacy public
    `status` field still says `Оплачено`. Tatiana's copy is updated, not deleted.
    """
    return effective_request_status(request or {}) in ["Деньги в кассе", "Отменено", "Отклонено"]


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


def effective_request_status(request_or_status: Any, payment_status: Any = "", money_status: Any = "") -> str:
    """Return the single effective status for Telegram display/actions.

    The stable rollback still may receive legacy independent fields from Apps Script
    (paymentStatus/moneyStatus). For Telegram, the final cash-in stage must imply
    that payment is also completed: two green checks, then remove admin/manager cards.
    """
    if isinstance(request_or_status, dict):
        status = str(request_or_status.get("status") or "").strip()
        payment_status = str(request_or_status.get("paymentStatus") or "").strip()
        money_status = str(request_or_status.get("moneyStatus") or "").strip()
    else:
        status = str(request_or_status or "").strip()
        payment_status = str(payment_status or "").strip()
        money_status = str(money_status or "").strip()

    # Final stage wins. It also means the request was paid.
    if status == "Деньги в кассе" or money_status == "Деньги в кассе":
        return "Деньги в кассе"
    if status in ["Отклонено", "Отменено"]:
        return status
    if payment_status in ["Отклонено", "Отменено"]:
        return payment_status
    if status == "Оплачено" or payment_status == "Оплачено":
        return "Оплачено"
    if is_new_payment_status(status) or is_new_payment_status(payment_status):
        return "Новая"
    return status or payment_status or "Новая"


def admin_keyboard(payment_id: str, status: str) -> Optional[InlineKeyboardMarkup]:
    effective = effective_request_status(status)
    if is_new_payment_status(effective):
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Оплачено", callback_data=f"admin:paid:{payment_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"admin:reject:{payment_id}"),
            ]
        ])
    if effective == "Оплачено":
        return InlineKeyboardMarkup([[InlineKeyboardButton("💰 Деньги в кассе", callback_data=f"admin:cashin:{payment_id}")]])
    return None


def manager_keyboard(payment_id: str, status: str) -> Optional[InlineKeyboardMarkup]:
    # Apps Script/site may return the initial payment status either as "Новая"
    # or as the UI label "На оплату". Both are active and cancelable.
    effective = effective_request_status(status)
    if is_new_payment_status(effective):
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отменить заявку", callback_data=f"manager:cancel:{payment_id}")]])
    return None


def payment_status_label(status: str, payment_status: str = "", money_status: str = "") -> str:
    effective = effective_request_status(status, payment_status, money_status)
    if effective == "Новая":
        return "🕒 На оплату"
    if effective in ["Оплачено", "Деньги в кассе"]:
        return "✅ Оплачено"
    if effective in ["Отменено", "Отклонено"]:
        return "❌ Отменено" if effective == "Отменено" else "❌ Отклонено"
    return effective


def money_status_label(status: str, money_status: str = "") -> str:
    effective = effective_request_status(status, "", money_status)
    if effective == "Деньги в кассе":
        return "✅ Деньги в кассе"
    if effective in ["Отменено", "Отклонено"]:
        return ""
    return "💰 Ждем деньги"


def is_active_request_for_manager(request: Dict[str, Any]) -> bool:
    effective = effective_request_status(request)
    return effective in ["Новая", "Оплачено"]


def payment_text(request: Dict[str, Any], title: str = "🧾 Заявка на оплату") -> str:
    status = request.get("status") or "Новая"
    payment_status = payment_status_label(status, request.get("paymentStatus") or "", request.get("moneyStatus") or "")
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
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
        reply_markup=admin_keyboard(payment_id, effective_request_status(request)),
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
        requests_list = [r for r in result.get("requests", []) if is_active_request_for_manager(r)][:10]
        if not requests_list:
            await safe_edit_text(progress_msg, "Заявок пока нет.")
            await update.message.reply_text("Главное меню:", reply_markup=MAIN_KEYBOARD)
            return
        await safe_edit_text(progress_msg, f"Найдено заявок: {len(requests_list)}")
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


def admin_status_reached(expected_status: str, actual_status: str) -> bool:
    actual = str(actual_status or "")
    expected = str(expected_status or "")
    if expected == "Оплачено":
        return actual in ["Оплачено", "Деньги в кассе"]
    if expected == "Деньги в кассе":
        return actual == "Деньги в кассе"
    if expected == "Отклонено":
        return actual == "Отклонено"
    return actual == expected


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
        if request and admin_status_reached(expected_status, request.get("status")):
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
                reply_markup=admin_keyboard(payment_id, effective_request_status(request)),
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
            await api_async("mark_status_synced", {"paymentId": payment_id, "status": processed_status or request.get("status")}, timeout=30)
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
    action_label = {
        "paid": "Фиксирую оплату",
        "reject": "Отклоняю заявку",
        "cashin": "Фиксирую деньги в кассе",
    }.get(action, "Обновляю статус")

    lock_key = str(payment_id or "")
    now = time.time()
    existing_lock = PENDING_PROGRESS_BY_PAYMENT_ID.get(lock_key)
    if existing_lock and now - float(existing_lock.get("ts", 0)) < 18:
        await query.answer("Эта карточка уже обновляется. Дождитесь ответа сервера.", show_alert=True)
        return

    old_text = query.message.text_html or query.message.text or ""
    old_markup = query.message.reply_markup
    PENDING_PROGRESS_BY_PAYMENT_ID[lock_key] = {"ts": now, "old_text": old_text}

    # Блокируем только эту карточку. Остальные карточки и бот остаются живыми.
    await safe_edit_text(
        query.message,
        old_text + f"\n\n⏳ {action_label}…",
        reply_markup=None,
        parse_mode="HTML",
    )

    try:
        # Короткий лимит: если сервер не ответил быстро, возвращаем карточку как была.
        # Если Apps Script всё-таки допишет статус позже, обычный polling list_status_updates догонит Telegram.
        result = await asyncio.wait_for(
            api_async_try(
                ["admin_update"],
                {"paymentId": payment_id, "status": status, "comment": "Telegram"},
                timeout=14,
            ),
            timeout=14,
        )
        request = result.get("request", {})
        await apply_admin_status_result(
            context,
            query.message,
            payment_id,
            request,
            processed_status=request.get("status") or status,
        )

    except asyncio.TimeoutError:
        # Проверяем один раз: иногда статус успел записаться, но ответ не вернулся.
        request = await fetch_payment_request(payment_id, timeout=5)
        if request and admin_status_reached(status, request.get("status")):
            await apply_admin_status_result(context, query.message, payment_id, request, processed_status=status)
            await query.answer("Статус записался, карточка обновлена.")
        else:
            await safe_edit_text(
                query.message,
                old_text + "\n\n⚠️ Сервер не ответил за 14 секунд. Карточка возвращена как была. Если статус записался позже, бот обновит её через сверку.",
                reply_markup=old_markup,
                parse_mode="HTML",
            )
            await query.answer("Сервер не ответил. Карточка возвращена как была.", show_alert=True)

    except Exception as err:
        # Если сервер успел записать статус, покажем результат. Иначе откатываем конкретную карточку.
        request = await fetch_payment_request(payment_id, timeout=6)
        if request and admin_status_reached(status, request.get("status")):
            await apply_admin_status_result(context, query.message, payment_id, request, processed_status=status)
            await query.answer("Статус записался, карточка обновлена.")
        else:
            await safe_edit_text(
                query.message,
                old_text + f"\n\n⚠️ Не удалось обновить статус: {esc(err)}",
                reply_markup=old_markup,
                parse_mode="HTML",
            )
            await query.answer(str(err), show_alert=True)

    finally:
        PENDING_PROGRESS_BY_PAYMENT_ID.pop(lock_key, None)


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
            reply_markup=admin_keyboard(request.get("paymentId"), effective_request_status(request)) if is_admin else manager_keyboard(request.get("paymentId"), effective_request_status(request)),
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
                    await api_async("mark_status_synced", {"paymentId": payment_id, "status": request.get("status")}, timeout=30)
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
                reply_markup=admin_keyboard(payment_id, effective_request_status(request)),
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

    app.run_polling()


if __name__ == "__main__":
    main()
