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

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
APPS_SCRIPT_URL = os.getenv("APPS_SCRIPT_URL")
BOT_API_SECRET = os.getenv("BOT_API_SECRET")
POLL_SITE_REQUESTS_SECONDS = int(os.getenv("POLL_SITE_REQUESTS_SECONDS", "20"))

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
    [["Новая заявка"], ["Мои заявки", "Привязать аккаунт"]],
    resize_keyboard=True,
    one_time_keyboard=False,
)

PAYMENT_METHODS = ["По счету", "На карту", "Нал"]
EXTRA_ITEM_ORDER = -1
FLOW_CACHE = {}
FLOW_CACHE_TTL_SECONDS = 180
ITEM_CACHE = {}
ITEM_CACHE_TTL_SECONDS = 300



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
    response = requests.post(APPS_SCRIPT_URL, json=payload, timeout=timeout)
    response.raise_for_status()
    result = response.json()
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "Ошибка Apps Script API")
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


async def load_flow_data(telegram_id: Any, prefer_cache: bool = True) -> Dict[str, Any]:
    if prefer_cache:
        cached = get_local_flow_cache(telegram_id)
        if cached:
            return dict(cached, localCached=True)

    try:
        result = await api_async('list_flow_fast', {'telegramId': telegram_id}, timeout=120)
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

async def load_items_data(telegram_id: Any, event_id: Any, prefer_cache: bool = True) -> Dict[str, Any]:
    if prefer_cache:
        cached = get_local_item_cache(telegram_id, event_id)
        if cached:
            return dict(cached, localCached=True)
    try:
        result = await api_async("list_items_ultra", {"telegramId": telegram_id, "eventId": event_id}, timeout=45)
    except Exception as err:
        err_text = str(err)
        if 'list_items_ultra' not in err_text and 'Неизвестное действие' not in err_text:
            raise
        result = await api_async("list_items_fast", {"telegramId": telegram_id, "eventId": event_id}, timeout=90)
    set_local_item_cache(telegram_id, event_id, result)
    return result


async def mark_notified_retry(payment_id: Any, admin_message_id: Any = None, manager_message_id: Any = None) -> bool:
    if not payment_id:
        return False
    try:
        await api_retry(
            "mark_notified",
            {
                "paymentId": payment_id,
                "adminMessageId": admin_message_id or "",
                "managerMessageId": manager_message_id or "",
            },
            attempts=4,
            delay=2.5,
        )
        return True
    except Exception as err:
        print(f"mark_notified failed for {payment_id}: {err}")
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


def is_cashbox_archived(request: Dict[str, Any]) -> bool:
    """Telegram should remove terminal/archived request cards from chats."""
    return (request.get("status") or "") in ["Деньги в кассе", "Отменено", "Отклонено"]


def remember_payment_messages(payment_id: Any, admin_message_id: Any = None, manager_message_id: Any = None, manager_telegram_id: Any = None) -> None:
    if not payment_id:
        return
    key = str(payment_id)
    item = PAYMENT_MESSAGE_CACHE.get(key, {})
    if admin_message_id:
        item["admin_message_id"] = admin_message_id
    if manager_message_id:
        item["manager_message_id"] = manager_message_id
    if manager_telegram_id:
        item["manager_telegram_id"] = manager_telegram_id
    item["updated_at"] = time.time()
    PAYMENT_MESSAGE_CACHE[key] = item


def remember_progress_message(payment_id: Any, chat_id: Any, message_id: Any) -> None:
    if payment_id and chat_id and message_id:
        PENDING_PROGRESS_BY_PAYMENT_ID[str(payment_id)] = {"chat_id": chat_id, "message_id": message_id, "updated_at": time.time()}


async def remove_progress_message(context: ContextTypes.DEFAULT_TYPE, payment_id: Any) -> None:
    item = PENDING_PROGRESS_BY_PAYMENT_ID.pop(str(payment_id or ""), None)
    if item:
        await safe_delete_message(context, item.get("chat_id"), item.get("message_id"))


async def remove_payment_messages_by_id(context: ContextTypes.DEFAULT_TYPE, payment_id: Any, manager_telegram_id: Any = None) -> None:
    key = str(payment_id or "")
    item = PAYMENT_MESSAGE_CACHE.pop(key, {})
    await safe_delete_message(context, ADMIN_CHAT_ID, item.get("admin_message_id"))
    manager_tg = manager_telegram_id or item.get("manager_telegram_id")
    await safe_delete_message(context, manager_tg, item.get("manager_message_id"))
    await remove_progress_message(context, payment_id)


async def remove_archived_payment_messages(
    context: ContextTypes.DEFAULT_TYPE,
    request: Dict[str, Any],
    admin_message_id: Any = None,
    manager_message_id: Any = None,
    manager_telegram_id: Any = None,
) -> None:
    payment_id = request.get("paymentId")
    cached = PAYMENT_MESSAGE_CACHE.pop(str(payment_id or ""), {})
    admin_id = admin_message_id or request.get("telegramAdminMessageId") or cached.get("admin_message_id")
    manager_msg_id = manager_message_id or request.get("telegramManagerMessageId") or cached.get("manager_message_id")
    manager_tg = manager_telegram_id or request.get("telegramId") or cached.get("manager_telegram_id")
    await safe_delete_message(context, ADMIN_CHAT_ID, admin_id)
    if manager_tg and manager_msg_id:
        await safe_delete_message(context, manager_tg, manager_msg_id)
    await remove_progress_message(context, payment_id)


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
        label = f"{short(item.get('positionName') or item.get('contractorName'), 32)} · оплачено {paid}"
        rows.append([InlineKeyboardButton(label, callback_data=f"item:{order}")])
    if allow_extra:
        rows.append([InlineKeyboardButton("+ Добавить позицию", callback_data=f"item:{EXTRA_ITEM_ORDER}")])
    rows.append([InlineKeyboardButton("← Назад к мероприятиям", callback_data="back:events")])
    return InlineKeyboardMarkup(rows)


def payment_method_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(m, callback_data=f"paymethod:{m}")] for m in PAYMENT_METHODS])


def admin_keyboard(payment_id: str, status: str) -> Optional[InlineKeyboardMarkup]:
    if status == "Новая":
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Оплачено", callback_data=f"admin:paid:{payment_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"admin:reject:{payment_id}"),
            ]
        ])
    if status == "Оплачено":
        return InlineKeyboardMarkup([[InlineKeyboardButton("💰 Деньги в кассе", callback_data=f"admin:cashin:{payment_id}")]])
    return None


def manager_keyboard(payment_id: str, status: str) -> Optional[InlineKeyboardMarkup]:
    if status == "Новая":
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отменить заявку", callback_data=f"manager:cancel:{payment_id}")]])
    return None


def payment_status_label(status: str) -> str:
    normalized = status or "Новая"
    if normalized == "Новая":
        return "🕒 На оплату"
    if normalized == "Оплачено":
        return "✅ Оплачено"
    if normalized == "Деньги в кассе":
        return "✅ Оплачено"
    if normalized in ["Отменено", "Отклонено"]:
        return "❌ Отменено" if normalized == "Отменено" else "❌ Отклонено"
    return normalized


def money_status_label(status: str) -> str:
    normalized = status or "Новая"
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
    payment_status = payment_status_label(status)
    money_status = money_status_label(status)
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


async def ensure_bound(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, Any]]:
    telegram_id = update.effective_user.id
    cached = context.user_data.get("bound_user")
    if cached:
        return cached
    try:
        result = api("me", {"telegramId": telegram_id})
        user = result.get("user")
        context.user_data["bound_user"] = user
        return user
    except Exception:
        return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    user = await ensure_bound(update, context)
    if user:
        await update.message.reply_text(
            f"Привет, {user.get('name')}! Можно создавать заявки.",
            reply_markup=MAIN_KEYBOARD,
        )
        return
    await update.message.reply_text(
        "Привет! Это тестовый бот заявок на оплату.\n\n"
        "Сначала привяжем Telegram к аккаунту на сайте. Введи телефон в формате +7 (___) ___-__-__:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return BIND_PHONE


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
        return
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
        result = api("bind_user", {
            "telegramId": tg_user.id,
            "username": tg_user.username or "",
            "fullName": tg_user.full_name or "",
            "phone": context.user_data.get("bind_phone"),
            "pin": update.message.text.strip(),
        })
        context.user_data["bound_user"] = result.get("user")
        await safe_edit_text(progress_msg, "✅ Аккаунт привязан.")
        await update.message.reply_text("Готово, аккаунт привязан. Можно создавать заявки.", reply_markup=MAIN_KEYBOARD)
        return ConversationHandler.END
    except Exception as err:
        await safe_edit_text(progress_msg, "⚠️ Не получилось привязать аккаунт.")
        await update.message.reply_text(f"Не получилось привязать аккаунт: {err}\n\nПопробуй снова: /start")
        return ConversationHandler.END


async def new_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return ConversationHandler.END
    user = await ensure_bound(update, context)
    if not user:
        await update.message.reply_text("Сначала привяжи аккаунт: /start")
        return ConversationHandler.END
    try:
        status_msg = await update.message.reply_text("⏳ Загружаю мероприятия…")
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
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Название не должно быть пустым. Введи название новой позиции:")
        return EXTRA_POSITION_NAME
    context.user_data["new_position_name"] = name
    await update.message.reply_text("Теперь введи сумму заявки:")
    return AMOUNT


async def get_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    amount = money_number(update.message.text)
    if not amount:
        await update.message.reply_text("Сумма должна быть числом. Например: 250000 или 250 000")
        return AMOUNT
    context.user_data["amount"] = amount
    await update.message.reply_text("Выбери способ оплаты:", reply_markup=payment_method_keyboard())
    return PAYMENT_METHOD


async def choose_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    digits = re.sub(r"\D", "", update.message.text or "")
    if len(digits) != 16:
        await update.message.reply_text("Номер карты должен содержать ровно 16 цифр. Попробуй ещё раз:")
        return CARD_NUMBER
    context.user_data["card_number"] = digits
    await update.message.reply_text("Комментарий к заявке? Если комментария нет — напиши «-».")
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
    remember_recently_published(request.get("paymentId"))
    manager_msg = await update.message.reply_html(
        payment_text(request, title),
        reply_markup=manager_keyboard(request.get("paymentId"), request.get("status")),
    )
    admin_msg = await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=payment_text(request, "🧾 Новая заявка на оплату"),
        parse_mode="HTML",
        reply_markup=admin_keyboard(request.get("paymentId"), request.get("status")),
    )
    remember_payment_messages(request.get("paymentId"), admin_msg.message_id, manager_msg.message_id, update.effective_user.id)
    if progress_msg:
        remember_progress_message(request.get("paymentId"), update.effective_chat.id, progress_msg.message_id)
    await mark_notified_retry(
        request.get("paymentId"),
        admin_message_id=admin_msg.message_id,
        manager_message_id=manager_msg.message_id,
    )


async def get_comment_and_submit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = (update.message.text or "").strip()
    if comment == "-":
        comment = ""
    context.user_data["comment"] = comment
    progress_msg = await update.message.reply_text("⏳ Отправляю заявку…")

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
            result = await api_async("create_request_instant", {"telegramId": update.effective_user.id, "payload": payload}, timeout=45)
        except Exception as instant_err:
            err_text = str(instant_err)
            if 'create_request_instant' not in err_text and 'Неизвестное действие' not in err_text:
                raise
            result = await api_async("create_request_fast", {"telegramId": update.effective_user.id, "payload": payload}, timeout=90)
        request = result.get("request", {})
        if payload.get("itemOrder") == EXTRA_ITEM_ORDER:
            clear_local_item_cache(update.effective_user.id, payload.get("eventId"))
            FLOW_CACHE.pop(flow_cache_key(update.effective_user.id), None)
        await publish_created_request_cards(update, context, request, progress_msg=progress_msg)
        await remove_progress_message(context, request.get("paymentId"))
        await update.message.reply_text("Готово. Заявка ушла админу и появилась на сайте.", reply_markup=MAIN_KEYBOARD)

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
            await update.message.reply_text("Готово. Заявка появилась на сайте. Повторно отправлять не нужно.", reply_markup=MAIN_KEYBOARD)
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
    user = await ensure_bound(update, context)
    if not user:
        await update.message.reply_text("Сначала привяжи аккаунт: /start")
        return
    progress_msg = await update.message.reply_text("⏳ Загружаю заявки…")
    try:
        result = api("list_my_requests", {"telegramId": update.effective_user.id})
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


async def handle_admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Обновляю статус…")
    if query.from_user.id != ADMIN_CHAT_ID:
        await query.answer("Эта кнопка только для админа.", show_alert=True)
        return
    _, action, payment_id = query.data.split(":", 2)
    status = {"paid": "Оплачено", "reject": "Отклонено", "cashin": "Деньги в кассе"}[action]
    old_text = query.message.text_html or query.message.text or ""
    await safe_edit_text(query.message, old_text + "\n\n⏳ Обновляю статус…", parse_mode="HTML")
    try:
        result = api("admin_update", {"paymentId": payment_id, "status": status, "comment": "Telegram"})
        request = result.get("request", {})

        # Финально закрытые заявки убираем из Telegram-чата: на сайте они уже в архиве.
        if is_cashbox_archived(request):
            await remove_archived_payment_messages(
                context,
                request,
                admin_message_id=query.message.message_id,
                manager_message_id=request.get("telegramManagerMessageId"),
                manager_telegram_id=request.get("telegramId"),
            )
        else:
            await query.edit_message_text(
                payment_text(request, "🧾 Заявка обновлена"),
                parse_mode="HTML",
                reply_markup=admin_keyboard(payment_id, request.get("status")),
            )
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
        try:
            api("mark_status_synced", {"paymentId": payment_id, "status": request.get("status")})
        except Exception:
            pass
    except Exception as err:
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
            reply_markup=admin_keyboard(request.get("paymentId"), request.get("status")) if is_admin else manager_keyboard(request.get("paymentId"), request.get("status")),
        )
        return True
    except Exception:
        return False


async def poll_status_updates(context: ContextTypes.DEFAULT_TYPE):
    try:
        result = api("list_status_updates", {})
        for request in result.get("requests", []):
            payment_id = request.get("paymentId")
            admin_msg_id = request.get("telegramAdminMessageId")
            manager_msg_id = request.get("telegramManagerMessageId")
            manager_tg = request.get("telegramId")

            if is_cashbox_archived(request):
                await remove_archived_payment_messages(
                    context,
                    request,
                    admin_message_id=admin_msg_id,
                    manager_message_id=manager_msg_id,
                    manager_telegram_id=manager_tg,
                )
            else:
                remember_payment_messages(payment_id, admin_msg_id, manager_msg_id, manager_tg)
                await edit_payment_message(
                    context,
                    ADMIN_CHAT_ID,
                    admin_msg_id,
                    request,
                    "🧾 Заявка обновлена",
                    is_admin=True,
                )

                if manager_tg and manager_msg_id:
                    await edit_payment_message(
                        context,
                        int(manager_tg),
                        manager_msg_id,
                        request,
                        "🔔 Статус заявки обновлён",
                        is_admin=False,
                    )

            try:
                api("mark_status_synced", {"paymentId": payment_id, "status": request.get("status")})
            except Exception:
                pass
    except Exception as err:
        print(f"poll_status_updates error: {err}")

async def poll_site_requests(context: ContextTypes.DEFAULT_TYPE):
    try:
        result = api("list_unnotified", {})
        for request in result.get("requests", []):
            payment_id = request.get("paymentId")
            if is_recently_published(payment_id):
                continue
            admin_msg = await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=payment_text(request, "🧾 Новая заявка с сайта"),
                parse_mode="HTML",
                reply_markup=admin_keyboard(payment_id, request.get("status")),
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
            remember_recently_published(payment_id)
            remember_payment_messages(payment_id, admin_msg.message_id, manager_msg_id, manager_tg)
            await mark_notified_retry(
                payment_id,
                admin_message_id=admin_msg.message_id,
                manager_message_id=manager_msg_id,
            )
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
        await update.message.reply_text("Действие отменено.", reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END


def main():
    require_env()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    bind_conversation = ConversationHandler(
        entry_points=[CommandHandler("start", start), MessageHandler(filters.Regex("^Привязать аккаунт$"), bind_start)],
        states={
            BIND_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, bind_phone)],
            BIND_PIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, bind_pin)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
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
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(bind_conversation)
    app.add_handler(request_conversation)
    app.add_handler(MessageHandler(filters.Regex("^Мои заявки$"), my_requests))
    app.add_handler(CallbackQueryHandler(handle_admin_action, pattern="^admin:"))
    app.add_handler(CallbackQueryHandler(handle_manager_action, pattern="^manager:"))
    app.add_handler(CommandHandler("cancel", cancel))

    app.run_polling()


if __name__ == "__main__":
    main()
