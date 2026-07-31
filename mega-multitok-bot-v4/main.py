from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramEntityTooLarge,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    User,
)
from dotenv import load_dotenv
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

BASE_DIR = Path(__file__).resolve().parent
WELCOME_IMAGE = BASE_DIR / "assets" / "welcome.png"
DATABASE_PATH = BASE_DIR / "mega_multitok.db"

load_dotenv(BASE_DIR / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
RAW_ADMIN_IDS = os.getenv("ADMIN_IDS", os.getenv("ADMIN_ID", "")).strip()
ADMIN_IDS = {
    int(value.strip())
    for value in RAW_ADMIN_IDS.split(",")
    if value.strip().isdigit()
}

MAX_FILE_MB = 49
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

URL_RE = re.compile(r"https?://[^\s<>]+", re.IGNORECASE)
ALLOWED_DOMAINS = {
    "tiktok.com",
    "vm.tiktok.com",
    "vt.tiktok.com",
    "youtube.com",
    "youtu.be",
    "instagram.com",
    "x.com",
    "twitter.com",
    "facebook.com",
    "fb.watch",
    "reddit.com",
    "v.redd.it",
    "vk.com",
    "rutube.ru",
    "twitch.tv",
    "clips.twitch.tv",
    "pinterest.com",
    "pin.it",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("mega-multitok")

router = Router()
download_slots = asyncio.Semaphore(2)
user_locks: dict[int, asyncio.Lock] = {}

START_TEXT = (
    "🔴 <b>Mega MultiTok</b>\n\n"
    "🔗 Отправь ссылку на видео, и бот пришлёт готовый файл.\n\n"
    "❤️‍🔥 TikTok, обычные видео YouTube, YouTube Shorts, "
    "Instagram Reels и другие площадки.\n\n"
    "❗️Закрытые и удалённые видео скачать нельзя."
)

HELP_TEXT = (
    "🔴 <b>Как скачать видео</b>\n\n"
    "1. Скопируй ссылку на ролик\n"
    "2. Отправь её сюда\n"
    "3. Дождись готового файла\n\n"
    "❤️ Поддерживаются обычные ролики YouTube и Shorts."
)

VIDEO_CAPTION = (
    "🔴 <b>Всё готово!</b>\n"
    "❤️ Видео скачано. Отправляй следующую ссылку."
)

START_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="🔴 TikTok", callback_data="platform_tiktok"),
            InlineKeyboardButton(text="🔴 YouTube", callback_data="platform_youtube"),
        ],
        [
            InlineKeyboardButton(text="🔴 Instagram", callback_data="platform_instagram"),
            InlineKeyboardButton(text="🔴 Другие сайты", callback_data="supported_sites"),
        ],
    ]
)

ADMIN_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="📣 Новая рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="✖️ Закрыть", callback_data="admin_close")],
    ]
)

BROADCAST_CANCEL_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="✖️ Отмена", callback_data="admin_broadcast_cancel")]
    ]
)

BROADCAST_CONFIRM_KEYBOARD = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="✅ Начать рассылку", callback_data="admin_broadcast_confirm")],
        [InlineKeyboardButton(text="✖️ Отмена", callback_data="admin_broadcast_cancel")],
    ]
)


class BroadcastState(StatesGroup):
    waiting_message = State()
    waiting_confirmation = State()


class DownloadProblem(RuntimeError):
    pass


class FileTooLarge(DownloadProblem):
    pass


def init_database_sync() -> None:
    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        connection.commit()


def save_user_sync(user: User | None) -> None:
    if user is None or user.is_bot:
        return

    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            """
            INSERT INTO users (user_id, username, first_name, joined_at, last_seen_at, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                last_seen_at = excluded.last_seen_at,
                is_active = 1
            """,
            (user.id, user.username, user.first_name, now, now),
        )
        connection.commit()


def get_users_sync(active_only: bool = True) -> list[int]:
    query = "SELECT user_id FROM users"
    if active_only:
        query += " WHERE is_active = 1"
    with sqlite3.connect(DATABASE_PATH) as connection:
        return [row[0] for row in connection.execute(query).fetchall()]


def get_stats_sync() -> tuple[int, int]:
    with sqlite3.connect(DATABASE_PATH) as connection:
        total = connection.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        active = connection.execute(
            "SELECT COUNT(*) FROM users WHERE is_active = 1"
        ).fetchone()[0]
    return total, active


def mark_user_inactive_sync(user_id: int) -> None:
    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            "UPDATE users SET is_active = 0 WHERE user_id = ?",
            (user_id,),
        )
        connection.commit()


async def save_user(user: User | None) -> None:
    await asyncio.to_thread(save_user_sync, user)


def is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def extract_url(text: str) -> str | None:
    match = URL_RE.search(text or "")
    if not match:
        return None
    return match.group(0).rstrip(".,);]}>\"'")


def is_supported_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    return parsed.scheme in {"http", "https"} and any(
        host == domain or host.endswith(f".{domain}") for domain in ALLOWED_DOMAINS
    )


def find_downloaded_file(folder: Path) -> Path:
    ignored = {".part", ".ytdl", ".json", ".jpg", ".jpeg", ".png", ".webp"}
    files = [
        path
        for path in folder.iterdir()
        if path.is_file() and path.suffix.lower() not in ignored
    ]
    if not files:
        raise DownloadProblem("Файл после загрузки не найден.")
    return max(files, key=lambda path: path.stat().st_size)


def clear_folder(folder: Path) -> None:
    for item in folder.iterdir():
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        else:
            item.unlink(missing_ok=True)


def friendly_error(error_text: str) -> str:
    text = error_text.lower()
    if "private" in text or "login" in text or "sign in" in text:
        return "❗️Видео закрыто или требует входа в аккаунт."
    if "unsupported url" in text:
        return "❗️Эта ссылка пока не поддерживается."
    if "unavailable" in text or "not available" in text:
        return "❗️Видео недоступно или удалено."
    if "copyright" in text:
        return "❗️Видео недоступно из-за ограничений правообладателя."
    return "❗️Не удалось скачать видео. Попробуй другую ссылку или повтори позже."


def download_video_sync(url: str, folder: Path) -> Path:
    selectors = (
        "best[ext=mp4][height<=720]/best[height<=720]/best[ext=mp4]/best",
        "best[ext=mp4][height<=480]/best[height<=480]/best",
        "best[ext=mp4][height<=360]/best[height<=360]/worst",
        "best[ext=mp4][height<=240]/best[height<=240]/worst",
    )

    last_error: Exception | None = None

    for selector in selectors:
        clear_folder(folder)
        options = {
            "format": selector,
            "outtmpl": str(folder / "%(id)s.%(ext)s"),
            "noplaylist": True,
            "restrictfilenames": True,
            "windowsfilenames": True,
            "quiet": True,
            "no_warnings": True,
            "retries": 3,
            "fragment_retries": 3,
            "socket_timeout": 30,
            "max_filesize": MAX_FILE_BYTES,
        }

        try:
            with YoutubeDL(options) as ydl:
                info = ydl.extract_info(url, download=True)

            if info is None:
                raise DownloadProblem("Площадка не вернула данные о видео.")

            video_path = find_downloaded_file(folder)
            if video_path.stat().st_size > MAX_FILE_BYTES:
                raise FileTooLarge

            return video_path

        except FileTooLarge as exc:
            last_error = exc
        except DownloadError as exc:
            error_text = str(exc)
            if "larger than max-filesize" in error_text.lower():
                last_error = FileTooLarge(error_text)
                continue
            raise DownloadProblem(friendly_error(error_text)) from exc
        except OSError as exc:
            last_error = exc

    if isinstance(last_error, FileTooLarge):
        raise FileTooLarge("❗️Ролик слишком большой. Попробуй более короткое видео.")
    raise DownloadProblem("❗️Не удалось скачать видео.") from last_error


async def download_video(url: str, folder: Path) -> Path:
    async with download_slots:
        return await asyncio.to_thread(download_video_sync, url, folder)


async def show_admin_panel(message: Message) -> None:
    total, active = await asyncio.to_thread(get_stats_sync)
    await message.answer(
        "🔴 <b>Админ-панель Mega MultiTok</b>\n\n"
        f"👥 Всего пользователей: <b>{total}</b>\n"
        f"🟢 Доступно для рассылки: <b>{active}</b>",
        reply_markup=ADMIN_KEYBOARD,
    )


@router.message(CommandStart())
async def start_handler(message: Message) -> None:
    await save_user(message.from_user)
    if WELCOME_IMAGE.exists():
        await message.answer_photo(
            photo=FSInputFile(WELCOME_IMAGE),
            caption=START_TEXT,
            reply_markup=START_KEYBOARD,
        )
    else:
        await message.answer(START_TEXT, reply_markup=START_KEYBOARD)


@router.message(Command("help"))
async def help_handler(message: Message) -> None:
    await save_user(message.from_user)
    await message.answer(HELP_TEXT)


@router.message(Command("admin"))
async def admin_handler(message: Message, state: FSMContext) -> None:
    await save_user(message.from_user)
    if not is_admin(message.from_user.id if message.from_user else None):
        await message.answer("❗️Нет доступа к админ-панели.")
        return
    await state.clear()
    await show_admin_panel(message)


@router.callback_query(F.data.startswith("platform_"))
async def platform_hint_handler(callback: CallbackQuery) -> None:
    await save_user(callback.from_user)
    platform = callback.data.removeprefix("platform_").capitalize()
    await callback.answer(
        f"Отправь сюда ссылку на видео из {platform}.",
        show_alert=False,
    )


@router.callback_query(F.data == "supported_sites")
async def supported_sites_handler(callback: CallbackQuery) -> None:
    await save_user(callback.from_user)
    await callback.answer(
        "TikTok, YouTube, Shorts, Instagram, X, VK, Rutube, Reddit, Twitch и Pinterest.",
        show_alert=True,
    )


@router.callback_query(F.data == "admin_stats")
async def admin_stats_handler(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    total, active = await asyncio.to_thread(get_stats_sync)
    await callback.answer(
        f"Всего: {total}\nДоступно для рассылки: {active}",
        show_alert=True,
    )


@router.callback_query(F.data == "admin_close")
async def admin_close_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    if callback.message:
        await callback.message.delete()
    await callback.answer()


@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(BroadcastState.waiting_message)
    if callback.message:
        await callback.message.answer(
            "📣 <b>Пришли рекламное сообщение</b>\n\n"
            "Можно отправить текст, фото, видео, документ или сообщение с подписью. "
            "После этого бот покажет подтверждение.",
            reply_markup=BROADCAST_CANCEL_KEYBOARD,
        )
    await callback.answer()


@router.message(BroadcastState.waiting_message)
async def admin_broadcast_message_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    if not is_admin(message.from_user.id if message.from_user else None):
        await state.clear()
        return

    try:
        preview = await bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
    except TelegramBadRequest:
        await message.answer(
            "❗️Это сообщение нельзя использовать для рассылки. Пришли обычный текст, фото или видео.",
            reply_markup=BROADCAST_CANCEL_KEYBOARD,
        )
        return

    await state.update_data(
        source_chat_id=message.chat.id,
        source_message_id=message.message_id,
        preview_message_id=preview.message_id,
    )
    await state.set_state(BroadcastState.waiting_confirmation)
    total, active = await asyncio.to_thread(get_stats_sync)
    await message.answer(
        "🔴 <b>Сообщение готово к рассылке</b>\n\n"
        f"Получателей: <b>{active}</b> из {total}\n"
        "Нажми кнопку ниже, чтобы начать.",
        reply_markup=BROADCAST_CONFIRM_KEYBOARD,
    )


@router.callback_query(F.data == "admin_broadcast_cancel")
async def admin_broadcast_cancel_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    if callback.message:
        await callback.message.edit_text("✖️ Рассылка отменена.")
    await callback.answer()


async def copy_broadcast_message(
    bot: Bot,
    user_id: int,
    source_chat_id: int,
    source_message_id: int,
) -> bool:
    try:
        await bot.copy_message(
            chat_id=user_id,
            from_chat_id=source_chat_id,
            message_id=source_message_id,
        )
        return True
    except TelegramRetryAfter as exc:
        await asyncio.sleep(float(exc.retry_after) + 0.5)
        try:
            await bot.copy_message(
                chat_id=user_id,
                from_chat_id=source_chat_id,
                message_id=source_message_id,
            )
            return True
        except (TelegramForbiddenError, TelegramBadRequest):
            await asyncio.to_thread(mark_user_inactive_sync, user_id)
            return False
    except (TelegramForbiddenError, TelegramBadRequest):
        await asyncio.to_thread(mark_user_inactive_sync, user_id)
        return False
    except Exception:
        logger.exception("Broadcast error for user %s", user_id)
        return False


@router.callback_query(
    BroadcastState.waiting_confirmation,
    F.data == "admin_broadcast_confirm",
)
async def admin_broadcast_confirm_handler(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    data = await state.get_data()
    source_chat_id = data.get("source_chat_id")
    source_message_id = data.get("source_message_id")
    if not source_chat_id or not source_message_id:
        await state.clear()
        await callback.answer("Сообщение для рассылки не найдено", show_alert=True)
        return

    await state.clear()
    if callback.message:
        await callback.message.edit_text("📣 Рассылка запущена...")
    await callback.answer()

    users = await asyncio.to_thread(get_users_sync, True)
    sent = 0
    failed = 0

    for user_id in users:
        success = await copy_broadcast_message(
            bot=bot,
            user_id=user_id,
            source_chat_id=int(source_chat_id),
            source_message_id=int(source_message_id),
        )
        if success:
            sent += 1
        else:
            failed += 1
        await asyncio.sleep(0.04)

    if callback.message:
        await callback.message.answer(
            "✅ <b>Рассылка завершена</b>\n\n"
            f"Отправлено: <b>{sent}</b>\n"
            f"Не доставлено: <b>{failed}</b>",
            reply_markup=ADMIN_KEYBOARD,
        )


@router.message(F.text)
async def link_handler(message: Message, bot: Bot) -> None:
    await save_user(message.from_user)
    text = message.text or ""
    url = extract_url(text)

    if not url:
        await message.answer("🔗 Отправь обычную ссылку на видео.")
        return

    if not is_supported_url(url):
        await message.answer(
            "❗️Пока принимаю ссылки из TikTok, YouTube, Instagram, X, VK, "
            "Rutube, Reddit, Twitch и Pinterest."
        )
        return

    user_id = message.from_user.id if message.from_user else message.chat.id
    lock = user_locks.setdefault(user_id, asyncio.Lock())

    if lock.locked():
        await message.answer("🔴 Предыдущая ссылка ещё обрабатывается. Подожди немного.")
        return

    async with lock:
        status = await message.answer("🔴 Скачиваю видео...")
        temp_folder = Path(tempfile.mkdtemp(prefix="mega_multitok_"))

        try:
            await bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)
            path = await download_video(url, temp_folder)

            await status.edit_text("❤️‍🔥 Отправляю файл...")
            visible_name = f"Mega_MultiTok{path.suffix.lower()}"
            upload = FSInputFile(path, filename=visible_name)

            try:
                await message.answer_video(
                    video=upload,
                    caption=VIDEO_CAPTION,
                    supports_streaming=True,
                )
            except TelegramBadRequest:
                upload = FSInputFile(path, filename=visible_name)
                await message.answer_document(
                    document=upload,
                    caption=VIDEO_CAPTION,
                )

            await status.delete()

        except FileTooLarge as exc:
            await status.edit_text(str(exc))
        except TelegramEntityTooLarge:
            await status.edit_text("❗️Telegram не принял файл из-за его размера.")
        except DownloadProblem as exc:
            await status.edit_text(str(exc))
        except Exception:
            logger.exception("Unexpected error")
            await status.edit_text("❗️Произошла ошибка. Попробуй ещё раз позже.")
        finally:
            shutil.rmtree(temp_folder, ignore_errors=True)
            user_locks.pop(user_id, None)


@router.message()
async def fallback_handler(message: Message) -> None:
    await save_user(message.from_user)
    await message.answer("🔗 Пришли ссылку на видео обычным сообщением.")


async def main() -> None:
    if not BOT_TOKEN or BOT_TOKEN == "PASTE_BOT_TOKEN_HERE":
        raise RuntimeError("Открой файл .env и вставь BOT_TOKEN от @BotFather.")

    await asyncio.to_thread(init_database_sync)

    session = AiohttpSession(timeout=180)
    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher(storage=MemoryStorage())
    dispatcher.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Mega MultiTok V4 запущен")

    try:
        await dispatcher.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
