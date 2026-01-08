import asyncio
import logging
import os
import random
import re
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command, BaseFilter, and_f
from aiogram.utils.markdown import hbold
from aiogram.types import FSInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from tools import get_audio
import database

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

# Валидация переменных .env
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен!")
if not ADMIN_ID:
    raise ValueError("ADMIN_ID не установлен!")

try:
    ADMIN_ID = int(ADMIN_ID)
except (ValueError, TypeError):
    raise ValueError(f"ADMIN_ID должен быть числом, получено: {ADMIN_ID}")


YOUTUBE_REGEX = re.compile(
    r"(https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s]+)"
)

download_queue = asyncio.Queue()
processing_task: asyncio.Task | None = None

# Логи
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log", encoding="utf-8")]
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def get_cover_thumbnail():
    """Обложка для аудио"""
    cover_paths = ["cover.png", "cover.jpeg"]
    for cover_path in cover_paths:
        if os.path.exists(cover_path):
            return FSInputFile(cover_path)
    return None

class AuthorizedUserFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return await database.is_user_registered(message.from_user.id)

class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id == ADMIN_ID

class ThanksFilter(BaseFilter):
    """Очень специальный фильтр сообщений :D"""
    async def __call__(self, message: types.Message) -> bool:
        if not message.text:
            return False
        text = message.text.lower().strip()
        if re.search(YOUTUBE_REGEX, text):
            return False
        thanks_words = [
            'спасибо', 'благодарю', 'благодарность', 'thanks', 'thank you',
            'мерси', 'пасибо', 'спс', 'thx', 'ty'
        ]
        return any(word in text for word in thanks_words)

authorized_user = AuthorizedUserFilter()
admin_only = AdminFilter()
thanks_filter = ThanksFilter()


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    full_name = message.from_user.full_name
    username = message.from_user.username
    
    await database.create_user(user_id, full_name, username)
    
    if await database.is_user_registered(user_id):
        await message.answer("Вы авторизованы.\nПришлите ссылки на YouTube-видео.")
    else:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Разрешить", callback_data=f"allow_{user_id}")],
                [InlineKeyboardButton(text="Отклонить", callback_data=f"deny_{user_id}")]
            ]
        )
        await bot.send_message(
            ADMIN_ID,
            f"Пользователь {hbold(full_name)} "
            f"(ID: <code>{user_id}</code>) хочет авторизоваться.",
            reply_markup=kb
        )
        await message.answer("Вы не авторизованы. Запрос отправлен администратору.")

@dp.callback_query(lambda c: c.data.startswith("allow_"))
async def allow_user(query: types.CallbackQuery):
    if query.from_user.id != ADMIN_ID:
        await query.answer("Только администратор может разрешать доступ", show_alert=True)
        return
    
    user_id = int(query.data.split("_")[1])
    user_info = await database.get_user_by_telegram_id(user_id)
    full_name = user_info.get('full_name', f'ID: {user_id}') if user_info else f'ID: {user_id}'
    
    success = await database.register_user(user_id)
    if success:
        await query.answer("Пользователь добавлен")
        try:
            await query.message.edit_text(
                f"Пользователь {hbold(full_name)} "
                f"(ID: <code>{user_id}</code>) - <b>зарегистрирован</b>.",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.debug(f"Не удалось отредактировать сообщение: {e}")
        
        await bot.send_message(user_id, "Доступ разрешён! Теперь вы можете пользоваться ботом.")
        logging.info(f"User {user_id} registered by admin")
    else:
        await query.answer("Ошибка при регистрации пользователя", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("deny_"))
async def deny_user(query: types.CallbackQuery):
    if query.from_user.id != ADMIN_ID:
        await query.answer("Только администратор может отклонять запросы", show_alert=True)
        return
    
    user_id = int(query.data.split("_")[1])
    user_info = await database.get_user_by_telegram_id(user_id)
    full_name = user_info.get('full_name', f'ID: {user_id}') if user_info else f'ID: {user_id}'
    
    await query.answer("Запрос отклонён")
    try:
        await query.message.edit_text(
            f"Пользователь {hbold(full_name)} "
            f"(ID: <code>{user_id}</code>) - <b>отклонён</b>.",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.debug(f"Не удалось отредактировать сообщение: {e}")
    
    await bot.send_message(user_id, "Ваш запрос на доступ отклонён администратором.")

@dp.message(Command("ping"))
async def cmd_ping(message: types.Message):
    """Проверка работоспособности"""
    await message.answer("Pong! Бот работает.")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Статистика бота"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("Команда доступна только администратору.")
        return
    
    stats = await database.get_statistics()
    total_size_mb = stats['total_size'] / (1024 * 1024) if stats['total_size'] else 0
    total_requests = stats.get('total_requests', 0)
    savings = total_requests - stats['total_downloads'] if total_requests > 0 else 0
    text = (
        f"<b>Статистика бота:</b>\n\n"
        f"Всего пользователей: {stats['total_users']}\n"
        f"Зарегистрированных: {stats['registered_users']}\n"
        f"Всего попыток скачивания: {total_requests}\n"
        f"Уникальных видео скачано: {stats['total_downloads']}\n"
        f"Уникальных видео в кэше: {stats['unique_videos']}\n"
        f"Экономия: {savings} скачиваний из кэша\n"
        f"Общий размер: {total_size_mb:.2f} МБ"
    )
    await message.answer(text, parse_mode="HTML")

@dp.message(and_f(authorized_user, thanks_filter))
async def handle_thanks(message: types.Message):
    """Обработчик очень специальных сообщений"""
    responses = [
        "Пожалуйста! Рад помочь!",
        "Всегда пожалуйста!",
        "Обращайтесь!",
        "Пожалуйста! Если что-то еще нужно - пишите!",
        "Не за что! Приятно помочь!"
    ]
    await message.answer(random.choice(responses))

@dp.message(authorized_user)
async def handle_links(message: types.Message):
    urls = re.findall(YOUTUBE_REGEX, message.text or "")
    if not urls:
        await message.answer("Нет ссылок на YouTube.")
        return

    # Валидация URL
    valid_urls = []
    for url in urls:
        url_lower = url.lower()
        is_valid = (
            url.startswith(('http://', 'https://')) and 
            ('youtube.com' in url_lower or 'youtu.be' in url_lower)
        )
        if is_valid:
            valid_urls.append(url)
        else:
            logging.warning(f"Пропущен некорректный URL: {url}")
    
    if not valid_urls:
        await message.answer("Не найдено валидных ссылок на YouTube.")
        return

    for url in valid_urls:
        await download_queue.put((message, url))

    global processing_task
    if not processing_task or processing_task.done():
        processing_task = asyncio.create_task(worker())


# Обработчик скачивания с кэшированием
async def process_audio_download(message: types.Message, url: str):
    user_id = message.from_user.id
    
    user_db = await database.get_user_by_telegram_id(user_id)
    if not user_db:
        full_name = message.from_user.full_name
        username = message.from_user.username
        db_user_id = await database.create_user(user_id, full_name, username)
        if not db_user_id:
            await message.answer("Ошибка: не удалось создать пользователя в базе данных.")
            logging.error(f"Failed to create user {user_id} in database")
            return
        user_db = await database.get_user_by_telegram_id(user_id)
        if not user_db:
            await message.answer("Ошибка: пользователь не найден в базе данных.")
            return
    
    db_user_id = user_db['id']
    
    # Проверяем есть ли уже это видео в БД
    cached_parts = await database.get_video_by_url(url)
    if cached_parts:
        part_numbers = [p.get('part_number', 0) for p in cached_parts]
        logging.info(f"Cache HIT for {url}: found {len(cached_parts)} parts with numbers {part_numbers}")
    else:
        logging.info(f"Cache MISS for {url}: not found in database")
    
    if cached_parts:
        # Отправляем из кэша если есть
        total_parts = len(cached_parts)
        logging.info(f"Processing {total_parts} cached parts for {url}")
        if total_parts == 1:
            # Один файл
            cached_video = cached_parts[0]
            try:
                thumbnail = get_cover_thumbnail()
                await bot.send_audio(
                    message.chat.id,
                    cached_video['file_id'],
                    title=cached_video.get('title'),
                    performer=cached_video.get('performer'),
                    thumbnail=thumbnail
                )
                # Обновлие статистики
                try:
                    await database.increment_user_requests(db_user_id)
                except Exception as stats_error:
                    logging.warning(f"Failed to update stats: {stats_error}")
                logging.info(f"Sent cached audio for {url} to user {user_id}")
                return
            except Exception as e:
                logging.warning(f"Failed to send cached file_id, will re-download: {e}")
                await message.answer(f"Кэш устарел, скачиваю заново...")
        else:
            # Несколько частей
            try:
                thumbnail = get_cover_thumbnail()
                for part in cached_parts:
                    part_num = part.get('part_number', 1)
                    part_title = part.get('title', '')
                    if f"(часть {part_num})" not in part_title:
                        title = f"{part_title} (часть {part_num})"
                    else:
                        title = part_title
                    
                    await bot.send_audio(
                        message.chat.id,
                        part['file_id'],
                        title=title,
                        performer=part.get('performer'),
                        thumbnail=thumbnail
                    )
                    logging.debug(f"Sending cached part {part_num}/{total_parts} for {url}")
                # Обновление статистики
                try:
                    await database.increment_user_requests(db_user_id)
                except Exception as stats_error:
                    logging.warning(f"Failed to update stats: {stats_error}")
                logging.info(f"Sent cached audio ({total_parts} parts) for {url} to user {user_id}")
                return
            except Exception as e:
                logging.warning(f"Failed to send cached file_id, will re-download: {e}")
                await message.answer(f"Кэш устарел, скачиваю заново...")
    
    # Видео нет в кэше или кэш не сработал. Скачиваем
    status = await message.answer(f"Скачиваю аудио...")
    files = None
    
    try:
        files, title, performer = await asyncio.to_thread(get_audio, url)
        if not files:
            await status.edit_text("Ошибка при скачивании!")
            return

        if len(files) == 1:
            thumbnail = get_cover_thumbnail()
            sent_message = await bot.send_audio(
                message.chat.id,
                FSInputFile(files[0]),
                title=title,
                performer=performer,
                thumbnail=thumbnail
            )
            # Сохраняем file_id для кэширования
            if sent_message.audio:
                file_id = sent_message.audio.file_id
                file_size = sent_message.audio.file_size
                await database.save_video(
                    youtube_url=url,
                    user_id=db_user_id,
                    file_id=file_id,
                    file_size=file_size,
                    title=title,
                    performer=performer,
                    part_number=1,
                    total_parts=1
                )
                logging.info(f"Saved video to cache: {url} -> {file_id}")
            try:
                await status.delete()
            except Exception as e:
                logging.debug(f"Не удалось удалить статус-сообщение: {e}")
        else:
            await status.edit_text("Файл большой, отправляю по частям...")
            total_parts = len(files)
            thumbnail = get_cover_thumbnail()
            # Сохраняем все части в кэш
            for i, f in enumerate(files, 1):
                sent_message = await bot.send_audio(
                    message.chat.id,
                    FSInputFile(f),
                    title=f"{title} (часть {i})",
                    performer=performer,
                    thumbnail=thumbnail
                )
                # Сохраняем каждую часть в кэш
                if sent_message.audio:
                    file_id = sent_message.audio.file_id
                    file_size = sent_message.audio.file_size
                    result = await database.save_video(
                        youtube_url=url,
                        user_id=db_user_id,
                        file_id=file_id,
                        file_size=file_size,
                        title=title,
                        performer=performer,
                        part_number=i,
                        total_parts=total_parts
                    )
                    if result > 0:
                        logging.info(f"Saved video part {i}/{total_parts} to cache: {url} -> {file_id}")
                    else:
                        logging.info(f"Video part {i} already in cache, skipped: {url}")
                        if i == 1:
                            # Если первая часть уже в кэше, значит весь файл уже есть
                            break
            try:
                await status.delete()
            except Exception as e:
                logging.debug(f"Не удалось удалить статус-сообщение: {e}")

    except Exception as e:
        logging.exception("Ошибка обработки")
        try:
            await status.edit_text(f"Ошибка при скачивании или обработке аудио: {e}")
        except Exception as edit_error:
            logging.debug(f"Не удалось обновить статус, отправляю новое сообщение: {edit_error}")
            await message.answer(f"Ошибка при скачивании или обработке аудио: {e}")
    finally:
        # безопасная очистка временных файлов
        if files:
            for f in files:
                if os.path.exists(f):
                    try:
                        os.remove(f)
                    except Exception as cleanup_error:
                        logging.warning(f"Не удалось удалить файл {f}: {cleanup_error}")


async def worker():
    """Воркер для обработки очереди скачивания"""
    while True:
        try:
            msg, url = await download_queue.get()
            await process_audio_download(msg, url)
            download_queue.task_done()
        except Exception as e:
            logging.exception(f"Ошибка в worker при обработке {url}: {e}")
            download_queue.task_done()


async def main():
    await database.init_db()
    logging.info("Бот запускается...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
