import asyncio
import logging
import os
import re
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command, BaseFilter
from aiogram.utils.markdown import hbold
from aiogram.types import FSInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from tools import get_audio

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
WHITELIST_PATH = "white_list.txt"

# Обработка ссылок
YOUTUBE_REGEX = re.compile(
    r"(https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s]+)"
)

# Очереди 
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

def get_whitelist() -> set[int]:
    if not os.path.exists(WHITELIST_PATH):
        return set()
    with open(WHITELIST_PATH, "r", encoding="utf-8") as f:
        return {int(line.strip()) for line in f if line.strip().isdigit()}

def add_to_whitelist(user_id: int):
    if user_id not in get_whitelist():
        with open(WHITELIST_PATH, "a", encoding="utf-8") as f:
            f.write(f"{user_id}\n")
        logging.info(f"Пользователь {user_id} добавлен в whitelist.")

class AuthorizedUserFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id in get_whitelist()

class AdminFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id == ADMIN_ID

authorized_user = AuthorizedUserFilter()
admin_only = AdminFilter()


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    if user_id in get_whitelist():
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
            f"Пользователь {hbold(message.from_user.full_name)} "
            f"(ID: <code>{user_id}</code>) хочет авторизоваться.",
            reply_markup=kb
        )
        await message.answer("Вы не авторизованы. Запрос отправлен администратору.")

@dp.callback_query(lambda c: c.data.startswith("allow_"))
async def allow_user(query: types.CallbackQuery):
    user_id = int(query.data.split("_")[1])
    add_to_whitelist(user_id)
    await query.answer("Пользователь добавлен")
    await bot.send_message(user_id, "Доступ разрешён! Теперь вы можете пользоваться ботом.")

@dp.callback_query(lambda c: c.data.startswith("deny_"))
async def deny_user(query: types.CallbackQuery):
    user_id = int(query.data.split("_")[1])
    await query.answer("Запрос отклонён")
    await bot.send_message(user_id, "Ваш запрос на доступ отклонён администратором.")

@dp.message(authorized_user)
async def handle_links(message: types.Message):
    urls = re.findall(YOUTUBE_REGEX, message.text or "")
    if not urls:
        await message.answer("Нет ссылок на YouTube.")
        return

    for url in urls:
        await download_queue.put((message, url))
    await message.answer(f"В очередь добавлено {len(urls)} видео.")

    global processing_task
    if not processing_task or processing_task.done():
        processing_task = asyncio.create_task(worker())


async def process_audio_download(message: types.Message, url: str):
    status = await message.answer(f"Скачиваю аудио:\n{url}")
    try:
        files, title, performer = await asyncio.to_thread(get_audio, url)
        if not files:
            await status.edit_text("Ошибка при скачивании!")
            return

        if len(files) == 1:
            await status.edit_text("Готово!")
            await bot.send_audio(
                message.chat.id,
                FSInputFile(files[0]),
                title=title,
                performer=performer
                # thumbnail=FSInputFile("cover.jpg")
            )
        else:
            await status.edit_text("Файл большой, отправляю по частям...")
            for i, f in enumerate(files, 1):
                await message.answer(f"Часть {i}:")
                await bot.send_audio(
                    message.chat.id,
                    FSInputFile(f),
                    title=f"{title} (часть {i})",
                    performer=performer
                    # thumbnail=FSInputFile("cover.jpg")
                )

    except Exception as e:
        logging.exception("Ошибка обработки")
        await status.edit_text(f"Ошибка при скачивании или обработке аудио: {e}")
    finally:
        for f in os.listdir():
            if f.startswith("input.") or f.startswith("chunk_"):
                try:
                    os.remove(f)
                except Exception as cleanup_error:
                    logging.warning(f"Не удалось удалить файл {f}: {cleanup_error}")


async def worker():
    while True:
        msg, url = await download_queue.get()
        await process_audio_download(msg, url)
        download_queue.task_done()


if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot, skip_updates=True))
