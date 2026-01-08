import aiosqlite
import logging
from datetime import datetime
from typing import Optional, List, Tuple

import os

DATA_DIR = os.getenv("DATA_DIR", ".")
DB_PATH = os.path.join(DATA_DIR, "bot.db")

if DATA_DIR != ".":
    os.makedirs(DATA_DIR, exist_ok=True)


async def init_db():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Таблица пользователей
            await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                full_name TEXT,
                registered BOOLEAN DEFAULT 0,
                registered_at TEXT,
                downloads_count INTEGER DEFAULT 0,
                requests_count INTEGER DEFAULT 0,
                total_downloaded_size INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            try:
                await db.execute("ALTER TABLE users ADD COLUMN username TEXT")
            except aiosqlite.OperationalError:
                pass
            
            try:
                await db.execute("ALTER TABLE users ADD COLUMN downloads_count INTEGER DEFAULT 0")
            except aiosqlite.OperationalError:
                pass
            
            try:
                await db.execute("ALTER TABLE users ADD COLUMN total_downloaded_size INTEGER DEFAULT 0")
            except aiosqlite.OperationalError:
                pass
            
            try:
                await db.execute("ALTER TABLE users ADD COLUMN requests_count INTEGER DEFAULT 0")
            except aiosqlite.OperationalError:
                pass


            await db.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                youtube_url TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                file_size INTEGER,
                title TEXT,
                performer TEXT,
                part_number INTEGER DEFAULT 1,
                total_parts INTEGER DEFAULT 1,
                downloaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                UNIQUE(youtube_url, part_number)
            )
            """)
            
            try:
                await db.execute("ALTER TABLE videos ADD COLUMN part_number INTEGER DEFAULT 1")
            except aiosqlite.OperationalError:
                pass
            
            try:
                await db.execute("ALTER TABLE videos ADD COLUMN total_parts INTEGER DEFAULT 1")
            except aiosqlite.OperationalError:
                pass


            try:
                await db.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_videos_url_part 
                    ON videos(youtube_url, part_number)
                """)
            except aiosqlite.OperationalError:
                pass
            
            # Индекс для быстрого поиска по URL
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_youtube_url ON videos(youtube_url)
            """)
            
            # Индекс для поиска по telegram_id пользователя
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_telegram_id ON users(telegram_id)
            """)
            
            await db.commit()
        logging.info("База данных инициализирована")
    except Exception as e:
        logging.error(f"Критическая ошибка при инициализации БД: {e}")
        raise


async def get_user_by_telegram_id(telegram_id: int) -> Optional[dict]:
    """Получение пользователя по telegram_id"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None


async def is_user_registered(telegram_id: int) -> bool:
    """Проверить регистрацию пользователя"""
    user = await get_user_by_telegram_id(telegram_id)
    if user is None:
        return False
    registered = user.get('registered')
    return registered == 1 or registered is True


async def create_user(telegram_id: int, full_name: str = None, username: str = None) -> int:
    """Создать нового пользователя"""
    async with aiosqlite.connect(DB_PATH) as db:
        existing_user = await get_user_by_telegram_id(telegram_id)
        if existing_user:
            if username != existing_user.get('username') or full_name != existing_user.get('full_name'):
                await db.execute(
                    """UPDATE users SET username = ?, full_name = ? WHERE telegram_id = ?""",
                    (username, full_name, telegram_id)
                )
                await db.commit()
            return existing_user['id']
        
        cursor = await db.execute(
            """INSERT INTO users (telegram_id, username, full_name, registered) 
               VALUES (?, ?, ?, 0)""",
            (telegram_id, username, full_name)
        )
        await db.commit()
        return cursor.lastrowid


async def register_user(telegram_id: int) -> bool:
    """Зарегистрировать пользователя"""
    async with aiosqlite.connect(DB_PATH) as db:
        registered_at = datetime.now().isoformat()
        cursor = await db.execute(
            """UPDATE users 
               SET registered = 1, registered_at = ? 
               WHERE telegram_id = ?""",
            (registered_at, telegram_id)
        )
        await db.commit()
        return cursor.rowcount > 0


async def get_all_users() -> List[dict]:
    """Получить всех пользователей"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users ORDER BY created_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]



async def get_video_by_url(youtube_url: str) -> Optional[List[dict]]:
    """Получить информацию о видео по URL (возвращает все части самой свежей загрузки)"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        async with db.execute(
            """SELECT * FROM videos 
               WHERE youtube_url = ? 
               ORDER BY downloaded_at DESC, part_number ASC""",
            (youtube_url,)
        ) as cursor:
            rows = await cursor.fetchall()
            if not rows:
                logging.debug(f"Cache miss for {youtube_url}: no records found")
                return None
            
            # Собираем все уникальные части (берем самую свежую версию каждой части)
            # Группируем по part_number и берем самую свежую запись для каждой части
            parts_by_number = {}
            for row in rows:
                row_dict = dict(row)
                part_num = row_dict.get('part_number', 1)
                downloaded_at = row_dict.get('downloaded_at', '')
                
                # Если такой части еще нет, или эта версия новее - сохраняем
                if part_num not in parts_by_number:
                    parts_by_number[part_num] = row_dict
                else:
                    # Сравниваем по времени (берем более свежую версию)
                    existing_time = parts_by_number[part_num].get('downloaded_at', '')
                    if downloaded_at > existing_time:
                        parts_by_number[part_num] = row_dict
            
            # Преобразуем в список и сортируем по part_number
            unique_parts = [parts_by_number[num] for num in sorted(parts_by_number.keys())]
            
            # Определяем total_parts из первой части
            total_parts = unique_parts[0].get('total_parts', 1) if unique_parts else 1
            
            # Если все части на месте, возвращаем их
            if len(unique_parts) == total_parts:
                logging.info(f"Cache hit for {youtube_url}: {len(unique_parts)} parts found (complete)")
                return unique_parts
            
            # Если есть хотя бы одна часть - возвращаем то что есть
            # (даже если не все части сохранены - лучше отправить что есть чем скачивать заново)
            if len(unique_parts) > 0:
                logging.info(f"Cache hit for {youtube_url}: {len(unique_parts)} parts found (expected {total_parts}, but using what we have)")
                return unique_parts
            
            # Если ничего не найдено
            logging.warning(f"No parts found for {youtube_url}")
            return None


async def save_video(
    youtube_url: str,
    user_id: int,
    file_id: str,
    file_size: int = None,
    title: str = None,
    performer: str = None,
    part_number: int = 1,
    total_parts: int = 1
) -> int:
    """Сохранить информацию о скачанном видео (или его части)"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем, есть ли уже эта конкретная часть в кэше
        async with db.execute(
            """SELECT id FROM videos 
               WHERE youtube_url = ? AND part_number = ? 
               ORDER BY downloaded_at DESC LIMIT 1""",
            (youtube_url, part_number)
        ) as cursor:
            existing_row = await cursor.fetchone()
            if existing_row:
                logging.info(f"Video part {part_number} already in cache for {youtube_url}, skipping save")
                return existing_row[0]
        
        # Если части нет в кэше - сохраняем
        try:
            cursor = await db.execute(
                """INSERT INTO videos 
                   (youtube_url, user_id, file_id, file_size, title, performer, part_number, total_parts) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (youtube_url, user_id, file_id, file_size, title, performer, part_number, total_parts)
            )
            
            # Обновляем статистику пользователя
            # Считаем только уникальные видео (по youtube_url), не части
            if part_number == 1:  # Обновляем счетчики только для первой части
                # Проверяем, это первое скачивание этого видео этим пользователем?
                async with db.execute(
                    """SELECT COUNT(*) FROM videos 
                       WHERE user_id = ? AND youtube_url = ? AND part_number = 1""",
                    (user_id, youtube_url)
                ) as count_cursor:
                    is_first_download = (await count_cursor.fetchone())[0] == 1
                
                if is_first_download:
                    # Увеличиваем счетчик скачанных видео
                    await db.execute(
                        """UPDATE users 
                           SET downloads_count = downloads_count + 1 
                           WHERE id = ?""",
                        (user_id,)
                    )
                
                # Всегда увеличиваем счетчик попыток скачивания
                await db.execute(
                    """UPDATE users 
                       SET requests_count = requests_count + 1 
                       WHERE id = ?""",
                    (user_id,)
                )
                
                # Добавляем размер файла к общему размеру
                if file_size:
                    await db.execute(
                        """UPDATE users 
                           SET total_downloaded_size = total_downloaded_size + ? 
                           WHERE id = ?""",
                        (file_size, user_id)
                    )
            
            await db.commit()
            logging.info(f"Saved video part {part_number}/{total_parts} to cache: {youtube_url}")
            return cursor.lastrowid
        except aiosqlite.IntegrityError as e:
            # Если запись уже существует (UNIQUE constraint), просто возвращаем существующий id
            logging.warning(f"Video part {part_number} already exists in cache (IntegrityError): {e}")
            # Получаем существующую запись
            async with db.execute(
                """SELECT id FROM videos 
                   WHERE youtube_url = ? AND part_number = ? 
                   ORDER BY downloaded_at DESC LIMIT 1""",
                (youtube_url, part_number)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return row[0]
            return 0


async def increment_user_requests(user_id: int):
    """Увеличить счетчик попыток скачивания пользователя (используется при отправке из кэша)"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE users 
               SET requests_count = requests_count + 1 
               WHERE id = ?""",
            (user_id,)
        )
        await db.commit()


async def get_user_videos(telegram_id: int, limit: int = 10) -> List[dict]:
    """Получить видео, скачанные пользователем"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT v.* FROM videos v
               JOIN users u ON v.user_id = u.id
               WHERE u.telegram_id = ?
               ORDER BY v.downloaded_at DESC
               LIMIT ?""",
            (telegram_id, limit)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_statistics() -> dict:
    """Получить статистику по базе данных"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Количество пользователей
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            total_users = (await cursor.fetchone())[0]
        
        async with db.execute("SELECT COUNT(*) FROM users WHERE registered = 1") as cursor:
            registered_users = (await cursor.fetchone())[0]
        
        # Количество видео
        async with db.execute("SELECT COUNT(*) FROM videos") as cursor:
            total_videos = (await cursor.fetchone())[0]
        
        # Количество уникальных URL
        async with db.execute("SELECT COUNT(DISTINCT youtube_url) FROM videos") as cursor:
            unique_videos = (await cursor.fetchone())[0]
        
        # Общий размер скачанных файлов
        async with db.execute("SELECT SUM(total_downloaded_size) FROM users") as cursor:
            total_size = (await cursor.fetchone())[0] or 0
        
        # Общее количество скачанных видео
        async with db.execute("SELECT SUM(downloads_count) FROM users") as cursor:
            total_downloads = (await cursor.fetchone())[0] or 0
        
        # Общее количество попыток скачивания (включая из кэша)
        async with db.execute("SELECT SUM(requests_count) FROM users") as cursor:
            total_requests = (await cursor.fetchone())[0] or 0
        
        return {
            'total_users': total_users,
            'registered_users': registered_users,
            'total_videos': total_videos,
            'unique_videos': unique_videos,
            'total_downloads': total_downloads,
            'total_requests': total_requests,
            'total_size': total_size
        }
