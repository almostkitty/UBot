from yt_dlp import YoutubeDL
from mutagen.mp3 import MP3
import os

MAX_SIZE = 48*1024*1024 # Максимальный размер аудиофайла в Telegram в байтах

def get_audio(link):
    def downloader(link):
        ydl_opts = {
            'format': 'bestaudio[abr<=128]/bestaudio',
            # 'outtmpl': '%(title)s.%(ext)s',
            'outtmpl': 'input.%(ext)s',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '128',  # битрейт MP3
            }],
        }

        try:
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(link, download=True)
                title = info.get('title')
                channel = info.get('uploader')
            return "input.mp3", title, channel
        except Exception as e:
            print(f"Ошибка при скачивании: {e}")
            return None, None, None

    audio_file, title, channel = downloader(link)
    # print(f"Название видео: {title}")
    # print(f"Канал: {channel}")

    SIZE = os.path.getsize(audio_file)

    audio = MP3(audio_file)
    # print(audio.info.length)
    # print(audio.info.bitrate)

    CHUNK = MAX_SIZE*8 / audio.info.bitrate
    # print(f"50 Мб аудио при данном битрейте займут примерно {CHUNK} секунд или {round(CHUNK/60, 2)} минут")
    # print("Длина изначальной дорожки:", audio.info.length)

    if SIZE <= MAX_SIZE:
        print("Файл поместится в одно сообщение.")
        return [audio_file], title, channel
    else:
        print("Необходимо уменьшить размер.")
        chunk_files = []
        def cut_audio():
            try:
                with open(audio_file, "rb") as f:
                    chunk_num = 1
                    while True:
                        chunk_data = f.read(MAX_SIZE)
                        if not chunk_data:
                            break
                        chunk_name = f"chunk_{chunk_num}.mp3"
                        with open(chunk_name, "wb") as chunk_file:
                            chunk_file.write(chunk_data)
                        print(f"Сохранён {chunk_name} ({len(chunk_data)} байт)")
                        chunk_files.append(chunk_name)
                        chunk_num += 1
            except Exception as e:
                print(f"Ошибка при нарезке файла: {e}")

        cut_audio()
        return chunk_files, title, channel
