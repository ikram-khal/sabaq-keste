import asyncio
import base64
import io
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, Set, List, Optional

import openpyxl
import pandas as pd
import pytz
from fastapi import FastAPI, Request, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from openpyxl.utils import get_column_letter
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/tmp/schedule_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN не задан")

ALLOWED_USERS = [int(id) for id in os.getenv("ALLOWED_USERS", "").split(",") if id]
DRIVE_CREDENTIALS = os.getenv("DRIVE_CREDENTIALS")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
DATA_DIR = "/tmp/data"
DB_FILE = "/tmp/schedule.db"
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB

os.makedirs(DATA_DIR, exist_ok=True)

# Список преподавателей
TEACHER_NAMES = [
    "Tajieva A", "Mamirbaeva D", "Koyshekenova T", "Arzieva B", "Dauletmuratova X",
    "Jalgasov N", "Xudaybergenov A", "Allanazarova F", "Saparov S", "Balkibaeva V",
    # ... остальные преподаватели ...
]

# Столбцы групп
GROUP_COLUMNS_FIRST_COURSE = [4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28]
GROUP_COLUMNS_SECOND_COURSE = [33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 56, 58, 60, 62, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87]

# Диапазоны строк для дней недели
DAY_RANGES = ["5-16", "18-29", "31-42", "44-55", "57-68", "70-81"]
DAY_NAMES = ["DUYSEMBI", "SIYSHEMBI", "SARSHEMBI", "PIYSHEMBI", "JUMA", "SHEMBI"]

# Время пар
PAIR_TIMES = {
    1: "8:30-9:50",
    2: "10:00-11:20",
    3: "11:30-12:50",
    4: "13:00-14:20",
    5: "14:30-15:50",
    6: "16:00-17:20",
}

# Клавиатуры
ROLE_KEYBOARD = ReplyKeyboardMarkup(
    [["Oqıtıwshı", "Student"]],
    one_time_keyboard=True,
    resize_keyboard=True,
)

TEACHER_KEYBOARD = ReplyKeyboardMarkup(
    [["Búgin", "Erteń"], ["Kúndi tańlaw", "Tolıq hápteni kóriw"]],
    resize_keyboard=True,
)

TEACHER_KEYBOARD_ADMIN = ReplyKeyboardMarkup(
    [["Búgin", "Erteń"], ["Kúndi tańlaw", "Tolıq hápteni kóriw"], ["Kesteni óshiriw"]],
    resize_keyboard=True,
)

STUDENT_KEYBOARD = ReplyKeyboardMarkup(
    [["Búgin", "Erteń"], ["Kúndi tańlaw", "Tolıq hápteni kóriw"]],
    resize_keyboard=True,
)

DAY_KEYBOARD = ReplyKeyboardMarkup(
    [DAY_NAMES[i : i + 2] for i in range(0, len(DAY_NAMES), 2)] + [["Artqa qaytıw"]],
    resize_keyboard=True,
)

# Модели данных
class UserData:
    def __init__(self):
        self.role: Optional[str] = None
        self.teacher_name: Optional[str] = None
        self.group: Optional[str] = None
        self.notifications: bool = True


class BotData:
    def __init__(self):
        self.original_file: Optional[str] = None
        self.last_file: Optional[str] = None
        self.users: Dict[int, UserData] = {}
        self.subscribed_users: Set[int] = set()


# Инициализация Google Drive
def init_drive() -> build:
    try:
        if not DRIVE_CREDENTIALS:
            raise ValueError("DRIVE_CREDENTIALS не заданы")

        creds_json = base64.b64decode(DRIVE_CREDENTIALS).decode("utf-8")
        creds_dict = json.loads(creds_json)
        credentials = Credentials.from_service_account_info(
            creds_dict, scopes=["https://www.googleapis.com/auth/drive"]
        )
        drive_service = build("drive", "v3", credentials=credentials)
        logger.info("Google Drive initialized")
        return drive_service
    except Exception as e:
        logger.error(f"Google Drive init error: {str(e)}")
        raise


# Работа с базой данных
def init_db():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS original_schedule
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                Kun TEXT, Jupliq INTEGER, Topar TEXT,
                Pan TEXT, Oqitiwshi TEXT, Kabinet TEXT)"""
            )
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS changes_schedule
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                Kun TEXT, Jupliq INTEGER, Topar TEXT,
                Pan TEXT, Oqitiwshi TEXT, Kabinet TEXT)"""
            )
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS users
                (user_id INTEGER PRIMARY KEY,
                role TEXT, teacher_name TEXT, group_name TEXT,
                notifications INTEGER DEFAULT 1)"""
            )
            conn.commit()
            logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization error: {str(e)}")
        raise


def check_table_exists(table_name: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                (table_name,)
            )
            return bool(cursor.fetchone())
    except Exception as e:
        logger.error(f"Table check error for {table_name}: {str(e)}")
        return False


def save_to_db(df: pd.DataFrame, table_name: str):
    if not check_table_exists(table_name):
        init_db()

    try:
        with sqlite3.connect(DB_FILE) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            logger.info(f"Data saved to {table_name}")

            # Сохраняем резервную копию в Google Drive
            csv_path = os.path.join(DATA_DIR, f"{table_name}.csv")
            df.to_csv(csv_path, index=False)
            if drive_service := app.state.drive_service:
                upload_to_drive(drive_service, csv_path, DRIVE_FOLDER_ID)
    except Exception as e:
        logger.error(f"Database save error for {table_name}: {str(e)}")
        raise


def get_from_db(table_name: str) -> pd.DataFrame:
    if not check_table_exists(table_name):
        return pd.DataFrame()

    try:
        with sqlite3.connect(DB_FILE) as conn:
            return pd.read_sql(f"SELECT * FROM {table_name}", conn)
    except Exception as e:
        logger.error(f"Database read error for {table_name}: {str(e)}")
        return pd.DataFrame()


# Работа с Google Drive
def upload_to_drive(
    drive_service: build, file_path: str, folder_id: str
) -> Optional[str]:
    try:
        file_name = os.path.basename(file_path)
        file_metadata = {"name": file_name, "parents": [folder_id]}
        media = MediaFileUpload(file_path)

        file = (
            drive_service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        logger.info(f"Uploaded {file_name} to Google Drive, ID: {file.get('id')}")
        return file.get("id")
    except Exception as e:
        logger.error(f"Upload to Drive error for {file_path}: {str(e)}")
        return None


def download_latest_from_drive(
    drive_service: build, folder_id: str, prefix: str
) -> Optional[str]:
    try:
        results = (
            drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and name contains '{prefix}'",
                orderBy="createdTime desc",
                pageSize=1,
                fields="files(id, name)",
            )
            .execute()
        )
        files = results.get("files", [])

        if not files:
            logger.warning(f"No files found with prefix {prefix}")
            return None

        file_id = files[0]["id"]
        file_name = files[0]["name"]
        file_path = os.path.join(DATA_DIR, file_name)

        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(file_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        logger.info(f"Downloaded {file_name} from Google Drive to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Download from Drive error for prefix {prefix}: {str(e)}")
        return None


# Обработка Excel файлов
def check_excel_file(file_path: str) -> str:
    try:
        required_columns = ["Kun", "Jupliq", "Topar", "Pan", "Oqitiwshi", "Kabinet"]
        column_aliases = {
            "Jupliq": ["Jupliq", "Jupliq", "Пара", "Lesson"],
            "Kun": ["Kun", "Kún", "Day"],
            "Topar": ["Topar", "Group"],
            "Pan": ["Pan", "Pán", "Subject"],
            "Oqitiwshi": ["Oqitiwshi", "Oqıtıwshı", "Teacher"],
            "Kabinet": ["Kabinet", "Room"],
        }

        df = pd.read_excel(file_path, sheet_name="keste", engine="openpyxl")
        missing_cols = []
        rename_dict = {}

        for required_col in required_columns:
            found = False
            for alias in column_aliases[required_col]:
                if alias in df.columns:
                    rename_dict[alias] = required_col
                    found = True
                    break
            if not found:
                missing_cols.append(required_col)

        if missing_cols:
            return f"Missing columns: {', '.join(missing_cols)}"

        df.rename(columns=rename_dict, inplace=True)
        df.to_excel(file_path, sheet_name="keste", index=False, engine="openpyxl")
        return "OK"
    except Exception as e:
        return f"File check error: {str(e)}"


# FastAPI приложение
app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Инициализация сервисов
        app.state.drive_service = init_drive()
        await init_bot()
        yield
    finally:
        # Завершение работы
        if hasattr(app.state, "bot_app"):
            await app.state.bot_app.shutdown()


app = FastAPI(lifespan=lifespan)


@app.post("/{token}")
async def webhook(token: str, request: Request):
    if token != TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

    try:
        json_data = await request.json()
        update = Update.de_json(json_data, app.state.bot_app.bot)
        await app.state.bot_app.process_update(update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Webhook error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/")
async def root():
    return {"status": "running", "version": "1.0"}


# Основные функции бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    bot_data = get_bot_data(context)

    if user_id not in bot_data.users:
        bot_data.users[user_id] = UserData()
        save_user_to_db(user_id, bot_data.users[user_id])
    else:
        bot_data.subscribed_users.add(user_id)

    user_data = bot_data.users[user_id]

    if user_data.role:
        keyboard = (
            TEACHER_KEYBOARD_ADMIN
            if user_id in ALLOWED_USERS
            else TEACHER_KEYBOARD
            if user_data.role == "Oqıtıwshı"
            else STUDENT_KEYBOARD
        )
        await update.message.reply_text(
            f"Salem! Siz {user_data.role} sıpatında dizimnen óttińiz. "
            "Sabaq kesteńizdi kóriw ushın túymeni tańlań:",
            reply_markup=keyboard,
        )
    else:
        await update.message.reply_text(
            "Salem! Botqa xosh keldińiz. Óz rolińizdi tańlań:",
            reply_markup=ROLE_KEYBOARD,
        )

    logger.info(f"User {user_id} started the bot")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in ALLOWED_USERS:
        await update.message.reply_text("Sizde fayldı júklew ruxsatı joq!")
        logger.warning(f"Unauthorized file upload attempt by {user_id}")
        return

    bot_data = get_bot_data(context)
    document = update.message.document

    if document.file_size > MAX_FILE_SIZE:
        await update.message.reply_text("Fayl ólshemi 10MB nan úlken!")
        return

    try:
        file_name = document.file_name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(DATA_DIR, f"{file_name}_{timestamp}")

        # Скачивание файла
        file = await document.get_file()
        await file.download_to_drive(file_path)

        # Обработка файла
        if file_name == "Sabaq keste_DELL.xlsx":
            await process_working_schedule(file_path, bot_data, update)
        elif file_name.endswith(".xlsx"):
            await process_standard_schedule(file_path, bot_data, update)
        else:
            await update.message.reply_text("Qátelik! Fayl formati qáte. Faqat .xlsx fayldarı qabıllanadı.")

    except Exception as e:
        await update.message.reply_text(f"Qátelik: {str(e)}")
        logger.error(f"File upload error: {str(e)}")


async def process_working_schedule(file_path: str, bot_data: BotData, update: Update):
    """Обработка рабочего расписания"""
    try:
        temp_file_path = os.path.join(DATA_DIR, f"keste_bot_orig_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        
        if not process_excel_schedule(file_path, temp_file_path):
            await update.message.reply_text("Qátelik! Fayl óńdelmedi, mazmunı bo's yamasa qáte bar.")
            return

        bot_data.original_file = temp_file_path
        df = pd.read_excel(temp_file_path, sheet_name="keste", engine="openpyxl")
        save_to_db(df, "original_schedule")
        upload_to_drive(app.state.drive_service, temp_file_path, DRIVE_FOLDER_ID)
        
        await update.message.reply_text("Fayl sátli júklendi hám tekserip shıǵıldı!")
        await notify_users(context, True, user_id)
        
    except Exception as e:
        logger.error(f"Error processing working schedule: {str(e)}")
        raise


def get_bot_data(context: ContextTypes.DEFAULT_TYPE) -> BotData:
    if "bot_data" not in context.bot_data:
        context.bot_data["bot_data"] = BotData()
        context.bot_data["bot_data"].users = load_users_from_db()
    return context.bot_data["bot_data"]


async def init_bot():
    """Инициализация бота"""
    try:
        init_db()
        app.state.bot_app = Application.builder().token(TOKEN).build()
        bot_data = BotData()
        bot_data.users = load_users_from_db()
        app.state.bot_app.bot_data["bot_data"] = bot_data

        # Восстановление данных из Google Drive
        if drive_service := app.state.drive_service:
            await restore_from_drive(drive_service, bot_data)

        # Регистрация обработчиков
        app.state.bot_app.add_handler(CommandHandler("start", start))
        app.state.bot_app.add_handler(CommandHandler("notify_on", notify_on))
        app.state.bot_app.add_handler(CommandHandler("notify_off", notify_off))
        app.state.bot_app.add_handler(
            MessageHandler(filters.Regex(r"^(Oqıtıwshı|Student)$"), handle_role)
        )
        app.state.bot_app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
        app.state.bot_app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
        )

        await app.state.bot_app.initialize()
        await app.state.bot_app.start()
        logger.info("Bot initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize bot: {str(e)}")
        raise


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
