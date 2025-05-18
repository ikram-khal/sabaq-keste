import asyncio
import base64
import io
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import openpyxl
import pandas as pd
import pytz
from fastapi import FastAPI, HTTPException, Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import (Application, CommandHandler, ContextTypes,
                         MessageHandler, filters)

# ==================== Конфигурация ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("schedule_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

class Config:
    def __init__(self):
        self.TOKEN = os.getenv("BOT_TOKEN")
        if not self.TOKEN:
            raise ValueError("BOT_TOKEN не задан")
        
        self.ALLOWED_USERS = [int(id) for id in os.getenv("ALLOWED_USERS", "").split(",") if id]
        self.DRIVE_CREDENTIALS = os.getenv("DRIVE_CREDENTIALS")
        self.DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
        self.DATA_DIR = "data"
        self.DB_FILE = "schedule.db"
        self.MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
        
        os.makedirs(self.DATA_DIR, exist_ok=True)

config = Config()

# ==================== Модели данных ====================
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

# ==================== Константы ====================
TEACHER_NAMES = [
    "Tajieva A", "Mamirbaeva D", "Koyshekenova T", "Arzieva B", "Dauletmuratova X",
    # ... остальные преподаватели ...
]

GROUP_COLUMNS = {
    "first_course": [4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28],
    "second_course": [33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 56, 58, 60, 62, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87]
}

DAY_RANGES = ["5-16", "18-29", "31-42", "44-55", "57-68", "70-81"]
DAY_NAMES = ["DUYSEMBI", "SIYSHEMBI", "SARSHEMBI", "PIYSHEMBI", "JUMA", "SHEMBI"]
PAIR_TIMES = {
    1: "8:30-9:50",
    2: "10:00-11:20",
    3: "11:30-12:50",
    4: "13:00-14:20", 
    5: "14:30-15:50",
    6: "16:00-17:20"
}

# ==================== База данных ====================
class Database:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS original_schedule (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Kun TEXT, Jupliq INTEGER, Topar TEXT,
                    Pan TEXT, Oqitiwshi TEXT, Kabinet TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS changes_schedule (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Kun TEXT, Jupliq INTEGER, Topar TEXT,
                    Pan TEXT, Oqitiwshi TEXT, Kabinet TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    role TEXT, teacher_name TEXT, group_name TEXT,
                    notifications INTEGER DEFAULT 1
                )
            """)
            conn.commit()

    def save_data(self, df: pd.DataFrame, table_name: str):
        with sqlite3.connect(self.db_file) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)

    def load_data(self, table_name: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_file) as conn:
            return pd.read_sql(f"SELECT * FROM {table_name}", conn)

    def save_user(self, user_id: int, user_data: UserData):
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO users 
                (user_id, role, teacher_name, group_name, notifications)
                VALUES (?, ?, ?, ?, ?)
            """, (
                user_id,
                user_data.role,
                user_data.teacher_name,
                user_data.group,
                int(user_data.notifications)
            )
            conn.commit()

    def load_users(self) -> Dict[int, UserData]:
        users = {}
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, role, teacher_name, group_name, notifications FROM users")
            for row in cursor.fetchall():
                user_data = UserData()
                user_data.role = row[1]
                user_data.teacher_name = row[2]
                user_data.group = row[3]
                user_data.notifications = bool(row[4])
                users[row[0]] = user_data
        return users

db = Database(config.DB_FILE)

# ==================== Google Drive ====================
class DriveService:
    def __init__(self, credentials: str, folder_id: str):
        self.credentials = credentials
        self.folder_id = folder_id
        self.service = self._init_service()

    def _init_service(self) -> build:
        try:
            creds_json = base64.b64decode(self.credentials).decode("utf-8")
            creds_dict = json.loads(creds_json)
            credentials = Credentials.from_service_account_info(
                creds_dict, scopes=["https://www.googleapis.com/auth/drive"]
            )
            return build("drive", "v3", credentials=credentials)
        except Exception as e:
            logger.error(f"Google Drive init error: {e}")
            raise

    def upload_file(self, file_path: str) -> Optional[str]:
        try:
            file_name = os.path.basename(file_path)
            file_metadata = {
                "name": file_name,
                "parents": [self.folder_id]
            }
            media = MediaFileUpload(file_path)
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id"
            ).execute()
            return file.get("id")
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return None

    def download_latest(self, prefix: str) -> Optional[str]:
        try:
            results = self.service.files().list(
                q=f"'{self.folder_id}' in parents and name contains '{prefix}'",
                orderBy="createdTime desc",
                pageSize=1,
                fields="files(id, name)"
            ).execute()
            
            if not (files := results.get("files", [])):
                return None

            file_path = os.path.join(config.DATA_DIR, files[0]["name"])
            request = self.service.files().get_media(fileId=files[0]["id"])
            
            with io.FileIO(file_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                while not downloader.next_chunk()[1]:
                    pass
            
            return file_path
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None

drive = DriveService(config.DRIVE_CREDENTIALS, config.DRIVE_FOLDER_ID) if config.DRIVE_CREDENTIALS else None

# ==================== Обработка Excel ====================
class ScheduleProcessor:
    @staticmethod
    def process_working_schedule(file_path: str) -> Optional[pd.DataFrame]:
        try:
            wb = openpyxl.load_workbook(file_path, data_only=True)
            ws = wb.active
            schedule_data = []

            for teacher in TEACHER_NAMES:
                ScheduleProcessor._process_course(ws, teacher, GROUP_COLUMNS["first_course"], 3, schedule_data)
                ScheduleProcessor._process_course(ws, teacher, GROUP_COLUMNS["second_course"], 32, schedule_data)

            if not schedule_data:
                return None

            return pd.DataFrame(schedule_data)[["Kun", "Jupliq", "Topar", "Pan", "Oqitiwshi", "Kabinet"]]
        except Exception as e:
            logger.error(f"Schedule processing error: {e}")
            return None

    @staticmethod
    def _process_course(ws, teacher: str, columns: List[int], time_col: int, result: List[dict]):
        for day_idx, day_range in enumerate(DAY_RANGES):
            day = DAY_NAMES[day_idx]
            start, end = map(int, day_range.split("-"))
            
            for row in range(start, end + 1):
                time = ws.cell(row=row, column=time_col).value or "JOQ"
                
                for col in columns:
                    cell = ws.cell(row=row + 1, column=col)
                    if cell.is_merged and cell.merge_area[0][0].column == col:
                        if cell.value and teacher.lower() in str(cell.value).lower():
                            result.append({
                                "Oqitiwshi": teacher,
                                "Kun": day,
                                "Jupliq": time,
                                "Topar": ws.cell(row=3, column=col).value or "",
                                "Pan": ws.cell(row=row, column=col).value or "JOQ",
                                "Kabinet": ws.cell(row=row, column=col + 1).value or "JOQ"
                            })

# ==================== Telegram Bot ====================
class TelegramBot:
    def __init__(self):
        self.app = None
        self.bot_data = BotData()
        self._init_handlers()

    async def initialize(self):
        self.app = Application.builder().token(config.TOKEN).build()
        self.app.bot_data["bot_data"] = self.bot_data
        self._setup_handlers()
        await self._restore_data()
        await self.app.initialize()
        await self.app.start()

    async def shutdown(self):
        if self.app:
            await self.app.shutdown()

    def _init_handlers(self):
        self.handlers = [
            CommandHandler("start", self._start),
            CommandHandler("notify_on", self._notify_on),
            CommandHandler("notify_off", self._notify_off),
            MessageHandler(filters.Regex(r"^(Oqıtıwshı|Student)$"), self._handle_role),
            MessageHandler(filters.Document.ALL, self._handle_file),
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message)
        ]

    def _setup_handlers(self):
        for handler in self.handlers:
            self.app.add_handler(handler)

    async def _restore_data(self):
        # Восстановление пользователей
        self.bot_data.users = db.load_users()
        self.bot_data.subscribed_users = {
            uid for uid, ud in self.bot_data.users.items() if ud.notifications
        }

        # Восстановление расписания из Google Drive
        if drive:
            if orig_file := drive.download_latest("keste_bot_orig"):
                self.bot_data.original_file = orig_file
                db.save_data(pd.read_excel(orig_file), "original_schedule")
            
            if changes_file := drive.download_latest("keste_bot_ozgeris"):
                self.bot_data.last_file = changes_file
                db.save_data(pd.read_excel(changes_file), "changes_schedule")

    # ... остальные методы обработчиков ...

# ==================== FastAPI ====================
app = FastAPI()
bot = TelegramBot()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.initialize()
    yield
    await bot.shutdown()

app = FastAPI(lifespan=lifespan)

@app.post("/{token}")
async def webhook(token: str, request: Request):
    if token != config.TOKEN:
        raise HTTPException(status_code=403)
    
    try:
        update = Update.de_json(await request.json(), bot.app.bot)
        await bot.app.process_update(update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/")
async def status():
    return {"status": "running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
