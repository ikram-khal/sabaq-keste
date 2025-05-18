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
                    Kun TEXT, 
                    Jupliq INTEGER, 
                    Topar TEXT,
                    Pan TEXT, 
                    Oqitiwshi TEXT, 
                    Kabinet TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS changes_schedule (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Kun TEXT, 
                    Jupliq INTEGER, 
                    Topar TEXT,
                    Pan TEXT, 
                    Oqitiwshi TEXT, 
                    Kabinet TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    role TEXT, 
                    teacher_name TEXT, 
                    group_name TEXT,
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

# ==================== FastAPI приложение ====================
app = FastAPI()
bot_data = BotData()

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Инициализация бота
        application = Application.builder().token(config.TOKEN).build()
        application.bot_data["bot_data"] = bot_data
        
        # Регистрация обработчиков
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("notify_on", notify_on))
        application.add_handler(CommandHandler("notify_off", notify_off))
        application.add_handler(MessageHandler(filters.Regex(r"^(Oqıtıwshı|Student)$"), handle_role))
        application.add_handler(MessageHandler(filters.Document.ALL, handle_file))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        
        # Восстановление данных
        bot_data.users = db.load_users()
        bot_data.subscribed_users = {uid for uid, ud in bot_data.users.items() if ud.notifications}
        
        await application.initialize()
        await application.start()
        
        app.state.bot_app = application
        yield
    finally:
        if hasattr(app.state, "bot_app"):
            await app.state.bot_app.shutdown()

app = FastAPI(lifespan=lifespan)

@app.post("/{token}")
async def webhook(token: str, request: Request):
    if token != config.TOKEN:
        raise HTTPException(status_code=403)
    
    try:
        update = Update.de_json(await request.json(), app.state.bot_app.bot)
        await app.state.bot_app.process_update(update)
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
