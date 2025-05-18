import os
import logging
import base64
import json
import traceback
import sys
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from dotenv import load_dotenv
import pandas as pd
import openpyxl
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Настройка логирования
logger = logging.getLogger("bot")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Логи в файл
file_handler = logging.FileHandler("/tmp/schedule_bot.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Логи в stderr для Render
stream_handler = logging.StreamHandler(sys.stderr)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# Загрузка переменных окружения
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ALLOWED_USERS = [int(user_id) for user_id in os.getenv("ALLOWED_USERS", "").split(",") if user_id]
DRIVE_CREDENTIALS = os.getenv("DRIVE_CREDENTIALS")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
DATA_DIR = "/tmp/data"
DB_FILE = "/tmp/schedule.db"

if not BOT_TOKEN:
    logger.error("BOT_TOKEN не задан")
    raise ValueError("BOT_TOKEN не задан")

# Создание директории для данных
os.makedirs(DATA_DIR, exist_ok=True)

# Инициализация Google Drive API
def get_drive_service():
    try:
        creds_json = base64.b64decode(DRIVE_CREDENTIALS).decode("utf-8")
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/drive"])
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        logger.error(f"Ошибка инициализации Google Drive: {str(e)}\n{traceback.format_exc()}")
        raise

# Параметры расписания
TEACHER_NAMES = [
    "Aymanova Sh", "Oringalieva D", "Yakupova K", "Esbergenova G", "Kalimbetova K",
    "Dauletiyarova N", "Jiemuratova G", "Xabibnazarova S", "Tursunbaev B", "Esemuratova T",
    "Narshabaeva A", "Xalmuratov I", "Bisenova A", "Dauletmuratova X", "Dauletbaeva N",
    "Madaminova N", "Balkibarva V", "Tajieva A", "Jalgasbaeva G", "Elmuratova Z",
    "Matmuratova G", "Bayimbetova M", "Naubetullaeva E", "Qaypova B", "Koyshekenova T",
    "Utebaeva A", "Arzieva B", "Bayniyazov A", "Abdiev B", "Joldasbaev O", "Kanlibaeva E",
    "Kurbanbaeva U", "Utemisov A", "Atamuratova M", "Seytjanova U", "Utepbergenova D",
    "Saparov S", "Allanazarova F", "Mamirbaeva D", "Balkibaeva V"
]
GROUP_COLUMNS_FIRST_COURSE = [4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28]
GROUP_COLUMNS_SECOND_COURSE = [33, 35, 37, 39, 41, 43, 45, 47, 49, 51]
GROUP_COLUMNS_THIRD_COURSE = [56, 58, 60, 62]
GROUP_COLUMNS_FOURTH_COURSE = [67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87]
DAY_NAMES = ["DUYSEMBI", "SIYSHEMBI", "SARSHEMBI", "PIYSHEMBI", "JUMA", "SHEMBI"]
DAY_RANGES = ["7-18", "20-31", "33-44", "46-57", "59-70", "72-83"]
GROUP_UNIONS = {
    ("101", "102"): "101-102",
    ("103", "104"): "103-104",
}

# Инициализация FastAPI
app = FastAPI()

# Инициализация Telegram бота
application = None

# Функции для работы с Google Drive
def upload_to_drive(file_path, file_name):
    try:
        drive_service = get_drive_service()
        file_metadata = {
            "name": file_name,
            "parents": [DRIVE_FOLDER_ID]
        }
        media = MediaFileUpload(file_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        logger.info(f"Uploaded {file_name} to Google Drive, ID: {file.get('id')}")
        return file.get("id")
    except Exception as e:
        logger.error(f"Error uploading to Google Drive: {str(e)}\n{traceback.format_exc()}")
        raise

def download_latest_from_drive():
    try:
        drive_service = get_drive_service()
        query = f"'{DRIVE_FOLDER_ID}' in parents and name contains 'keste_bot_orig_'"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])
        if not files:
            logger.warning("No schedule files found in Google Drive")
            return None
        latest_file = max(files, key=lambda x: x["name"])
        file_id = latest_file["id"]
        request = drive_service.files().get_media(fileId=file_id)
        file_path = os.path.join(DATA_DIR, latest_file["name"])
        with open(file_path, "wb") as f:
            f.write(request.execute())
        logger.info(f"Downloaded latest schedule: {latest_file['name']}")
        return file_path
    except Exception as e:
        logger.error(f"Error downloading from Google Drive: {str(e)}\n{traceback.format_exc()}")
        raise

# Функции обработки расписания
def create_column_set(group_columns):
    return set(group_columns)

def contains_teacher_name(cell_value, teacher_name):
    if not cell_value or not isinstance(cell_value, str):
        return False
    return teacher_name.lower() in cell_value.lower()

def get_group_list(ws, merge_area, group_col_set):
    group_list = []
    start_col, start_row = merge_area.min_col, merge_area.min_row
    end_col = merge_area.max_col
    for col in range(start_col, end_col + 1):
        if col in group_col_set:
            group_name = ws.cell(row=start_row - 2, column=col).value
            if group_name:
                group_list.append(str(group_name))
    return group_list

def get_audience(ws, row, col, group_columns):
    next_col = col + 1
    if next_col in group_columns:
        next_col += 1
    audience = ws.cell(row=row, column=next_col).value or "JOQ"
    return str(audience)

def get_union_name(group_list, group_unions):
    group_tuple = tuple(sorted(group_list))
    return group_unions.get(group_tuple, ", ".join(group_list))

def is_cell_in_merged(ws, row, col):
    for merged_range in ws.merged_cells.ranges:
        if merged_range.min_row <= row <= merged_range.max_row and merged_range.min_col <= col <= merged_range.max_col:
            return merged_range
    return None

def process_course(ws, teacher_name, group_columns, time_column, schedule_data):
    group_col_set = create_column_set(group_columns)
    
    for day_idx, day_range in enumerate(DAY_RANGES):
        day = DAY_NAMES[day_idx]
        start_row, end_row = map(int, day_range.split("-"))
        
        logger.debug(f"Processing {day} (rows {start_row}-{end_row}) for teacher {teacher_name}")
        
        for row in range(start_row, end_row + 1):
            time = ws.cell(row=row, column=time_column).value or "JOQ"
            if isinstance(time, str):
                time = time.split("\n")[0] if "\n" in time else time
            logger.debug(f"Row {row}, time: {time}")
            
            for col in group_columns:
                cell = ws.cell(row=row + 1, column=col)
                merge_area = is_cell_in_merged(ws, row + 1, col)
                if merge_area:
                    start_col, start_row = merge_area.min_col, merge_area.min_row
                    if col == start_col:
                        cell_value = ws.cell(row=start_row, column=start_col).value
                        logger.debug(f"Column {col}, cell value: {cell_value}")
                        if cell_value and contains_teacher_name(cell_value, teacher_name):
                            group_list = get_group_list(ws, merge_area, group_col_set)
                            group_name = get_union_name(group_list, GROUP_UNIONS)
                            subject = ws.cell(row=row, column=start_col).value or "JOQ"
                            audience = get_audience(ws, row, start_col + (merge_area.max_col - start_col), group_columns)
                            logger.debug(f"Found schedule: teacher={teacher_name}, group={group_name}, subject={subject}, audience={audience}")
                            schedule_data.append({
                                "Oqitiwshi": teacher_name,
                                "Kun": day,
                                "Jupliq": time,
                                "Topar": group_name,
                                "Pan": subject,
                                "Kabinet": audience
                            })

def process_working_schedule(file_path, temp_file_path):
    try:
        file_size = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"Processing file {file_path}, size: {file_size:.2f} MB")
        if file_size > 50:
            logger.error("File too large for processing")
            return False
        
        if not os.access(os.path.dirname(temp_file_path), os.W_OK):
            logger.error(f"No write permission for {os.path.dirname(temp_file_path)}")
            return False
        
        wb = openpyxl.load_workbook(file_path, data_only=True)
        ws = wb.active
        schedule_data = []
        
        for teacher in TEACHER_NAMES:
            process_course(ws, teacher, GROUP_COLUMNS_FIRST_COURSE, 3, schedule_data)
            process_course(ws, teacher, GROUP_COLUMNS_SECOND_COURSE, 32, schedule_data)
            process_course(ws, teacher, GROUP_COLUMNS_THIRD_COURSE, 32, schedule_data)
            process_course(ws, teacher, GROUP_COLUMNS_FOURTH_COURSE, 32, schedule_data)
        
        df = pd.DataFrame(schedule_data)
        if df.empty:
            logger.warning("No schedule data extracted from working schedule")
            return False
        df = df[["Kun", "Jupliq", "Topar", "Pan", "Oqitiwshi", "Kabinet"]]
        
        if not create_temp_schedule_file(df, temp_file_path):
            return False
        return True
    except Exception as e:
        logger.error(f"Error processing working schedule: {str(e)}\n{traceback.format_exc()}")
        return False

def create_temp_schedule_file(df, temp_file_path):
    try:
        df.to_excel(temp_file_path, index=False)
        logger.info(f"Temporary schedule file created: {temp_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error creating temporary file {temp_file_path}: {str(e)}\n{traceback.format_exc()}")
        return False

# Функции Telegram бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.debug(f"Received /start from user {user_id}")
    if user_id not in ALLOWED_USERS:
        await update.message.reply_text("Sizge ruxsat joq!")
        logger.warning(f"Unauthorized access by user {user_id}")
        return
    keyboard = [
        [InlineKeyboardButton("Oqıtıwshı", callback_data="teacher")],
        [InlineKeyboardButton("Student", callback_data="student")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Salem! Botqa xosh keldińiz. Óz rolińizdi tańlań:", reply_markup=reply_markup)
    logger.info(f"User {user_id} started the bot")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.debug(f"Received button callback from user {user_id}: {query.data}")
    if user_id not in ALLOWED_USERS:
        await query.message.reply_text("Sizge ruxsat joq!")
        logger.warning(f"Unauthorized access by user {user_id}")
        return
    role = query.data
    context.user_data["role"] = role
    if role == "teacher":
        await query.message.reply_text("Óz atıńızdı jazıń (mısalı: Tajieva A):")
    else:
        await query.message.reply_text("Topar nomerin jazıń (mısalı: 101):")
    logger.info(f"User {user_id} selected role: {role}")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.debug(f"Received text from user {user_id}: {update.message.text}")
    if user_id not in ALLOWED_USERS:
        await update.message.reply_text("Sizge ruxsat joq!")
        logger.warning(f"Unauthorized access by user {user_id}")
        return
    role = context.user_data.get("role")
    if not role:
        await update.message.reply_text("Aldın rolińizdi tańlań (/start)!")
        logger.warning(f"User {user_id} sent text without selecting role")
        return
    text = update.message.text.strip()
    if role == "teacher" and text not in TEACHER_NAMES:
        await update.message.reply_text("Bunday oqıtıwshı tabılmadı. Qaytadan jazıń:")
        logger.warning(f"User {user_id} entered invalid teacher name: {text}")
        return
    context.user_data["identifier"] = text
    keyboard = [
        [InlineKeyboardButton("Búgin", callback_data="today")],
        [InlineKeyboardButton("Erteń", callback_data="tomorrow")],
        [InlineKeyboardButton("Tolıq keste", callback_data="full")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Kesteni tańlań:", reply_markup=reply_markup)
    logger.info(f"User {user_id} set identifier: {text}")

async def schedule_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.debug(f"Received schedule callback from user {user_id}: {query.data}")
    if user_id not in ALLOWED_USERS:
        await query.message.reply_text("Sizge ruxsat joq!")
        logger.warning(f"Unauthorized access by user {user_id}")
        return
    role = context.user_data.get("role")
    identifier = context.user_data.get("identifier")
    if not role or not identifier:
        await query.message.reply_text("Aldın rolińizdi hám identifikatorıńızdi tańlań (/start)!")
        logger.warning(f"User {user_id} requested schedule without role/identifier")
        return
    
    file_path = download_latest_from_drive()
    if not file_path:
        await query.message.reply_text("Keste faylı tabılmadı!")
        logger.warning(f"No schedule file found for user {user_id}")
        return
    
    try:
        df = pd.read_excel(file_path)
        today = datetime.now().strftime("%A").upper()
        tomorrow = (datetime.now() + pd.Timedelta(days=1)).strftime("%A").upper()
        day_map = {
            "MONDAY": "DUYSEMBI",
            "TUESDAY": "SIYSHEMBI",
            "WEDNESDAY": "SARSHEMBI",
            "THURSDAY": "PIYSHEMBI",
            "FRIDAY": "JUMA",
            "SATURDAY": "SHEMBI",
            "SUNDAY": "JEKSHEMBI"
        }
        today = day_map.get(today, "DUYSEMBI")
        tomorrow = day_map.get(tomorrow, "SIYSHEMBI")
        
        if query.data == "today":
            df_filtered = df[df["Kun"] == today]
        elif query.data == "tomorrow":
            df_filtered = df[df["Kun"] == tomorrow]
        else:
            df_filtered = df
        
        if role == "teacher":
            df_filtered = df_filtered[df_filtered["Oqitiwshi"] == identifier]
        else:
            df_filtered = df_filtered[df_filtered["Topar"].str.contains(identifier, case=False)]
        
        if df_filtered.empty:
            await query.message.reply_text("Bul kúnge keste joq!")
            logger.info(f"No schedule found for user {user_id}, {query.data}")
            return
        
        response = []
        for _, row in df_filtered.iterrows():
            response.append(
                f"Kún: {row['Kun']}\n"
                f"Jupliq: {row['Jupliq']}\n"
                f"Topar: {row['Topar']}\n"
                f"Pan: {row['Pan']}\n"
                f"Oqitiwshi: {row['Oqitiwshi']}\n"
                f"Kabinet: {row['Kabinet']}\n"
                "----------"
            )
        await query.message.reply_text("\n".join(response))
        logger.info(f"User {user_id} requested schedule: {query.data}")
    except Exception as e:
        logger.error(f"Error generating schedule: {str(e)}\n{traceback.format_exc()}")
        await query.message.reply_text("Kesteni kóriwde qáte ketti!")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Removed temporary file: {file_path}")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.debug(f"Received document from user {user_id}: {update.message.document.file_name}")
    if user_id not in ALLOWED_USERS:
        await update.message.reply_text("Sizge ruxsat joq!")
        logger.warning(f"Unauthorized access by user {user_id}")
        return
    
    document = update.message.document
    if not document.file_name.endswith((".xlsx", ".xls")):
        await update.message.reply_text("Tek xlsx yamasa xls fayllar qabillaytuǵın!")
        logger.warning(f"User {user_id} uploaded non-Excel file: {document.file_name}")
        return
    
    file_name = document.file_name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = os.path.join(DATA_DIR, f"Sabaq_keste_DELL_{timestamp}.xlsx")
    temp_file_path = os.path.join(DATA_DIR, f"keste_bot_orig_{timestamp}.xlsx")
    
    try:
        file = await document.get_file()
        await file.download_to_drive(local_path)
        logger.info(f"File saved: {local_path}")
        
        if process_working_schedule(local_path, temp_file_path):
            upload_to_drive(local_path, file_name)
            upload_to_drive(temp_file_path, f"keste_bot_orig_{timestamp}.xlsx")
            await update.message.reply_text("Fayl sátli júklendi hám tekserip shıǵıldı!")
            logger.info(f"File processed and uploaded: {file_name}")
        else:
            await update.message.reply_text("Qátelik! Fayl óńdelmedi, mazmunı bo's yamasa qáte bar.")
            logger.error("Failed to process uploaded file")
    except Exception as e:
        logger.error(f"Error handling document: {str(e)}\n{traceback.format_exc()}")
        await update.message.reply_text("Fayl júklewde qáte ketti!")
    finally:
        for path in [local_path, temp_file_path]:
            if os.path.exists(path):
                os.remove(path)
                logger.info(f"Removed temporary file: {path}")

# FastAPI эндпоинт для webhook
@app.post("/{token}")
async def telegram_webhook(token: str, request: Request):
    logger.debug(f"Received webhook request with token: {token}")
    if token != BOT_TOKEN:
        logger.error(f"Invalid token: {token}")
        raise HTTPException(status_code=403, detail="Invalid token")
    try:
        update = await request.json()
        logger.debug(f"Webhook update: {json.dumps(update, indent=2)}")
        if application is None:
            logger.error("Application not initialized")
            raise HTTPException(status_code=500, detail="Bot not initialized")
        await application.update_queue.put(Update.de_json(update, application.bot))
        logger.debug("Update added to queue")
        return {"ok": True}
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Webhook error: {str(e)}")

# FastAPI эндпоинт для корневого URL
@app.get("/")
async def root():
    logger.debug("Received GET request to root")
    return {"message": "Telegram bot is running"}

# Проверка зависимостей
def check_dependencies():
    try:
        import telegram
        import fastapi
        import uvicorn
        logger.info(f"Dependencies: telegram={telegram.__version__}, fastapi={fastapi.__version__}, uvicorn={uvicorn.__version__}")
    except ImportError as e:
        logger.error(f"Missing dependency: {str(e)}")
        raise

# Инициализация и запуск
async def init_bot():
    global application
    try:
        logger.info("Checking dependencies")
        check_dependencies()
        logger.info("Initializing Telegram Application")
        application = Application.builder().token(BOT_TOKEN).build()
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(button_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
        application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
        await application.initialize()
        await application.start()
        logger.info("Bot initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing bot: {str(e)}\n{traceback.format_exc()}")
        raise

if __name__ == "__main__":
    import uvicorn
    import asyncio
    
    async def main():
        try:
            logger.info("Starting application")
            await init_bot()
            logger.info("Running Uvicorn server")
            uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
        except Exception as e:
            logger.error(f"Error starting application: {str(e)}\n{traceback.format_exc()}")
            raise
    
    asyncio.run(main())
