import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import time
import logging
from collections import defaultdict
import asyncio
from telegram import Bot, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.error import TelegramError, NetworkError
from datetime import datetime, timezone
import functools
import signal
import threading

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class FirestoreMonitor:
    def __init__(self, bot_token, main_collection='users', sub_collection='requests',
                 user_fields=['display_name', 'created_time', 'email', 'phone_number'],
                 request_fields=['created_at', 'status', 'type', 'payment']):
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)

        self.db = firestore.client()
        self.main_collection = main_collection
        self.sub_collection = sub_collection
        self.user_fields = user_fields
        self.request_fields = request_fields
        self.users_listener = None
        self.requests_listener = None
        self.users_data = defaultdict(lambda: {"user_info": {}, "requests": {}})
        self.processed_users = set()
        self.processed_requests = set()

        # Telegram bot initialization
        self.bot = Bot(token=bot_token)
        self.active_chats = set()
        self.message_queue = asyncio.Queue()

        # Set the start date to today at 00:00:00 UTC
        self.start_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        self.is_running = True
        self.tasks = []
        self.loop = None

    async def send_message_to_telegram(self, message):
        if not self.active_chats:
            logging.warning("No active chats to send messages to.")
            return

        for chat_id in self.active_chats:
            try:
                await self.bot.send_message(chat_id=chat_id, text=message)
                logging.info(f"Message sent to chat {chat_id}: {message}")
            except TelegramError as e:
                logging.error(f"Failed to send message to Telegram chat {chat_id}: {e}")

    async def message_sender(self):
        logging.info("Message sender started")
        while self.is_running:
            try:
                message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                logging.debug(f"Got message from queue: {message}")
                await self.send_message_to_telegram(message)
                await asyncio.sleep(1)  # To avoid hitting Telegram's rate limits
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logging.error(f"Error in message_sender: {e}")
        logging.info("Message sender stopped")

    def preload_data(self):
        # Load all users first
        users_ref = self.db.collection(self.main_collection)
        users_query = users_ref.where(filter=firestore.FieldFilter("created_time", ">=", self.start_date))
        for user in users_query.get():
            self.update_user_data(user, is_preload=True)

        # Then load all requests
        requests_ref = self.db.collection_group(self.sub_collection)
        requests_query = requests_ref.where(filter=firestore.FieldFilter("created_at", ">=", self.start_date))
        for request in requests_query.get():
            self.update_request_data(request, is_preload=True)

    def on_users_snapshot(self, col_snapshot, changes, read_time):
        for change in changes:
            if change.type.name in ['ADDED', 'MODIFIED']:
                self.update_user_data(change.document, is_preload=False)

    def on_requests_snapshot(self, col_snapshot, changes, read_time):
        for change in changes:
            if change.type.name in ['ADDED', 'MODIFIED']:
                self.update_request_data(change.document, is_preload=False)

    def update_user_data(self, doc, is_preload=False):
        user_id = doc.id
        user_data = doc.to_dict()
        created_time = user_data.get('created_time')

        if isinstance(created_time, datetime):
            if created_time < self.start_date:
                return

        if user_id in self.processed_users and is_preload:
            return

        self.users_data[user_id]["user_info"] = {field: user_data.get(field, "N/A") for field in self.user_fields}
        self.processed_users.add(user_id)

        if not is_preload:
            self.print_user(user_id)

    def update_request_data(self, doc, is_preload=False):
        request_id = doc.id
        user_id = doc.reference.parent.parent.id
        request_data = doc.to_dict()
        created_at = request_data.get('created_at')

        if isinstance(created_at, datetime):
            if created_at < self.start_date:
                return

        request_unique_id = f"{user_id}_{request_id}"

        if request_unique_id in self.processed_requests and is_preload:
            return

        self.users_data[user_id]["requests"][request_id] = {field: request_data.get(field, "N/A") for field in self.request_fields}
        self.processed_requests.add(request_unique_id)

        # Ensure we have user data before printing the request
        if user_id not in self.users_data or not self.users_data[user_id]["user_info"]:
            user_doc = self.db.collection(self.main_collection).document(user_id).get()
            if user_doc.exists:
                self.update_user_data(user_doc, is_preload=True)
            else:
                logging.warning(f"User data not found for user_id: {user_id}")

        if not is_preload:
            self.print_request(user_id, request_id)

    def format_user_message(self, user_info):
        return " | ".join([f"{k}: {v}" for k, v in user_info.items() if k != 'display_name'])

    def format_request_message(self, request_data, user_info):
        request_str = " | ".join([f"{k}: {v}" for k, v in request_data.items()])
        phone_number = user_info.get('phone_number', 'N/A')
        return f"{request_str} | phone_number: {phone_number}"

    def print_user(self, user_id):
        user_info = self.users_data[user_id]["user_info"]
        display_name = user_info.get('display_name', 'Unknown')
        user_str = self.format_user_message(user_info)
        message = f"New User {display_name}: {user_str}"
        logging.info(f"New user detected: {message}")
        if self.loop and self.is_running:
            self.loop.call_soon_threadsafe(self.message_queue.put_nowait, message)
            logging.debug(f"User message added to queue: {message}")

    def print_request(self, user_id, request_id):
        user_info = self.users_data[user_id]["user_info"]
        display_name = user_info.get('display_name', 'Unknown')
        request_data = self.users_data[user_id]["requests"][request_id]
        request_str = self.format_request_message(request_data, user_info)
        message = f"New Request for {display_name}: {request_str}"
        logging.info(f"New request detected: {message}")
        if self.loop and self.is_running:
            self.loop.call_soon_threadsafe(self.message_queue.put_nowait, message)
            logging.debug(f"Request message added to queue: {message}")

    async def start_monitoring(self):
        self.loop = asyncio.get_running_loop()
        self.preload_data()

        users_ref = self.db.collection(self.main_collection)
        self.users_listener = users_ref.on_snapshot(self.on_users_snapshot)

        requests_ref = self.db.collection_group(self.sub_collection)
        self.requests_listener = requests_ref.on_snapshot(self.on_requests_snapshot)

        start_message = (
            f"Начат мониторинг новых пользователей в '{self.main_collection}' и запросов в '{self.sub_collection}'\n"
            f"Отслеживаются данные начиная с {self.start_date.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
            f"Отслеживаемые поля пользователей: {', '.join(self.user_fields)}\n"
            f"Отслеживаемые поля запросов: {', '.join(self.request_fields)}\n"
            f"{'-' * 50}"
        )
        logging.info(start_message)
        await self.send_message_to_telegram(start_message)

        # Start the message sender task
        self.tasks.append(asyncio.create_task(self.message_sender()))

    async def stop_monitoring(self):
        self.is_running = False
        if self.users_listener:
            self.users_listener.unsubscribe()
        if self.requests_listener:
            self.requests_listener.unsubscribe()
        self.users_listener = None
        self.requests_listener = None

        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

        logging.info("Мониторинг остановлен")
        await self.send_message_to_telegram("Мониторинг остановлен")

    def add_chat(self, chat_id):
        self.active_chats.add(chat_id)
        logging.info(f"Added chat {chat_id} to active chats")

    def remove_chat(self, chat_id):
        self.active_chats.discard(chat_id)
        logging.info(f"Removed chat {chat_id} from active chats")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    monitor.add_chat(chat_id)
    await context.bot.send_message(chat_id=chat_id, text="Мониторинг начат. Вы будете получать уведомления.")
    logging.info(f"Start command received from chat {chat_id}")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    monitor.remove_chat(chat_id)
    await context.bot.send_message(chat_id=chat_id, text="Мониторинг остановлен. Вы больше не будете получать уведомления.")
    logging.info(f"Stop command received from chat {chat_id}")

async def main():
    # Укажите нужные поля для пользователей и запросов
    user_fields = ['display_name', 'created_time', 'email_contact', 'phone_number']
    request_fields = ['created_at', 'status', 'type', 'payment', 'address']

    # Замените на свой токен
    bot_token = 'Вставляете токен сюда!!!'

    global monitor
    monitor = FirestoreMonitor(bot_token, user_fields=user_fields, request_fields=request_fields)

    application = ApplicationBuilder().token(bot_token).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop))

    await monitor.start_monitoring()

    async with application:
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)

        try:
            # Run the event loop
            while monitor.is_running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logging.info("Получен сигнал прерывания. Завершение работы...")
        finally:
            monitor.is_running = False
            await monitor.stop_monitoring()
            await application.stop()
            await application.updater.stop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Программа завершена пользователем")
    except Exception as e:
        logging.error(f"Необработанное исключение: {e}")
    finally:
        logging.info("Программа завершена")
