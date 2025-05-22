import os
import asyncio
import random
import json
import logging
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import Channel
from telethon.errors import FloodWaitError, ChatAdminRequiredError, ChannelPrivateError
import pandas as pd
from dotenv import load_dotenv
import re
import sys
from proxy_pool import ProxyPool
from proxies import PROXIES
import glob
import subprocess
from notion_integration import NotionIntegration
from evaluate_chat import evaluate_chat

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv(dotenv_path='.env')

# Глобальная переменная для хранения результатов анализа
results = []
# Счётчик для контроля общего количества анализов за сессию
analyzed_chats_total = 0
MAX_CHATS_BEFORE_PAUSE = 250
PAUSE_SECONDS = 600  # 10 минут

class Cache:
    def __init__(self, cache_file='chat_cache.json'):
        self.cache_file = cache_file
        self.cache = self._load_cache()

    def _load_cache(self):
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.error(f"Ошибка при загрузке кэша: {e}")
            return {}

    def _save_cache(self):
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка при сохранении кэша: {e}")

    def get(self, chat_id):
        return self.cache.get(chat_id)

    def set(self, chat_id, data):
        self.cache[chat_id] = data
        self._save_cache()

class TelegramAnalyzer:
    def __init__(self):
        self.proxy_pool = ProxyPool(PROXIES)
        self.accounts = []
        i = 1
        while True:
            api_id = os.getenv(f'API_ID_{i}')
            api_hash = os.getenv(f'API_HASH_{i}')
            if api_id and api_hash:
                self.accounts.append({
                    "api_id": api_id,
                    "api_hash": api_hash,
                    "session": f'telegram_analyzer_{i}'
                })
                i += 1
            else:
                break
        self.clients = []
        self.current_client_index = 0
        self.cache = Cache()
        self.rate_limit = 30  # запросов в секунду
        self.last_request_time = {}
        self.floodwait_until = [0] * len(self.accounts)  # timestamp до которого аккаунт "заморожен"
        self.notion = NotionIntegration()

    async def start(self):
        await self._init_clients()
        logger.info(f"Бот запущен и готов к работе! Всего аккаунтов: {len(self.accounts)}")
        print("Бот слушает команды...")

    async def _init_clients(self):
        for idx, account in enumerate(self.accounts):
            proxy = self.proxy_pool.get_proxy()
            session_path = os.path.abspath(f'telegram_analyzer_{idx+1}.session')
            client = TelegramClient(
                session_path,
                account["api_id"],
                account["api_hash"],
                proxy=proxy
            )
            try:
                await client.start()
            except Exception as e:
                logger.error(f"Ошибка при инициализации клиента {account['session']}: {e}")
                if 'database is locked' in str(e):
                    logger.error(f"Session-файл {session_path} заблокирован! Попробуйте перезапустить компьютер и убедитесь, что нет других процессов Python.")
                raise
            self.clients.append(client)
            logger.info(f"Клиент {account['session']} (api_id={account['api_id']}) успешно инициализирован")

    async def get_next_client(self):
        import time
        now = time.time()
        for _ in range(len(self.clients)):
            idx = self.current_client_index
            if self.floodwait_until[idx] <= now:
                client = self.clients[idx]
                account = self.accounts[idx]
                logger.info(f"Следующий аккаунт для анализа: {account['session']} (api_id={account['api_id']})")
                self.current_client_index = (self.current_client_index + 1) % len(self.clients)
                return client, idx
            self.current_client_index = (self.current_client_index + 1) % len(self.clients)
        # Если все аккаунты в FloodWait — ждём минимальное время
        min_wait = min(self.floodwait_until) - now
        logger.warning(f"Все аккаунты в FloodWait. Жду {int(min_wait)} секунд...")
        if min_wait > 0:
            await asyncio.sleep(min_wait)
        # После ожидания пробуем снова
        return await self.get_next_client()

    async def restart_with_new_ip(self):
        """Переподключение всех клиентов с новыми IP"""
        for i, client in enumerate(self.clients):
            await client.disconnect()
            await asyncio.sleep(5)  # небольшая пауза для смены IP
            proxy = self.proxy_pool.get_proxy()
            # Важно: пересоздаём клиента только после disconnect
            self.clients[i] = TelegramClient(
                self.accounts[i]["session"],
                self.accounts[i]["api_id"],
                self.accounts[i]["api_hash"],
                proxy=proxy
            )
            await self.clients[i].start()
            logger.info(f"Переподключение: получен новый IP через прокси для клиента {self.accounts[i]['session']}!")

    async def get_chat_info(self, chat_id):
        import time
        try:
            cached_data = self.cache.get(chat_id)
            if cached_data and 'data' in cached_data:
                logger.info(f"Используем кэшированные данные для {chat_id}")
                return cached_data['data']
            else:
                logger.info(f"Кэш для {chat_id} не содержит 'data', игнорируем кэш.")
            await asyncio.sleep(10)
            client, idx = await self.get_next_client()
            chat = await client.get_entity(chat_id)
            if isinstance(chat, Channel):
                await asyncio.sleep(5)
                full_chat = await client(GetFullChannelRequest(chat))
                result = {
                    'title': chat.title,
                    'description': full_chat.full_chat.about,
                    'members_count': getattr(full_chat.full_chat, 'participants_count', None),
                    'is_public': not chat.megagroup,
                    'date_created': chat.date.isoformat(),
                    'last_activity': datetime.now(timezone.utc).isoformat(),
                    'account_used': self.accounts[idx]["session"]
                }
                self.cache.set(chat_id, result)
                return result
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"⚠️ FloodWait: аккаунт {self.accounts[self.current_client_index-1]['session']} заморожен на {wait_time} секунд (чат {chat_id})")
            self.floodwait_until[self.current_client_index-1] = time.time() + wait_time
            return await self.get_chat_info(chat_id)
        except Exception as e:
            logger.error(f"Ошибка при получении информации о чате {chat_id}: {e}")
            return None

    async def analyze_dau(self, chat_id, hours=24):
        import time
        try:
            client, idx = await self.get_next_client()
            messages = []
            async for message in client.iter_messages(chat_id, limit=100):
                if message.date > datetime.now(timezone.utc) - timedelta(hours=hours):
                    messages.append(message)
                else:
                    break
            unique_senders = set()
            for msg in messages:
                if hasattr(msg.from_id, 'user_id'):
                    unique_senders.add(msg.from_id.user_id)
            return {
                'total_messages': len(messages),
                'unique_senders': len(unique_senders),
                'time_period': f'Последние {hours} часов',
                'account_used': self.accounts[idx]["session"]
            }
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"⚠️ FloodWait: аккаунт {self.accounts[self.current_client_index-1]['session']} заморожен на {wait_time} секунд")
            self.floodwait_until[self.current_client_index-1] = time.time() + wait_time
            return await self.analyze_dau(chat_id, hours)
        except Exception as e:
            print(f"Ошибка при анализе DAU: {e}")
            return None

    async def analyze_dau_monthly(self, chat_id, days=30):
        import time
        try:
            client, idx = await self.get_next_client()
            from collections import defaultdict
            messages_by_day = defaultdict(set)
            days_with_messages = set()
            async for message in client.iter_messages(chat_id, limit=3000):
                if message.date > datetime.now(timezone.utc) - timedelta(days=days):
                    day = message.date.date()
                    days_with_messages.add(day)
                    if hasattr(message.from_id, 'user_id'):
                        messages_by_day[day].add(message.from_id.user_id)
                else:
                    break
            if not messages_by_day:
                return {'avg_dau': None, 'avg_dau_percent': None, 'days_with_messages': 0}
            daily_counts = [len(users) for users in messages_by_day.values()]
            avg_dau = round(sum(daily_counts) / len(daily_counts), 2)
            return {
                'avg_dau': avg_dau,
                'days_counted': len(daily_counts),
                'days_with_messages': len(days_with_messages),
                'account_used': self.accounts[idx]["session"]
            }
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"⚠️ FloodWait: аккаунт {self.accounts[self.current_client_index-1]['session']} заморожен на {wait_time} секунд")
            self.floodwait_until[self.current_client_index-1] = time.time() + wait_time
            return await self.analyze_dau_monthly(chat_id, days)
        except Exception as e:
            print(f"Ошибка при анализе DAU за месяц: {e}")
            return {'avg_dau': None, 'avg_dau_percent': None, 'days_with_messages': 0}

    async def generate_report(self, chat_id):
        """Генерация полного отчета"""
        chat_info = await self.get_chat_info(chat_id)
        if not chat_info:
            return "Не удалось получить информацию о чате"
        
        dau_info = await self.analyze_dau(chat_id)
        if not dau_info:
            return "Не удалось проанализировать активность"
        
        members_count = chat_info.get('members_count') or 0
        if members_count:
            dau_percent = round((dau_info['unique_senders'] / members_count * 100), 2)
        else:
            dau_percent = 'нет данных'
        
        report = f"""
📊 Отчет по чату: {chat_info['title']}

📝 Описание: {chat_info['description']}
👥 Подписчиков: {chat_info['members_count']}
🌐 Тип: {'Публичный' if chat_info['is_public'] else 'Приватный'}
📅 Создан: {chat_info['date_created']}

📈 Активность за последние 24 часа:
   • Всего сообщений: {dau_info['total_messages']}
   • Уникальных отправителей: {dau_info['unique_senders']}
   • DAU: {dau_percent}%
"""
        return report

def extract_username(chat_id):
    chat_id = chat_id.strip()
    if chat_id.startswith('@https://t.me/'):
        return chat_id[len('@https://t.me/'):]
    if chat_id.startswith('https://t.me/'):
        return chat_id[len('https://t.me/'):]
    if chat_id.startswith('@'):
        return chat_id[1:]
    return chat_id

def save_partial_report(results):
    """Сохранение промежуточных результатов анализа"""
    if not results:
        logger.warning("Нет данных для сохранения")
        return
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'telegram_analysis_partial_{timestamp}.xlsx'
    
    try:
        df = pd.DataFrame(results)
        df.to_excel(filename, index=False)
        logger.info(f'Промежуточные результаты сохранены в файл: {filename}')
    except Exception as e:
        logger.error(f'Ошибка при сохранении промежуточных результатов: {e}')

def save_report(results):
    """Сохранение полного отчета в Excel"""
    if not results:
        print('Нет данных для сохранения.')
        return
    df = pd.DataFrame(results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'report_{timestamp}.xlsx'
    df.to_excel(filename, index=False)
    print(f'Отчет сохранен в файле {filename}')

def save_last_24h_results(results):
    """Сохранение результатов анализа за последние 24 часа"""
    if not results:
        logger.warning("Нет данных для сохранения")
        return
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'last_24h_analysis_{timestamp}.xlsx'
    
    try:
        df = pd.DataFrame(results)
        df.to_excel(filename, index=False)
        logger.info(f'Результаты анализа за последние 24 часа сохранены в файл: {filename}')
        print(f'\nРезультаты сохранены в файл: {filename}')
    except Exception as e:
        logger.error(f'Ошибка при сохранении результатов: {e}')

def get_resume(members, avg_dau, avg_dau_percent, days_with_messages, days_in_month=30):
    if members is None or pd.isna(members):
        return 'нет данных'
    try:
        members = float(members)
    except:
        return 'нет данных'
    if members >= 1000:
        if (avg_dau is not None and avg_dau < 5 and avg_dau_percent is not None and avg_dau_percent < 0.1) or (days_with_messages is not None and days_with_messages < days_in_month * 0.3):
            return 'Мертвый чат'
        elif avg_dau_percent is not None and avg_dau_percent > 10:
            return 'Флудилка'
        else:
            return 'Живой чат'
    elif members >= 100:
        if (avg_dau is not None and avg_dau < 1 and avg_dau_percent is not None and avg_dau_percent < 0.1) or (days_with_messages is not None and days_with_messages < days_in_month * 0.3):
            return 'Мертвый чат'
        elif avg_dau_percent is not None and avg_dau_percent > 15:
            return 'Флудилка'
        elif avg_dau is not None and avg_dau >= 1:
            return 'Живой чат (нишевой/объявлений)'
        else:
            return 'Живой чат'
    else:
        if avg_dau is not None and avg_dau < 1 or (days_with_messages is not None and days_with_messages < days_in_month * 0.3):
            return 'Мертвый чат'
        elif avg_dau_percent is not None and avg_dau_percent > 20:
            return 'Флудилка'
        else:
            return 'Живой чат'

async def main():
    analyzer = TelegramAnalyzer()
    await analyzer.start()
    
    try:
        # Получаем только чаты со статусом 'To Analyze' с пагинацией
        try:
            chats_to_analyze = analyzer.notion.get_chats_to_analyze_with_pagination()
            logger.info(f"[DEBUG] Всего чатов для анализа (To Analyze, с пагинацией): {len(chats_to_analyze)}")
        except Exception as e:
            logger.error(f"Ошибка при получении чатов из Notion: {e}")
            chats_to_analyze = []
        
        print(f"Готов к анализу чатов, всего в очереди: {len(chats_to_analyze)}")
        logger.info(f"Готов к анализу чатов, всего в очереди: {len(chats_to_analyze)}")
        
        for chat_page in chats_to_analyze:
            chat_properties = chat_page.get("properties", {})
            chat_id_property = chat_properties.get("Канал/чат", {})
            rich_text = chat_id_property.get("rich_text", [])
            chat_id = rich_text[0].get("text", {}).get("content", "") if rich_text else ""
            if not chat_id:
                logger.warning("[DEBUG] Пропущен чат без chat_id")
                    continue
            logger.info(f"[DEBUG] Начинаю анализ чата {chat_id}")
            print(f"[DEBUG] Анализирую чат: {chat_id}")
            
            # Получаем информацию о чате
            chat_info = await analyzer.get_chat_info(chat_id)
                        if not chat_info:
                logger.warning(f"Ошибка анализа чата {chat_id}, устанавливаю статус Error в Notion")
                error_results = {"chat_id": chat_id, "name": ""}
                logger.warning(f"Передаю в update_chat_analysis: {error_results}")
                analyzer.notion.update_chat_analysis(chat_page["id"], error_results, status="Error")
                continue
            
            # Анализируем DAU
            dau_info = await analyzer.analyze_dau(chat_id)
            monthly_dau = await analyzer.analyze_dau_monthly(chat_id)
            if not dau_info or not monthly_dau:
                logger.warning(f"Ошибка анализа DAU для чата {chat_id}, устанавливаю статус Error в Notion")
                error_results = {"chat_id": chat_id, "name": chat_info.get('title', '') if chat_info else ""}
                logger.warning(f"Передаю в update_chat_analysis: {error_results}")
                analyzer.notion.update_chat_analysis(chat_page["id"], error_results, status="Error")
                            continue
            
            # Формируем результаты анализа для всех полей
            members_count = chat_info.get("members_count", 0)
            dau = dau_info.get("unique_senders", 0) if dau_info else 0
            dau_percent = round((dau / members_count * 100), 2) if members_count and dau is not None else 0
            monthly_avg_dau = monthly_dau.get("avg_dau", 0) if monthly_dau else 0
            monthly_avg_dau_percent = round((monthly_avg_dau / members_count * 100), 2) if members_count and monthly_avg_dau is not None else 0
            days_with_messages = monthly_dau.get("days_with_messages", 0) if monthly_dau else 0
            total_messages = dau_info.get("total_messages", 0) if dau_info else 0
            resume = get_resume(members_count, monthly_avg_dau, monthly_avg_dau_percent, days_with_messages)
            cache_date = datetime.now().isoformat()
            account_used = chat_info.get("account_used", "")
            description = chat_info.get("description", "")
            name = chat_info.get("title", "")

            analysis_results = {
                "chat_id": chat_id,
                "name": name,
                "description": description,
                "members_count": members_count,
                "dau": dau,
                "dau_percent": dau_percent,
                "monthly_avg_dau": monthly_avg_dau,
                "monthly_avg_dau_percent": monthly_avg_dau_percent,
                "days_with_messages": days_with_messages,
                "total_messages": total_messages,
                "resume": resume,
                "cache_date": cache_date,
                "account": account_used,
                "activity_score": monthly_avg_dau_percent,
                "notes": f"DAU за 24 часа: {dau}\n"
                        f"DAU за месяц: {monthly_avg_dau}\n"
                        f"Процент DAU: {monthly_avg_dau_percent}%"
            }
            
            # Обновляем страницу в Notion
            analyzer.notion.update_chat_analysis(chat_page["id"], analysis_results)
            logger.info(f"[DEBUG] Метрики для {chat_id} обновлены в Notion")
            
            # ML-оценка
            try:
                print(f"Выполняю ML-оценку для {chat_id}")
                evaluate_chat(chat_id)
                print(f"ML-оценка для {chat_id} завершена успешно")
                logger.info(f"ML-оценка для {chat_id} завершена успешно")
                except Exception as e:
                print(f"Ошибка ML-оценки для {chat_id}: {e}")
                logger.error(f"Ошибка ML-оценки для {chat_id}: {e}")
            
            # Делаем паузу между анализами
            await asyncio.sleep(random.uniform(5, 10))
    
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
    finally:
        # Экспортируем результаты в Excel
        analyzer.notion.export_to_excel(f"notion_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        
        # Закрываем все клиенты
        for client in analyzer.clients:
            await client.disconnect()

if __name__ == "__main__":
        asyncio.run(main())
    