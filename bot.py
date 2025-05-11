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
load_dotenv()

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
        self.cache[chat_id] = {
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        self._save_cache()

class TelegramAnalyzer:
    def __init__(self):
        self.client = TelegramClient(
            'telegram_analyzer',
            os.getenv('API_ID'),
            os.getenv('API_HASH')
        )
        self.cache = Cache()
        self.rate_limit = 30  # запросов в секунду
        self.last_request_time = {}
        
    async def start(self):
        await self.client.start()
        logger.info("Бот запущен и готов к работе!")

    async def get_chat_info(self, chat_id):
        """Получение базовой информации о чате"""
        try:
            # Проверяем кэш
            cached_data = self.cache.get(chat_id)
            if cached_data:
                logger.info(f"Используем кэшированные данные для {chat_id}")
                return cached_data['data']

            chat = await self.client.get_entity(chat_id)
            if isinstance(chat, Channel):
                full_chat = await self.client(GetFullChannelRequest(chat))
                result = {
                    'title': chat.title,
                    'description': full_chat.full_chat.about,
                    'members_count': getattr(full_chat.full_chat, 'participants_count', None),
                    'is_public': not chat.megagroup,
                    'date_created': chat.date.isoformat(),
                    'last_activity': datetime.now(timezone.utc).isoformat()
                }
                # Сохраняем в кэш
                self.cache.set(chat_id, result)
                return result
        except Exception as e:
            logger.error(f"Ошибка при получении информации о чате {chat_id}: {e}")
            return None

    async def analyze_dau(self, chat_id, hours=24):
        """Анализ DAU за последние N часов"""
        try:
            messages = []
            async for message in self.client.iter_messages(chat_id, limit=100):
                if message.date > datetime.now(timezone.utc) - timedelta(hours=hours):
                    messages.append(message)
                else:
                    break
            
            # Подсчет уникальных отправителей
            unique_senders = set()
            for msg in messages:
                if hasattr(msg.from_id, 'user_id'):
                    unique_senders.add(msg.from_id.user_id)
            
            return {
                'total_messages': len(messages),
                'unique_senders': len(unique_senders),
                'time_period': f'Последние {hours} часов'
            }
        except Exception as e:
            print(f"Ошибка при анализе DAU: {e}")
            return None

    async def analyze_dau_monthly(self, chat_id, days=30):
        """Анализ среднего DAU за последние N дней (по дням) и регулярности сообщений"""
        try:
            from collections import defaultdict
            messages_by_day = defaultdict(set)
            days_with_messages = set()
            async for message in self.client.iter_messages(chat_id, limit=3000):
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
                'days_with_messages': len(days_with_messages)
            }
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
    import pandas as pd
    from datetime import datetime
    if not results:
        print('Нет данных для сохранения.')
        return
    df = pd.DataFrame(results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'partial_report_{timestamp}.xlsx'
    df.to_excel(filename, index=False)
    print(f'Промежуточные результаты сохранены в файле {filename}')

async def main():
    analyzer = TelegramAnalyzer()
    await analyzer.start()
    
    logger.info('Введите список username или ссылок на каналы/чаты (по одному в строке). Для завершения введите пустую строку:')
    chat_ids = []
    while True:
        chat_id = input().strip()
        if not chat_id:
            break
        chat_ids.append(chat_id)
    
    # Фильтруем пустые и некорректные ссылки
    def is_valid_username(username):
        return bool(username) and username != '' and username != ' ' and username != 'https://t.me/'

    results = []
    try:
        for chat_id in chat_ids:
            username = extract_username(chat_id)
            if not is_valid_username(username):
                logger.warning(f'Пропускаю некорректную ссылку: {chat_id}')
                continue
            
            logger.info(f'Анализирую: {chat_id}...')
            try:
                chat_info = await analyzer.get_chat_info(username)
                if not chat_info:
                    logger.warning(f'Не удалось получить информацию о чате {chat_id}')
                    continue

                dau_info = await analyzer.analyze_dau(username)
                dau_month_info = await analyzer.analyze_dau_monthly(username)

                # Случайная пауза 15-30 секунд между запросами
                pause_time = random.uniform(15, 30)
                logger.info(f'Делаю паузу на {pause_time:.1f} секунд...')
                await asyncio.sleep(pause_time)

            except FloodWaitError as e:
                logger.error(f'Обнаружено ограничение Telegram: необходимо подождать {e.seconds} секунд')
                logger.info('Сохраняю промежуточные результаты и завершаю анализ.')
                save_partial_report(results)
                sys.exit(0)
            except (ChatAdminRequiredError, ChannelPrivateError) as e:
                logger.error(f'Нет доступа к чату {chat_id}: {str(e)}')
                continue
            except Exception as e:
                logger.error(f'Ошибка при анализе чата {chat_id}: {str(e)}')
                continue

            members_count = chat_info.get('members_count') if chat_info else None
            if members_count:
                dau_percent = round((dau_info['unique_senders'] / members_count * 100), 2) if dau_info else None
                avg_dau_percent = round((dau_month_info['avg_dau'] / members_count * 100), 2) if dau_month_info and dau_month_info['avg_dau'] and members_count else None
            else:
                dau_percent = None
                avg_dau_percent = None
            # Улучшенный анализ статуса чата
            resume = 'нет данных'
            days_with_messages = dau_month_info['days_with_messages'] if dau_month_info and 'days_with_messages' in dau_month_info else 0
            days_in_month = 30
            if members_count and dau_month_info and dau_month_info['avg_dau'] is not None and avg_dau_percent is not None:
                if members_count >= 1000:
                    if (dau_month_info['avg_dau'] < 5 and avg_dau_percent < 0.1) or days_with_messages < days_in_month * 0.3:
                        resume = 'Мертвый чат'
                    elif avg_dau_percent > 10:
                        resume = 'Флудилка'
                    else:
                        resume = 'Живой чат'
                elif members_count >= 100:
                    if (dau_month_info['avg_dau'] < 1 and avg_dau_percent < 0.1) or days_with_messages < days_in_month * 0.3:
                        resume = 'Мертвый чат'
                    elif avg_dau_percent > 15:
                        resume = 'Флудилка'
                    elif dau_month_info['avg_dau'] >= 1:
                        resume = 'Живой чат (нишевой/объявлений)'
                    else:
                        resume = 'Живой чат'
                else:
                    if dau_month_info['avg_dau'] < 1 or days_with_messages < days_in_month * 0.3:
                        resume = 'Мертвый чат'
                    elif avg_dau_percent > 20:
                        resume = 'Флудилка'
                    else:
                        resume = 'Живой чат'
            results.append({
                'Канал/чат': chat_id,
                'Название': chat_info['title'] if chat_info else 'Ошибка',
                'Описание': chat_info['description'] if chat_info else 'Ошибка',
                'Подписчиков': members_count if members_count is not None else 'нет данных',
                'DAU': dau_info['unique_senders'] if dau_info else 'нет данных',
                'DAU %': dau_percent if dau_percent is not None else 'нет данных',
                'DAU (месяц, среднее)': dau_month_info['avg_dau'] if dau_month_info and dau_month_info['avg_dau'] is not None else 'нет данных',
                'DAU % (месяц, среднее)': avg_dau_percent if avg_dau_percent is not None else 'нет данных',
                'Дней с сообщениями (30д)': days_with_messages,
                'Всего сообщений (24ч)': dau_info['total_messages'] if dau_info else 'нет данных',
                'Резюме': resume
            })
    except KeyboardInterrupt:
        logger.warning('Анализ прерван пользователем! Сохраняю промежуточные результаты...')
        save_partial_report(results)
        return
    except Exception as e:
        logger.error(f'Произошла непредвиденная ошибка: {str(e)}')
        logger.info('Сохраняю промежуточные результаты...')
        save_partial_report(results)
        return
    
    df = pd.DataFrame(results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'report_{timestamp}.xlsx'
    df.to_excel(filename, index=False)
    logger.info(f'Анализ завершён! Результаты сохранены в файле {filename}')

if __name__ == '__main__':
    asyncio.run(main()) 
    