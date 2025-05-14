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

    async def start(self):
        await self._init_clients()
        logger.info(f"Бот запущен и готов к работе! Всего аккаунтов: {len(self.accounts)}")

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

    def get_next_client(self):
        import time
        start_idx = self.current_client_index
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
            import asyncio
            asyncio.get_event_loop().run_until_complete(asyncio.sleep(min_wait))
        # После ожидания пробуем снова
        return self.get_next_client()

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
            if cached_data:
                logger.info(f"Используем кэшированные данные для {chat_id}")
                return cached_data['data']
            await asyncio.sleep(10)
            client, idx = self.get_next_client()
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
            logger.warning(f"FloodWait: аккаунт {self.accounts[self.current_client_index-1]['session']} заморожен на {wait_time} секунд")
            self.floodwait_until[self.current_client_index-1] = time.time() + wait_time
            return await self.get_chat_info(chat_id)
        except Exception as e:
            logger.error(f"Ошибка при получении информации о чате {chat_id}: {e}")
            return None

    async def analyze_dau(self, chat_id, hours=24):
        import time
        try:
            client, idx = self.get_next_client()
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
            logger.warning(f"FloodWait: аккаунт {self.accounts[self.current_client_index-1]['session']} заморожен на {wait_time} секунд")
            self.floodwait_until[self.current_client_index-1] = time.time() + wait_time
            return await self.analyze_dau(chat_id, hours)
        except Exception as e:
            print(f"Ошибка при анализе DAU: {e}")
            return None

    async def analyze_dau_monthly(self, chat_id, days=30):
        import time
        try:
            client, idx = self.get_next_client()
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
            logger.warning(f"FloodWait: аккаунт {self.accounts[self.current_client_index-1]['session']} заморожен на {wait_time} секунд")
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

async def main():
    import os
    print('--- ПРОВЕРКА .env ---')
    print('Файл .env существует:', os.path.exists('.env'))
    if os.path.exists('.env'):
        with open('.env', 'r', encoding='utf-8') as f:
            print('Содержимое .env:')
            print(f.read())
    print('--- ВСЕ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ, которые видит Python ---')
    for k, v in os.environ.items():
        if 'API' in k or 'HASH' in k:
            print(f'{k}={v}')
    print('--- КОНЕЦ ПРОВЕРКИ ---')
    print('API_ID_1:', os.getenv('API_ID_1'))
    print('API_HASH_1:', os.getenv('API_HASH_1'))
    print('API_ID_2:', os.getenv('API_ID_2'))
    print('API_HASH_2:', os.getenv('API_HASH_2'))
    print('API_ID_3:', os.getenv('API_ID_3'))
    print('API_HASH_3:', os.getenv('API_HASH_3'))
    analyzer = TelegramAnalyzer()
    await analyzer.start()
    
    # Список чатов для анализа (задано пользователем)
    chat_ids = [
        '@WebDevelRu',
        '@vosh2021_otveti',
        '@vopros_traf',
        '@virtualliteracy01',
        '@vidchat',
        '@verbalkinks',
        '@Uchitelya_shcoli2',
        '@uc1_1C',
        '@u4ebnik',
        '@typ_math_chat',
        '@tusxfiles',
        '@tulahack',
        '@TRKI3C1',
    ]
    logger.info(f'Будет проанализировано {len(chat_ids)} чатов.')
    
    def is_valid_username(username):
        return bool(username) and username != '' and username != ' ' and username != 'https://t.me/'

    CHUNK_SIZE = 5
    chat_chunks = [chat_ids[i:i + CHUNK_SIZE] for i in range(0, len(chat_ids), CHUNK_SIZE)]
    
    results = []
    try:
        for chunk_index, chunk in enumerate(chat_chunks):
            logger.info(f'Обрабатываю группу {chunk_index + 1} из {len(chat_chunks)}')
            
            for chat_id in chunk:
                username = extract_username(chat_id)
                if not is_valid_username(username):
                    logger.warning(f'Пропускаю некорректную ссылку: {chat_id}')
                    continue
                
                # Логируем аккаунт, который будет использоваться для анализа
                account_idx = analyzer.current_client_index
                account = analyzer.accounts[account_idx]
                logger.info(f'Анализирую: {chat_id} аккаунтом {account["session"]} (api_id={account["api_id"]})...')
                try:
                    cached_data = analyzer.cache.get(username)
                    if cached_data:
                        logger.info(f'Используем кэшированные данные для {username}')
                        chat_info = cached_data.get('chat_info', {})
                        dau_info = cached_data.get('dau_info', {})
                        dau_month_info = cached_data.get('dau_month_info', {})
                        cached_at = cached_data.get('cached_at', 'неизвестно')
                    else:
                        chat_info = await analyzer.get_chat_info(username)
                        if not chat_info:
                            logger.warning(f'Не удалось получить информацию о чате {chat_id}')
                            continue
                        dau_info = await analyzer.analyze_dau(username)
                        dau_month_info = await analyzer.analyze_dau_monthly(username)
                        cached_at = datetime.now().isoformat()
                        analyzer.cache.set(username, {
                            'chat_info': chat_info,
                            'dau_info': dau_info,
                            'dau_month_info': dau_month_info,
                            'cached_at': cached_at
                        })
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
                    logger.error(f"Произошла ошибка: {e}")
                    save_partial_report(results)
                    break

                members_count = chat_info.get('members_count') if chat_info else None
                if members_count:
                    dau_percent = round((dau_info.get('unique_senders', 0) / members_count * 100), 2) if dau_info else None
                    avg_dau_percent = round((dau_month_info.get('avg_dau', 0) / members_count * 100), 2) if dau_month_info and dau_month_info.get('avg_dau') and members_count else None
                else:
                    dau_percent = None
                    avg_dau_percent = None

                resume = 'нет данных'
                days_with_messages = dau_month_info.get('days_with_messages', 0) if dau_month_info else 0
                days_in_month = 30
                if members_count and dau_month_info and dau_month_info.get('avg_dau') is not None and avg_dau_percent is not None:
                    if members_count >= 1000:
                        if (dau_month_info.get('avg_dau', 0) < 5 and avg_dau_percent < 0.1) or days_with_messages < days_in_month * 0.3:
                            resume = 'Мертвый чат'
                        elif avg_dau_percent > 10:
                            resume = 'Флудилка'
                        else:
                            resume = 'Живой чат'
                    elif members_count >= 100:
                        if (dau_month_info.get('avg_dau', 0) < 1 and avg_dau_percent < 0.1) or days_with_messages < days_in_month * 0.3:
                            resume = 'Мертвый чат'
                        elif avg_dau_percent > 15:
                            resume = 'Флудилка'
                        elif dau_month_info.get('avg_dau', 0) >= 1:
                            resume = 'Живой чат (нишевой/объявлений)'
                        else:
                            resume = 'Живой чат'
                    else:
                        if dau_month_info.get('avg_dau', 0) < 1 or days_with_messages < days_in_month * 0.3:
                            resume = 'Мертвый чат'
                        elif avg_dau_percent > 20:
                            resume = 'Флудилка'
                        else:
                            resume = 'Живой чат'

                results.append({
                    'Канал/чат': f't.me/{username}',
                    'Название': chat_info.get('title', ''),
                    'Описание': chat_info.get('description', ''),
                    'Подписчиков': members_count,
                    'DAU': dau_info.get('unique_senders', 0) if dau_info else 0,
                    'DAU %': dau_percent if dau_percent is not None else 0,
                    'DAU (месяц, среднее)': dau_month_info.get('avg_dau', 0) if dau_month_info else 0,
                    'DAU % (месяц, среднее)': avg_dau_percent if avg_dau_percent is not None else 0,
                    'Дней с сообщениями (30д)': days_with_messages,
                    'Всего сообщений (24ч)': dau_info.get('total_messages', 0) if dau_info else 0,
                    'Резюме': resume,
                    'Дата кэша': cached_at,
                    'Аккаунт': chat_info.get('account_used', 'неизвестно')
                })

            # После каждой группы — переподключение для смены IP
            await analyzer.restart_with_new_ip()
            logger.info(f'IP успешно сменён после группы {chunk_index + 1}')
            # Пауза между группами
            if chunk_index < len(chat_chunks) - 1:
                group_pause = random.uniform(60, 90)  # 60-90 секунд между группами
                logger.info(f'Завершена группа {chunk_index + 1}. Пауза {group_pause:.1f} секунд перед следующей группой...')
                await asyncio.sleep(group_pause)

        # Сохраняем результаты в Excel
        save_report(results)
        logger.info('Анализ завершен!')
    except Exception as e:
        logger.error(f'Произошла ошибка: {str(e)}')
        save_partial_report(results)
    finally:
        for client in analyzer.clients:
            await client.disconnect()

    # Добавляем проверку авторизации для всех клиентов
    for idx, client in enumerate(analyzer.clients):
        is_auth = await client.is_user_authorized()
        if is_auth:
            me = await client.get_me()
            print(f"Аккаунт {analyzer.accounts[idx]['session']} авторизован: {is_auth}, user_id: {me.id}, username: @{me.username}, имя: {me.first_name} {me.last_name}")
        else:
            print(f"Аккаунт {analyzer.accounts[idx]['session']} авторизован: {is_auth}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа прервана пользователем")
        # Сохраняем результаты за последние 24 часа
        save_last_24h_results(results)
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
        # Сохраняем результаты за последние 24 часа
        save_last_24h_results(results) 
    