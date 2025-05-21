import os
from typing import List, Dict, Any
from notion_client import Client
from dotenv import load_dotenv
import pandas as pd
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('notion_integration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

def safe_get_rich_text(properties, field):
    rich = properties.get(field, {}).get("rich_text", [])
    if rich and "text" in rich[0]:
        return rich[0]["text"].get("content", "")
    return ""

def safe_get_title(properties):
    title = properties.get("Name", {}).get("title", [])
    if title and "text" in title[0]:
        return title[0]["text"].get("content", "")
    return ""

class NotionIntegration:
    def __init__(self):
        self.notion = Client(auth=os.getenv("NOTION_TOKEN"))
        self.database_id = os.getenv("NOTION_DATABASE_ID")

    def get_chats_to_analyze(self) -> List[Dict[str, Any]]:
        """
        Получает список чатов для анализа из Notion базы данных
        """
        response = self.notion.databases.query(
            database_id=self.database_id,
            filter={
                "property": "Status",
                "select": {
                    "equals": "To Analyze"
                }
            }
        )
        return response.get("results", [])

    def update_chat_analysis(self, page_id: str, analysis_results: Dict[str, Any], status: str = "Analyzed"):
        """
        Обновляет страницу в Notion с результатами анализа и статусом
        """
        self.notion.pages.update(
            page_id=page_id,
            properties={
                "Status": {"select": {"name": status}},
                "Канал/чат": {"rich_text": [{"text": {"content": analysis_results.get("chat_id", "")}}]},
                "Название": {"title": [{"text": {"content": analysis_results.get("name", "")}}]},
                "Описание": {"rich_text": [{"text": {"content": analysis_results.get("description", "")}}]},
                "Подписчиков": {"number": analysis_results.get("members_count", 0)},
                "DAU": {"number": analysis_results.get("dau", 0)},
                "DAU %": {"number": analysis_results.get("dau_percent", 0)},
                "DAU (месяц, среднее)": {"number": analysis_results.get("monthly_avg_dau", 0)},
                "DAU % (месяц, среднее)": {"number": analysis_results.get("monthly_avg_dau_percent", 0)},
                "Дней с сообщениями (30д)": {"number": analysis_results.get("days_with_messages", 0)},
                "Всего сообщений (24ч)": {"number": analysis_results.get("total_messages", 0)},
                "Резюме": {"rich_text": [{"text": {"content": analysis_results.get("resume", "")}}]},
                "Дата кэша": {"date": {"start": analysis_results.get("cache_date", "")}},
                "Аккаунт": {"rich_text": [{"text": {"content": analysis_results.get("account", "")}}]},
                "Activity Score": {"number": analysis_results.get("activity_score", 0)},
                "Last Analysis": {"date": {"start": pd.Timestamp.now().isoformat()}},
                "Analysis Notes": {"rich_text": [{"text": {"content": analysis_results.get("notes", "")}}]}
            }
        )

    def export_to_excel(self, filename):
        """Экспорт данных из Notion в Excel"""
        try:
            # Получаем все страницы из базы данных
            pages = self.notion.databases.query(database_id=self.database_id).get("results", [])
            if not pages:
                logger.warning("Нет данных для экспорта")
                return

            # Подготавливаем данные для Excel
            data = []
            for page in pages:
                properties = page.get("properties", {})
                # Безопасное извлечение значений с проверкой на None
                last_analysis = properties.get("Last Analysis", {})
                last_analysis_date = last_analysis.get("date", {}) if last_analysis else {}
                last_analysis_start = last_analysis_date.get("start", "") if last_analysis_date else ""

                data.append({
                    "ID": page.get("id", ""),
                    "Name": properties.get("Name", {}).get("title", [{}])[0].get("text", {}).get("content", "") if properties.get("Name", {}).get("title") else "",
                    "Status": properties.get("Status", {}).get("select", {}).get("name", ""),
                    "Last Analysis": last_analysis_start,
                    "Members Count": properties.get("Members Count", {}).get("number", 0),
                    "DAU": properties.get("DAU", {}).get("number", 0),
                    "DAU %": properties.get("DAU %", {}).get("number", 0),
                    "Monthly Avg DAU": properties.get("Monthly Avg DAU", {}).get("number", 0),
                    "Monthly Avg DAU %": properties.get("Monthly Avg DAU %", {}).get("number", 0),
                    "Days With Messages": properties.get("Days With Messages", {}).get("number", 0),
                    "Total Messages": properties.get("Total Messages", {}).get("number", 0),
                    "Resume": properties.get("Resume", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "") if properties.get("Resume", {}).get("rich_text") else "",
                    "Cache Date": properties.get("Cache Date", {}).get("date", {}).get("start", "") if properties.get("Cache Date", {}).get("date") else "",
                    "Account": properties.get("Account", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "") if properties.get("Account", {}).get("rich_text") else "",
                    "Activity Score": properties.get("Activity Score", {}).get("number", 0),
                    "Notes": properties.get("Notes", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "") if properties.get("Notes", {}).get("rich_text") else ""
                })

            # Создаем DataFrame и сохраняем в Excel
            df = pd.DataFrame(data)
            df.to_excel(filename, index=False)
            logger.info(f"Данные успешно экспортированы в {filename}")
        except Exception as e:
            logger.error(f"Ошибка при экспорте данных: {e}")

    def get_chat_metrics(self, chat_id):
        """
        Получает метрики чата по chat_id из базы данных Notion.
        Поддерживает разные форматы ссылок:
        - https://t.me/chat_id
        - t.me/chat_id
        - @chat_id
        - chat_id
        """
        # Нормализуем chat_id
        if chat_id.startswith('https://t.me/'):
            chat_id = chat_id[13:]
        elif chat_id.startswith('t.me/'):
            chat_id = chat_id[5:]
        elif chat_id.startswith('@'):
            chat_id = chat_id[1:]
            
        # Формируем варианты поиска
        search_variants = [
            chat_id,
            f"https://t.me/{chat_id}",
            f"t.me/{chat_id}",
            f"@{chat_id}"
        ]
        
        # Пробуем найти чат по каждому варианту
        for variant in search_variants:
            query = {
                "database_id": self.database_id,
                "filter": {
                    "property": "Канал/чат",
                    "rich_text": {"equals": variant}
                }
            }
            response = self.notion.databases.query(**query)
            results = response.get("results", [])
            if results:
                page = results[0]
                props = page["properties"]
                return {
                    "page_id": page["id"],
                    "members_count": props.get("Подписчиков", {}).get("number", 0),
                    "dau": props.get("DAU", {}).get("number", 0),
                    "dau_percent": props.get("DAU %", {}).get("number", 0),
                    "dau_month_avg_percent": props.get("DAU % (месяц, среднее)", {}).get("number", 0),
                    "dau_month_avg": props.get("DAU (месяц, среднее)", {}).get("number", 0),
                    "active_days": props.get("Дней с сообщениями (30д)", {}).get("number", 0),
                    "messages_per_day": props.get("Всего сообщений (24ч)", {}).get("number", 0),
                }
        
        return None

    def get_all_chats_with_pagination(self):
        """
        Получает все чаты из базы Notion с поддержкой пагинации
        """
        all_results = []
        next_cursor = None
        while True:
            kwargs = {"database_id": self.database_id}
            if next_cursor:
                kwargs["start_cursor"] = next_cursor
            response = self.notion.databases.query(**kwargs)
            all_results.extend(response.get("results", []))
            if response.get("has_more"):
                next_cursor = response.get("next_cursor")
            else:
                break
        return all_results

def get_analyzed_chats():
    notion = NotionIntegration()
    response = notion.notion.databases.query(
        database_id=notion.database_id,
        filter={
            "property": "Status",
            "select": {
                "equals": "Analyzed"
            }
        }
    )
    results = response.get("results", [])
    print(f"Найдено {len(results)} чатов со статусом 'Analyzed':")
    for page in results:
        properties = page.get("properties", {})
        name = properties.get("Name", {}).get("title", [{}])[0].get("text", {}).get("content", "")
        chat_id = properties.get("Канал/чат", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "")
        print(f"Название: {name}, ID чата: {chat_id}")

if __name__ == "__main__":
    get_analyzed_chats() 