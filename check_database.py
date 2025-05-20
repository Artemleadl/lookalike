from notion_integration import NotionIntegration
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_check.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_database_structure():
    """
    Проверяет структуру базы данных Notion
    """
    try:
        notion = NotionIntegration()
        
        # Получаем информацию о базе данных
        database = notion.notion.databases.retrieve(
            database_id=notion.database_id
        )
        
        # Выводим свойства базы данных
        print("\nСтруктура базы данных:")
        for prop_name, prop_details in database.get("properties", {}).items():
            print(f"\n{prop_name}:")
            print(f"  Тип: {prop_details.get('type')}")
            print(f"  ID: {prop_details.get('id')}")
            
    except Exception as e:
        logger.error(f"Ошибка при проверке структуры базы данных: {e}")

def check_chat_dau(chat_id):
    """
    Проверяет значения DAU для конкретного чата
    """
    notion = NotionIntegration()
    response = notion.notion.databases.query(
        database_id=notion.database_id,
        filter={
            "property": "Канал/чат",
            "rich_text": {
                "equals": chat_id
            }
        }
    )
    
    if not response.get("results"):
        print(f"Чат {chat_id} не найден в базе")
        return
        
    page = response["results"][0]
    properties = page.get("properties", {})
    
    # Получаем все метрики DAU
    dau = properties.get("DAU", {}).get("number")
    dau_percent = properties.get("DAU %", {}).get("number")
    dau_month_avg = properties.get("DAU (месяц, среднее)", {}).get("number")
    dau_month_avg_percent = properties.get("DAU % (месяц, среднее)", {}).get("number")
    
    print(f"\nМетрики DAU для {chat_id}:")
    print(f"DAU: {dau}")
    print(f"DAU %: {dau_percent}")
    print(f"DAU (месяц, среднее): {dau_month_avg}")
    print(f"DAU % (месяц, среднее): {dau_month_avg_percent}")

def check_chat_metrics(chat_id):
    """
    Проверяет все важные метрики для конкретного чата
    """
    notion = NotionIntegration()
    response = notion.notion.databases.query(
        database_id=notion.database_id,
        filter={
            "property": "Канал/чат",
            "rich_text": {
                "equals": chat_id
            }
        }
    )
    
    if not response.get("results"):
        print(f"Чат {chat_id} не найден в базе")
        return
        
    page = response["results"][0]
    properties = page.get("properties", {})
    
    # Получаем все важные метрики
    metrics = {
        "Подписчиков": properties.get("Подписчиков", {}).get("number"),
        "DAU": properties.get("DAU", {}).get("number"),
        "DAU %": properties.get("DAU %", {}).get("number"),
        "DAU (месяц, среднее)": properties.get("DAU (месяц, среднее)", {}).get("number"),
        "DAU % (месяц, среднее)": properties.get("DAU % (месяц, среднее)", {}).get("number"),
        "Дней с сообщениями (30д)": properties.get("Дней с сообщениями (30д)", {}).get("number"),
        "Всего сообщений (24ч)": properties.get("Всего сообщений (24ч)", {}).get("number"),
        "Activity Score": properties.get("Activity Score", {}).get("number")
    }
    
    print(f"\nМетрики для {chat_id}:")
    for metric_name, value in metrics.items():
        print(f"{metric_name}: {value}")

if __name__ == "__main__":
    # Список чатов для проверки
    chats = [
        "https://t.me/wb_china1",
        "https://t.me/job_in_hr",
        "https://t.me/work_for_top",
        "https://t.me/rabota_udalennaya",
        "https://t.me/VisaGlobalExperts",
        "https://t.me/KrakowUA_IT",
        "https://t.me/ukraine_poland_chat",
        "https://t.me/hrexpert63_vacancies",
        "https://t.me/nomadspainchat",
        "https://t.me/people360_marketing"
    ]
    
    for chat_id in chats:
        check_chat_metrics(chat_id) 