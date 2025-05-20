from notion_integration import NotionIntegration
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('chat_metrics.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_chat_metrics(chat_id: str) -> dict:
    """
    Получает метрики чата из Notion по его ID
    """
    try:
        notion = NotionIntegration()
        
        # Ищем чат в базе данных
        response = notion.notion.databases.query(
            database_id=notion.database_id,
            filter={
                "property": "Канал/чат",
                "rich_text": {
                    "contains": chat_id
                }
            }
        )
        
        results = response.get("results", [])
        if not results:
            logger.error(f"Чат {chat_id} не найден в базе данных")
            return None
            
        # Берем первый найденный результат
        page = results[0]
        properties = page.get("properties", {})
        
        # Извлекаем метрики
        metrics = {
            'chat_id': chat_id,
            'name': properties.get("Name", {}).get("title", [{}])[0].get("text", {}).get("content", ""),
            'dau_percent': properties.get("DAU %", {}).get("number", 0),
            'messages_per_day': properties.get("Total Messages", {}).get("number", 0),
            'active_days': properties.get("Дней с сообщениями (30д)", {}).get("number", 0),
            'members_count': properties.get("Members Count", {}).get("number", 0)
        }
        
        logger.info(f"Получены метрики для чата {chat_id}")
        return metrics
        
    except Exception as e:
        logger.error(f"Ошибка при получении метрик чата {chat_id}: {e}")
        return None

def main():
    chat_id = "mari_vakansii"
    metrics = get_chat_metrics(chat_id)
    
    if metrics:
        print("\nМетрики чата:")
        for key, value in metrics.items():
            print(f"{key}: {value}")
    else:
        print(f"Не удалось получить метрики для чата {chat_id}")

if __name__ == "__main__":
    main() 