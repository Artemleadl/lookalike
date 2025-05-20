from notion_integration import NotionIntegration
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update_metrics.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def update_chat_metrics(chat_id: str, metrics: dict):
    """
    Обновляет метрики чата в Notion
    """
    notion = NotionIntegration()
    
    # Получаем страницу чата
    response = notion.notion.databases.query(
        database_id=notion.database_id,
        filter={
            "property": "Канал/чат",
            "rich_text": {
                "equals": chat_id
            }
        }
    )
    if not response.get('results'):
        logger.error(f"Чат {chat_id} не найден в базе данных")
        return False
    
    page_id = response['results'][0]['id']
    
    # Обновляем метрики
    properties = {
        "Подписчиков": {"number": metrics['members_count']},
        "DAU": {"number": metrics['dau']},
        "DAU %": {"number": metrics['dau_percent']},
        "Всего сообщений (24ч)": {"number": metrics['messages_per_day']},
        "Дней с сообщениями (30д)": {"number": metrics['active_days']},
        "Название": {"title": [{"text": {"content": metrics['name']}}]}
    }
    
    try:
        notion.notion.pages.update(
            page_id=page_id,
            properties=properties
        )
        logger.info(f"Метрики для чата {chat_id} успешно обновлены")
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении метрик для чата {chat_id}: {str(e)}")
        return False

def main():
    chat_id = "https://t.me/karta_po"
    metrics = {
        "name": "Ловля дат на карту побыта (inpol)",
        "members_count": 5474,
        "dau": 1532,
        "dau_percent": 28.0,  # (1532/5474)*100
        "messages_per_day": 0,  # Предполагаем, что сообщений нет
        "active_days": 0  # Предполагаем, что активных дней нет
    }
    
    result = update_chat_metrics(chat_id, metrics)
    print(f"Результат обновления: {'Успешно' if result else 'Ошибка'}")

if __name__ == "__main__":
    main() 