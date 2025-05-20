from notion_integration import NotionIntegration
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_model_fields():
    """
    Добавляет новые поля в базу данных Notion для хранения результатов предсказания модели
    """
    notion = NotionIntegration()
    
    try:
        # Получаем текущую структуру базы данных
        database = notion.notion.databases.retrieve(
            database_id=notion.database_id
        )
        
        # Добавляем новые поля
        properties = {
            "Prediction": {
                "select": {
                    "options": [
                        {"name": "Качественный", "color": "green"},
                        {"name": "Низкокачественный", "color": "red"}
                    ]
                }
            },
            "Quality Probability": {
                "number": {
                    "format": "percent"
                }
            }
        }
        
        # Обновляем базу данных
        notion.notion.databases.update(
            database_id=notion.database_id,
            properties=properties
        )
        
        logger.info("Новые поля успешно добавлены в базу данных")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении полей: {str(e)}")
        return False

if __name__ == "__main__":
    add_model_fields() 