from notion_integration import NotionIntegration
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_evaluation_status():
    """
    Проверяет статус оценки чатов в базе данных Notion
    """
    notion = NotionIntegration()
    response = notion.notion.databases.query(
        database_id=notion.database_id
    )
    results = response.get("results", [])
    
    evaluated = 0
    not_evaluated = 0
    not_evaluated_chats = []
    
    for page in results:
        properties = page.get("properties", {})
        
        # Получаем информацию о чате
        chat_id_prop = properties.get("Канал/чат", {}).get("rich_text", [])
        chat_id = chat_id_prop[0].get("text", {}).get("content", "") if chat_id_prop else ""
        
        name_prop = properties.get("Name", {}).get("title", [])
        name = name_prop[0].get("text", {}).get("content", "") if name_prop else ""
        
        # Проверяем наличие оценки
        prediction = None
        probability = None
        prediction_prop = properties.get("Prediction")
        if prediction_prop and isinstance(prediction_prop, dict):
            select_val = prediction_prop.get("select")
            if select_val and isinstance(select_val, dict):
                prediction = select_val.get("name", "")
        probability_prop = properties.get("Quality Probability")
        if probability_prop and isinstance(probability_prop, dict):
            probability = probability_prop.get("number")
        
        if prediction and probability is not None:
            evaluated += 1
        else:
            not_evaluated += 1
            not_evaluated_chats.append({
                "name": name,
                "chat_id": chat_id
            })
    
    logger.info(f"\nСтатистика оценки чатов:")
    logger.info(f"Всего чатов: {evaluated + not_evaluated}")
    logger.info(f"Оценено: {evaluated}")
    logger.info(f"Не оценено: {not_evaluated}")
    
    if not_evaluated_chats:
        logger.info("\nСписок неоцененных чатов:")
        for chat in not_evaluated_chats:
            logger.info(f"- {chat['name']} ({chat['chat_id']})")

if __name__ == "__main__":
    check_evaluation_status() 