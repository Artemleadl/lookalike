import joblib
import pandas as pd
from notion_integration import NotionIntegration
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def evaluate_chat(chat_id):
    """
    Оценивает качество чата с помощью предобученной модели
    """
    # Загружаем модель
    model = joblib.load('final_leadl_chat_quality_model.pkl')
    
    # Получаем метрики чата из Notion
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
        logger.error(f"Чат {chat_id} не найден в базе")
        return
        
    page = response["results"][0]
    properties = page.get("properties", {})
    
    # Получаем все необходимые метрики
    metrics = {
        "subscribers": properties.get("Подписчиков", {}).get("number", 0),
        "dau": properties.get("DAU", {}).get("number", 0),
        "dau_percent": properties.get("DAU %", {}).get("number", 0),
        "dau_month_avg_percent": properties.get("DAU % (месяц, среднее)", {}).get("number", 0),
        "dau_month_avg": properties.get("DAU (месяц, среднее)", {}).get("number", 0),
        "days_with_msgs": properties.get("Дней с сообщениями (30д)", {}).get("number", 0),
        "msgs_last_24h": properties.get("Всего сообщений (24ч)", {}).get("number", 0)
    }
    
    # Проверяем, есть ли хотя бы базовые метрики
    if metrics["subscribers"] is None or metrics["dau"] is None:
        logger.error(f"Отсутствуют базовые метрики для чата {chat_id}")
        return
    
    # Если среднее за месяц отсутствует, используем текущее значение
    if metrics["dau_month_avg"] is None:
        metrics["dau_month_avg"] = metrics["dau"]
    if metrics["dau_month_avg_percent"] is None:
        metrics["dau_month_avg_percent"] = metrics["dau_percent"]
    
    # Создаем DataFrame для предсказания
    df = pd.DataFrame([metrics])
    
    # Делаем предсказание
    prediction = model.predict(df)[0]
    probability = model.predict_proba(df)[0][1]
    
    # Обновляем результаты в Notion
    notion.notion.pages.update(
        page_id=page["id"],
        properties={
            "Prediction": {
                "select": {
                    "name": "Качественный" if prediction == 1 else "Низкокачественный"
                }
            },
            "Quality Probability": {
                "number": float(probability)
            }
        }
    )
    
    logger.info(f"\nРезультаты оценки для {chat_id}:")
    logger.info(f"Предсказание: {'Качественный' if prediction == 1 else 'Низкокачественный'}")
    logger.info(f"Вероятность качества: {probability:.2f}")
    logger.info("\nИспользованные метрики:")
    for metric, value in metrics.items():
        logger.info(f"{metric}: {value}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        chat_id = sys.argv[1]
    else:
        chat_id = "people360_marketing"
    evaluate_chat(chat_id) 