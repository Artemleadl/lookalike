from notion_integration import NotionIntegration
import logging
from evaluate_chat import evaluate_chat
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_all_chats():
    """
    Получает список всех чатов из базы данных Notion
    """
    notion = NotionIntegration()
    response = notion.notion.databases.query(
        database_id=notion.database_id
    )
    results = response.get("results", [])
    
    chats = []
    for page in results:
        properties = page.get("properties", {})
        
        # Безопасное получение chat_id
        chat_id_prop = properties.get("Канал/чат", {}).get("rich_text", [])
        chat_id = chat_id_prop[0].get("text", {}).get("content", "") if chat_id_prop else ""
        
        # Безопасное получение name
        name_prop = properties.get("Name", {}).get("title", [])
        name = name_prop[0].get("text", {}).get("content", "") if name_prop else ""
        
        if chat_id:
            chats.append({
                "chat_id": chat_id,
                "name": name
            })
    
    return chats

def evaluate_all_chats():
    """
    Проводит оценку всех чатов в базе данных
    """
    chats = get_all_chats()
    total = len(chats)
    logger.info(f"Найдено {total} чатов для оценки")
    
    for i, chat in enumerate(chats, 1):
        logger.info(f"\nОценка чата {i}/{total}: {chat['name']} ({chat['chat_id']})")
        try:
            evaluate_chat(chat['chat_id'])
            # Добавляем небольшую задержку между запросами
            time.sleep(1)
        except Exception as e:
            logger.error(f"Ошибка при оценке чата {chat['chat_id']}: {str(e)}")
    
    logger.info("\nОценка всех чатов завершена")

def get_not_evaluated_chats():
    """
    Возвращает список неоценённых чатов (Prediction и Quality Probability отсутствуют)
    """
    notion = NotionIntegration()
    response = notion.notion.databases.query(
        database_id=notion.database_id
    )
    results = response.get("results", [])
    not_evaluated_chats = []
    for page in results:
        properties = page.get("properties", {})
        chat_id_prop = properties.get("Канал/чат", {}).get("rich_text", [])
        chat_id = chat_id_prop[0].get("text", {}).get("content", "") if chat_id_prop else ""
        name_prop = properties.get("Name", {}).get("title", [])
        name = name_prop[0].get("text", {}).get("content", "") if name_prop else ""
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
        if not (prediction and probability is not None) and chat_id:
            not_evaluated_chats.append(chat_id)
    return not_evaluated_chats

def check_chat_metrics(chat_id):
    """
    Проверяет наличие всех необходимых метрик для чата
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
        logger.error(f"Чат {chat_id} не найден в базе")
        return False
        
    page = response["results"][0]
    properties = page.get("properties", {})
    
    # Проверяем наличие всех необходимых метрик
    metrics = {
        "Подписчиков": properties.get("Подписчиков", {}).get("number"),
        "DAU": properties.get("DAU", {}).get("number"),
        "DAU %": properties.get("DAU %", {}).get("number"),
        "DAU % (месяц, среднее)": properties.get("DAU % (месяц, среднее)", {}).get("number"),
        "DAU (месяц, среднее)": properties.get("DAU (месяц, среднее)", {}).get("number"),
        "Дней с сообщениями (30д)": properties.get("Дней с сообщениями (30д)", {}).get("number"),
        "Всего сообщений (24ч)": properties.get("Всего сообщений (24ч)", {}).get("number")
    }
    
    missing_metrics = [k for k, v in metrics.items() if v is None]
    
    if missing_metrics:
        logger.warning(f"Отсутствующие метрики для {chat_id}: {', '.join(missing_metrics)}")
        return False
    
    return True

def evaluate_chats_from_list(chat_ids):
    from evaluate_chat import evaluate_chat
    evaluated = []
    skipped = []
    for chat_id in chat_ids:
        print(f"\nПробую оценить: {chat_id}")
        # Проверяем наличие базовых метрик
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
            skipped.append(chat_id)
            continue
        page = response["results"][0]
        properties = page.get("properties", {})
        subs = properties.get("Подписчиков", {}).get("number")
        dau = properties.get("DAU", {}).get("number")
        if subs is None or dau is None:
            print(f"Пропущен (нет базовых метрик): {chat_id}")
            skipped.append(chat_id)
            continue
        try:
            evaluate_chat(chat_id)
            evaluated.append(chat_id)
        except Exception as e:
            print(f"Ошибка при оценке {chat_id}: {e}")
            skipped.append(chat_id)
    print(f"\nОценено: {len(evaluated)} чатов")
    print(f"Пропущено: {len(skipped)} чатов")
    if skipped:
        print("\nТребуют ручной проверки/заполнения:")
        for chat_id in skipped:
            print(f"- {chat_id}")

if __name__ == "__main__":
    chat_ids = [
        "@mari_vakansii",
        "@vakansii_moskve_msk_rf",
        "@chatik_piarvz",
        "@marketplaceone",
        "@theyescoin_chat24",
        "@fotomarketmp",
        "https://t.me/work_for_top",
        "https://t.me/rabota_udalennaya",
        "https://t.me/VisaGlobalExperts",
        "https://t.me/KrakowUA_IT",
        "https://t.me/ukraine_poland_chat",
        "https://t.me/hrexpert63_vacancies",
        "https://t.me/nomadspainchat",
        "https://t.me/helpedtech",
        "https://t.me/SaveLeadClub",
        "https://t.me/tgjob",
        "https://t.me/redmilliard_chat",
        "https://t.me/sswork",
        "https://t.me/potrindet",
        "https://t.me/Dubai_UAE_Hub",
        "https://t.me/prichalpsy",
        "https://t.me/chatfornomads",
        "https://t.me/hr_recrute",
        "https://t.me/freelancervchate",
        "https://t.me/smm_chat5",
        "https://t.me/Solfreelance",
        "https://t.me/TargetContext",
        "https://t.me/n8n_community",
        "https://t.me/wb_china1",
        "https://t.me/job_in_hr"
    ]
    evaluate_chats_from_list(chat_ids) 