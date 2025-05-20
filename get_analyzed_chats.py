import pandas as pd
from notion_integration import NotionIntegration
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import logging
import numpy as np
from scipy import stats

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analyzed_chats.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def build_target_chat_portrait(df):
    """
    Строит портрет целевого чата на основе статистических данных
    """
    print("\n=== Портрет целевого чата ===")
    
    # Основные метрики
    metrics = {
        'DAU %': 'Процент активных пользователей',
        'Activity Score': 'Интегральный показатель активности',
        'Total Messages': 'Количество сообщений',
        'Members Count': 'Количество подписчиков'
    }
    
    portrait = {}
    for metric, description in metrics.items():
        values = df[metric].dropna()
        if len(values) > 0:
            portrait[metric] = {
                'description': description,
                'mean': values.mean(),
                'median': values.median(),
                'std': values.std(),
                'min': values.min(),
                'max': values.max(),
                'q1': values.quantile(0.25),
                'q3': values.quantile(0.75),
                'iqr': values.quantile(0.75) - values.quantile(0.25)
            }
    
    # Вывод портрета
    print("\nСтатистические характеристики целевых чатов:")
    for metric, stats in portrait.items():
        print(f"\n{metric} ({stats['description']}):")
        print(f"  Среднее значение: {stats['mean']:.2f}")
        print(f"  Медиана: {stats['median']:.2f}")
        print(f"  Стандартное отклонение: {stats['std']:.2f}")
        print(f"  Минимум: {stats['min']:.2f}")
        print(f"  Максимум: {stats['max']:.2f}")
        print(f"  Q1 (25%): {stats['q1']:.2f}")
        print(f"  Q3 (75%): {stats['q3']:.2f}")
        print(f"  IQR: {stats['iqr']:.2f}")
    
    # Рекомендуемые пороговые значения
    print("\nРекомендуемые пороговые значения для поиска похожих чатов:")
    for metric, stats in portrait.items():
        lower_bound = stats['q1'] - 1.5 * stats['iqr']
        upper_bound = stats['q3'] + 1.5 * stats['iqr']
        print(f"\n{metric}:")
        print(f"  Рекомендуемый диапазон: {lower_bound:.2f} - {upper_bound:.2f}")
        print(f"  Минимальное рекомендуемое значение: {stats['q1']:.2f}")
    
    # Корреляции между метриками
    print("\nКорреляции между метриками:")
    correlation_matrix = df[list(metrics.keys())].corr()
    print(correlation_matrix)
    
    # Визуализация распределений
    plt.figure(figsize=(15, 10))
    for i, (metric, _) in enumerate(metrics.items(), 1):
        plt.subplot(2, 2, i)
        sns.histplot(data=df, x=metric, bins=30)
        plt.title(f'Распределение {metric}')
        plt.xlabel(metric)
        plt.ylabel('Количество чатов')
    
    plt.tight_layout()
    plt.savefig('target_chat_portrait.png')
    logger.info("Графики портрета сохранены в файл 'target_chat_portrait.png'")
    
    return portrait

def analyze_analyzed_chats():
    """
    Анализирует чаты со статусом 'Analyzed' из Notion базы данных
    """
    try:
        # Инициализируем интеграцию с Notion
        notion = NotionIntegration()
        
        # Получаем данные из Notion
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
        logger.info(f"Найдено {len(results)} чатов со статусом 'Analyzed'")
        
        # Подготавливаем данные для анализа
        data = []
        for page in results:
            properties = page.get("properties", {})
            data.append({
                "Name": properties.get("Name", {}).get("title", [{}])[0].get("text", {}).get("content", ""),
                "Chat ID": properties.get("Канал/чат", {}).get("rich_text", [{}])[0].get("text", {}).get("content", ""),
                "Members Count": properties.get("Members Count", {}).get("number", 0),
                "DAU": properties.get("DAU", {}).get("number", 0),
                "DAU %": properties.get("DAU %", {}).get("number", 0),
                "Total Messages": properties.get("Total Messages", {}).get("number", 0),
                "Activity Score": properties.get("Activity Score", {}).get("number", 0),
                "Last Analysis": properties.get("Last Analysis", {}).get("date", {}).get("start", "")
            })
        
        # Создаем DataFrame
        df = pd.DataFrame(data)
        
        # Экспортируем в Excel
        filename = f"analyzed_chats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df.to_excel(filename, index=False)
        logger.info(f"Данные экспортированы в {filename}")
        
        # Строим портрет целевого чата
        portrait = build_target_chat_portrait(df)
        
        # Базовый анализ
        print("\n=== Базовый анализ проанализированных чатов ===")
        print(f"Всего проанализированных чатов: {len(df)}")
        print(f"Среднее количество подписчиков: {df['Members Count'].mean():.0f}")
        print(f"Средний DAU: {df['DAU'].mean():.0f}")
        print(f"Средний процент DAU: {df['DAU %'].mean():.2f}%")
        print(f"Средний Activity Score: {df['Activity Score'].mean():.2f}")
        
        # Анализ корреляций
        print("\n=== Анализ корреляций ===")
        correlation_matrix = df[['Members Count', 'DAU', 'DAU %', 'Total Messages', 'Activity Score']].corr()
        print("\nКорреляционная матрица:")
        print(correlation_matrix)
        
        # Анализ распределений
        print("\n=== Анализ распределений ===")
        metrics = ['DAU %', 'Activity Score', 'Total Messages']
        for metric in metrics:
            q1 = df[metric].quantile(0.25)
            q3 = df[metric].quantile(0.75)
            iqr = q3 - q1
            upper_bound = q3 + 1.5 * iqr
            print(f"\n{metric}:")
            print(f"Медиана: {df[metric].median():.2f}")
            print(f"Q1 (25%): {q1:.2f}")
            print(f"Q3 (75%): {q3:.2f}")
            print(f"Верхняя граница выбросов: {upper_bound:.2f}")
        
        # Анализ успешных чатов (топ 25% по DAU % и Activity Score)
        print("\n=== Анализ успешных чатов ===")
        dau_threshold = df['DAU %'].quantile(0.75)
        activity_threshold = df['Activity Score'].quantile(0.75)
        
        successful_chats = df[
            (df['DAU %'] >= dau_threshold) & 
            (df['Activity Score'] >= activity_threshold)
        ]
        
        print(f"\nКоличество успешных чатов (топ 25% по DAU % и Activity Score): {len(successful_chats)}")
        print("\nХарактеристики успешных чатов:")
        print(f"Средний DAU %: {successful_chats['DAU %'].mean():.2f}%")
        print(f"Средний Activity Score: {successful_chats['Activity Score'].mean():.2f}")
        print(f"Среднее количество сообщений: {successful_chats['Total Messages'].mean():.0f}")
        
        print("\nСписок успешных чатов:")
        print(successful_chats[['Name', 'Chat ID', 'DAU %', 'Activity Score', 'Total Messages']].to_string(index=False))
        
        # Создаем визуализации
        plt.figure(figsize=(20, 15))
        
        # Распределение подписчиков
        plt.subplot(2, 3, 1)
        sns.histplot(data=df, x='Members Count', bins=30)
        plt.title('Распределение количества подписчиков')
        plt.xlabel('Количество подписчиков')
        plt.ylabel('Количество чатов')
        
        # Распределение DAU
        plt.subplot(2, 3, 2)
        sns.histplot(data=df, x='DAU %', bins=30)
        plt.title('Распределение процента DAU')
        plt.xlabel('Процент DAU')
        plt.ylabel('Количество чатов')
        
        # Распределение Activity Score
        plt.subplot(2, 3, 3)
        sns.histplot(data=df, x='Activity Score', bins=30)
        plt.title('Распределение Activity Score')
        plt.xlabel('Activity Score')
        plt.ylabel('Количество чатов')
        
        # Корреляция между подписчиками и DAU
        plt.subplot(2, 3, 4)
        sns.scatterplot(data=df, x='Members Count', y='DAU %')
        plt.title('Корреляция: Подписчики vs DAU %')
        plt.xlabel('Количество подписчиков')
        plt.ylabel('Процент DAU')
        
        # Корреляция между Activity Score и DAU
        plt.subplot(2, 3, 5)
        sns.scatterplot(data=df, x='Activity Score', y='DAU %')
        plt.title('Корреляция: Activity Score vs DAU %')
        plt.xlabel('Activity Score')
        plt.ylabel('Процент DAU')
        
        # Корреляция между сообщениями и DAU
        plt.subplot(2, 3, 6)
        sns.scatterplot(data=df, x='Total Messages', y='DAU %')
        plt.title('Корреляция: Сообщения vs DAU %')
        plt.xlabel('Количество сообщений')
        plt.ylabel('Процент DAU')
        
        plt.tight_layout()
        plt.savefig('analyzed_chats_analysis.png')
        logger.info("Графики сохранены в файл 'analyzed_chats_analysis.png'")
        
        # Топ-10 чатов по DAU
        print("\n=== Топ-10 чатов по DAU % ===")
        top_dau = df.nlargest(10, 'DAU %')[['Name', 'Chat ID', 'DAU %', 'Members Count', 'Activity Score']]
        print(top_dau.to_string(index=False))
        
        # Топ-10 чатов по Activity Score
        print("\n=== Топ-10 чатов по Activity Score ===")
        top_activity = df.nlargest(10, 'Activity Score')[['Name', 'Chat ID', 'Activity Score', 'DAU %', 'Members Count']]
        print(top_activity.to_string(index=False))
        
    except Exception as e:
        logger.error(f"Произошла ошибка при анализе чатов: {e}")
        raise

if __name__ == "__main__":
    analyze_analyzed_chats() 