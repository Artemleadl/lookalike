import pandas as pd
from notion_integration import NotionIntegration
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

def analyze_notion_data():
    # Инициализируем интеграцию с Notion
    notion = NotionIntegration()
    
    # Экспортируем данные в Excel
    filename = f"notion_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    notion.export_to_excel(filename)
    
    # Читаем данные из Excel
    df = pd.read_excel(filename)
    
    # Базовый анализ
    print("\n=== Базовый анализ ===")
    print(f"Всего чатов в базе: {len(df)}")
    print(f"Среднее количество подписчиков: {df['Members Count'].mean():.0f}")
    print(f"Медианное количество подписчиков: {df['Members Count'].median():.0f}")
    print(f"Максимальное количество подписчиков: {df['Members Count'].max():.0f}")
    
    # Анализ DAU
    print("\n=== Анализ DAU ===")
    print(f"Средний DAU: {df['DAU'].mean():.0f}")
    print(f"Средний процент DAU: {df['DAU %'].mean():.2f}%")
    print(f"Максимальный процент DAU: {df['DAU %'].max():.2f}%")
    
    # Анализ активности
    print("\n=== Анализ активности ===")
    print(f"Среднее количество сообщений за 24ч: {df['Total Messages'].mean():.0f}")
    print(f"Среднее количество дней с сообщениями: {df['Days With Messages'].mean():.1f}")
    
    # Создаем визуализации
    plt.figure(figsize=(15, 10))
    
    # Распределение подписчиков
    plt.subplot(2, 2, 1)
    sns.histplot(data=df, x='Members Count', bins=30)
    plt.title('Распределение количества подписчиков')
    plt.xlabel('Количество подписчиков')
    plt.ylabel('Количество чатов')
    
    # Распределение DAU
    plt.subplot(2, 2, 2)
    sns.histplot(data=df, x='DAU %', bins=30)
    plt.title('Распределение процента DAU')
    plt.xlabel('Процент DAU')
    plt.ylabel('Количество чатов')
    
    # Корреляция между подписчиками и DAU
    plt.subplot(2, 2, 3)
    sns.scatterplot(data=df, x='Members Count', y='DAU %')
    plt.title('Корреляция: Подписчики vs DAU %')
    plt.xlabel('Количество подписчиков')
    plt.ylabel('Процент DAU')
    
    # Корреляция между сообщениями и DAU
    plt.subplot(2, 2, 4)
    sns.scatterplot(data=df, x='Total Messages', y='DAU %')
    plt.title('Корреляция: Сообщения vs DAU %')
    plt.xlabel('Количество сообщений за 24ч')
    plt.ylabel('Процент DAU')
    
    plt.tight_layout()
    plt.savefig('notion_analysis.png')
    print("\nГрафики сохранены в файл 'notion_analysis.png'")
    
    # Топ-10 чатов по DAU
    print("\n=== Топ-10 чатов по DAU % ===")
    top_dau = df.nlargest(10, 'DAU %')[['Name', 'DAU %', 'Members Count', 'Total Messages']]
    print(top_dau.to_string(index=False))
    
    # Топ-10 чатов по количеству сообщений
    print("\n=== Топ-10 чатов по количеству сообщений ===")
    top_messages = df.nlargest(10, 'Total Messages')[['Name', 'Total Messages', 'DAU %', 'Members Count']]
    print(top_messages.to_string(index=False))

if __name__ == "__main__":
    analyze_notion_data() 