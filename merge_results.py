import pandas as pd
from datetime import datetime

# Список файлов с результатами
files = [
    'report_20250513_162244.xlsx',
    'report_20250507_155938.xlsx',
    'report_20250512_122413.xlsx',
    'report_20250511_215802.xlsx',
    'report_20250511_202954.xlsx'
]

# Создаем пустой DataFrame для объединенных результатов
all_results = pd.DataFrame()

# Читаем и объединяем данные из всех файлов
for file in files:
    try:
        df = pd.read_excel(file)
        all_results = pd.concat([all_results, df], ignore_index=True)
        print(f'Добавлены данные из файла {file}')
    except Exception as e:
        print(f'Ошибка при чтении файла {file}: {e}')

# Удаляем дубликаты по колонке 'Канал/чат'
all_results = all_results.drop_duplicates(subset=['Канал/чат'])

# Сохраняем объединенные результаты
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f'combined_results_{timestamp}.xlsx'
all_results.to_excel(output_file, index=False)

print(f'\nВсего уникальных чатов: {len(all_results)}')
print(f'Результаты сохранены в файл: {output_file}') 