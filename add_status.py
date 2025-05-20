import pandas as pd

df = pd.read_excel('RECOVERED_NONEMPTY_RESULTS.xlsx')
days_in_month = 30

def get_resume(row):
    members = row.get('members_count')
    avg_dau = row.get('avg_dau')
    avg_dau_percent = row.get('avg_dau_percent')
    days_with_messages = row.get('days_with_messages')
    if pd.isna(members) or members is None:
        return 'нет данных'
    try:
        members = float(members)
    except:
        return 'нет данных'
    if members >= 1000:
        if (avg_dau is not None and avg_dau < 5 and avg_dau_percent is not None and avg_dau_percent < 0.1) or (days_with_messages is not None and days_with_messages < days_in_month * 0.3):
            return 'Мертвый чат'
        elif avg_dau_percent is not None and avg_dau_percent > 10:
            return 'Флудилка'
        else:
            return 'Живой чат'
    elif members >= 100:
        if (avg_dau is not None and avg_dau < 1 and avg_dau_percent is not None and avg_dau_percent < 0.1) or (days_with_messages is not None and days_with_messages < days_in_month * 0.3):
            return 'Мертвый чат'
        elif avg_dau_percent is not None and avg_dau_percent > 15:
            return 'Флудилка'
        elif avg_dau is not None and avg_dau >= 1:
            return 'Живой чат (нишевой/объявлений)'
        else:
            return 'Живой чат'
    else:
        if avg_dau is not None and avg_dau < 1 or (days_with_messages is not None and days_with_messages < days_in_month * 0.3):
            return 'Мертвый чат'
        elif avg_dau_percent is not None and avg_dau_percent > 20:
            return 'Флудилка'
        else:
            return 'Живой чат'

df['Резюме'] = df.apply(get_resume, axis=1)
df.to_excel('RECOVERED_NONEMPTY_RESULTS_WITH_STATUS.xlsx', index=False)
print('Готово! Строк:', len(df)) 