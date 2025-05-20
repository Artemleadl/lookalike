import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('chat_scoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ChatScorer:
    def __init__(self):
        # Пороговые значения для оценки
        self.thresholds = {
            'dau_percent': {
                'critical': 0.1,  # Ниже этого значения чат считается нецелевым
                'good': 0.3,      # Выше этого значения чат считается перспективным
                'excellent': 0.5  # Выше этого значения чат считается высокоцелевым
            },
            'messages_per_day': {
                'critical': 5,    # Ниже этого значения чат считается нецелевым
                'good': 30,       # Выше этого значения чат считается перспективным
                'excellent': 50   # Выше этого значения чат считается высокоцелевым
            },
            'active_days': {
                'critical': 15,   # Ниже этого значения чат считается нецелевым
                'good': 20,       # Выше этого значения чат считается перспективным
                'excellent': 25   # Выше этого значения чат считается высокоцелевым
            },
            'members_count': {
                'large': 100000   # Порог для больших чатов
            }
        }
        
        # Веса для различных метрик
        self.weights = {
            'dau_percent': 0.4,
            'messages_per_day': 0.3,
            'active_days': 0.2,
            'members_quality': 0.1
        }

    def calculate_dau_score(self, dau_percent: float) -> Tuple[float, str]:
        """
        Оценка чата по проценту DAU
        """
        if dau_percent < self.thresholds['dau_percent']['critical']:
            return 0.0, "Критически низкая вовлеченность"
        elif dau_percent < self.thresholds['dau_percent']['good']:
            return 0.5, "Средняя вовлеченность"
        elif dau_percent < self.thresholds['dau_percent']['excellent']:
            return 0.8, "Хорошая вовлеченность"
        else:
            return 1.0, "Отличная вовлеченность"

    def calculate_messages_score(self, messages_per_day: int) -> Tuple[float, str]:
        """
        Оценка чата по количеству сообщений в день
        """
        if messages_per_day < self.thresholds['messages_per_day']['critical']:
            return 0.0, "Критически низкая активность"
        elif messages_per_day < self.thresholds['messages_per_day']['good']:
            return 0.5, "Средняя активность"
        elif messages_per_day < self.thresholds['messages_per_day']['excellent']:
            return 0.8, "Хорошая активность"
        else:
            return 1.0, "Отличная активность"

    def calculate_active_days_score(self, active_days: int) -> Tuple[float, str]:
        """
        Оценка чата по количеству активных дней
        """
        if active_days < self.thresholds['active_days']['critical']:
            return 0.5, "Низкая регулярность активности"
        elif active_days < self.thresholds['active_days']['good']:
            return 0.7, "Средняя регулярность активности"
        elif active_days < self.thresholds['active_days']['excellent']:
            return 0.9, "Хорошая регулярность активности"
        else:
            return 1.0, "Отличная регулярность активности"

    def calculate_members_quality_score(self, members_count: int, dau_percent: float) -> Tuple[float, str]:
        """
        Оценка качества аудитории с учетом размера и вовлеченности
        """
        if members_count > self.thresholds['members_count']['large'] and dau_percent < self.thresholds['dau_percent']['critical']:
            return 0.0, "Большая, но неактивная аудитория"
        elif members_count > self.thresholds['members_count']['large'] and dau_percent < self.thresholds['dau_percent']['good']:
            return 0.3, "Большая аудитория со средней вовлеченностью"
        elif members_count > self.thresholds['members_count']['large'] and dau_percent >= self.thresholds['dau_percent']['good']:
            return 0.8, "Большая и активная аудитория"
        elif dau_percent >= self.thresholds['dau_percent']['good']:
            return 1.0, "Активная аудитория"
        else:
            return 0.5, "Среднее качество аудитории"

    def calculate_total_score(self, chat_data: Dict) -> Dict:
        """
        Расчет общего скора чата на основе всех метрик
        """
        try:
            # Получаем базовые оценки по каждой метрике
            dau_score, dau_comment = self.calculate_dau_score(chat_data['dau_percent'])
            messages_score, messages_comment = self.calculate_messages_score(chat_data['messages_per_day'])
            active_days_score, active_days_comment = self.calculate_active_days_score(chat_data['active_days'])
            members_score, members_comment = self.calculate_members_quality_score(
                chat_data['members_count'],
                chat_data['dau_percent']
            )

            # Рассчитываем общий скор
            total_score = (
                dau_score * self.weights['dau_percent'] +
                messages_score * self.weights['messages_per_day'] +
                active_days_score * self.weights['active_days'] +
                members_score * self.weights['members_quality']
            )

            # Определяем статус чата
            if total_score >= 0.8:
                status = "Высокоперспективный"
            elif total_score >= 0.6:
                status = "Перспективный"
            elif total_score >= 0.4:
                status = "Средний"
            else:
                status = "Низкоперспективный"

            return {
                'total_score': total_score,
                'status': status,
                'details': {
                    'dau': {
                        'score': dau_score,
                        'comment': dau_comment
                    },
                    'messages': {
                        'score': messages_score,
                        'comment': messages_comment
                    },
                    'active_days': {
                        'score': active_days_score,
                        'comment': active_days_comment
                    },
                    'members': {
                        'score': members_score,
                        'comment': members_comment
                    }
                }
            }

        except Exception as e:
            logger.error(f"Ошибка при расчете скора: {e}")
            return {
                'total_score': 0,
                'status': "Ошибка расчета",
                'details': {
                    'error': str(e)
                }
            }

    def analyze_chats(self, chats_data: List[Dict]) -> pd.DataFrame:
        """
        Анализ списка чатов и создание DataFrame с результатами
        """
        results = []
        for chat in chats_data:
            score_result = self.calculate_total_score(chat)
            results.append({
                'chat_id': chat.get('chat_id', ''),
                'name': chat.get('name', ''),
                'total_score': score_result['total_score'],
                'status': score_result['status'],
                'dau_percent': chat.get('dau_percent', 0),
                'messages_per_day': chat.get('messages_per_day', 0),
                'active_days': chat.get('active_days', 0),
                'members_count': chat.get('members_count', 0),
                'dau_comment': score_result['details']['dau']['comment'],
                'messages_comment': score_result['details']['messages']['comment'],
                'active_days_comment': score_result['details']['active_days']['comment'],
                'members_comment': score_result['details']['members']['comment']
            })
        
        return pd.DataFrame(results)

def main():
    # Пример использования
    scorer = ChatScorer()
    
    # Пример данных чата
    sample_chat = {
        'chat_id': 'https://t.me/example',
        'name': 'Example Chat',
        'dau_percent': 0.35,
        'messages_per_day': 45,
        'active_days': 22,
        'members_count': 5000
    }
    
    # Расчет скора для одного чата
    result = scorer.calculate_total_score(sample_chat)
    print("\nРезультаты оценки чата:")
    print(f"Общий скор: {result['total_score']:.2f}")
    print(f"Статус: {result['status']}")
    print("\nДетали оценки:")
    for metric, details in result['details'].items():
        print(f"{metric}: {details['comment']} (скор: {details['score']:.2f})")

if __name__ == "__main__":
    main() 