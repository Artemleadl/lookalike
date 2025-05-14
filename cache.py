from datetime import datetime, timedelta, timezone
 
class Cache:
    def is_expired(self, timestamp):
        """Проверка срока действия кэша (30 дней)"""
        cache_time = datetime.fromisoformat(timestamp)
        return datetime.now(timezone.utc) - cache_time > timedelta(days=30) 