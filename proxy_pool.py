import random
import time
from proxies import PROXIES

class ProxyPool:
    def __init__(self, proxies):
        self.all_proxies = proxies.copy()
        self.blocked = {}

    def get_proxy(self):
        now = time.time()
        available = [p for p in self.all_proxies if self.blocked.get(p, 0) < now]
        if not available:
            raise Exception("Нет доступных прокси!")
        return random.choice(available)

    def block_proxy(self, proxy, timeout=3600):
        self.blocked[proxy] = time.time() + timeout 