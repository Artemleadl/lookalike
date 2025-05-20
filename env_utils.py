import os

def set_env_variable(key, value, env_path=".env"):
    """
    Добавляет или обновляет переменную окружения в .env файле.
    Если переменная уже есть — заменяет её значение.
    Если переменной нет — добавляет её в конец файла.
    """
    lines = []
    found = False
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}\n"
            found = True
            break
    if not found:
        lines.append(f"{key}={value}\n")
    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines) 