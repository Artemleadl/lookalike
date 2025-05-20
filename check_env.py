import os
from dotenv import load_dotenv

load_dotenv()

print("NOTION_TOKEN:", os.getenv("NOTION_TOKEN"))
print("NOTION_DATABASE_ID:", os.getenv("NOTION_DATABASE_ID")) 