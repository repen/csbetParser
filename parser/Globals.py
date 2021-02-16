import os
BASE_DIR = os.getenv('BASE_DIR', os.getcwd())
WORK_DIR = BASE_DIR
REMOTE_API = "http://185.246.66.209:5000/betcsgo"
TOKEN = os.getenv("YADISK_TOKEN", False)
if not TOKEN:
    raise ValueError("YADISK_TOKEN not token")