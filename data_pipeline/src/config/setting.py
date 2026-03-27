import os
from dotenv import load_dotenv
load_dotenv()
BRONZE_PATH = os.getenv("BRONZE_PATH","./data/bronze")
