import os
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DB", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME_ZEROUM = os.getenv("DB_NAME_ZEROUM", "meu_banco")
DB_NAME_ENERGIABET = os.getenv("DB_NAME_ENERGIABET", "meu_banco")
DB_USER = os.getenv("DB_USER", "usuario")
DB_PASS = os.getenv("DB_PASS", "senha")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

API_AUTH = os.getenv("API_AUTH", "")
API_USER_ZEROUM = os.getenv("API_USER_ZEROUM", "")
API_PASS_ZEROUM = os.getenv("API_PASS_ZEROUM", "")
API_USER_ENERGIABET = os.getenv("API_USER_ENERGIABET", "")
API_PASS_ENERGIABET = os.getenv("API_PASS_ENERGIABET", "")
API_ROTA_CSV = os.getenv("API_ROTA_CSV", "")

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL") 

AES_KEY_ZEROUM = os.getenv("AES_KEY_ZEROUM")
AES_IV_ZEROUM = os.getenv("AES_IV_ZEROUM")
AES_KEY_ENERGIABET = os.getenv("AES_KEY_ENERGIABET")
AES_IV_ENERGIABET = os.getenv("AES_IV_ENERGIABET")