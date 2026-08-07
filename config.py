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
#APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
APP_PASSWORD = os.getenv("SENDER_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL") 

AES_KEY_ZEROUM = os.getenv("AES_KEY_ZEROUM")
AES_IV_ZEROUM = os.getenv("AES_IV_ZEROUM")
AES_KEY_ENERGIABET = os.getenv("AES_KEY_ENERGIABET")
AES_IV_ENERGIABET = os.getenv("AES_IV_ENERGIABET")

DB_NAME_ZRO_1_BET_ADTK = os.getenv("DB_NAME_ZRO_1_BET_ADTK")
DB_HOST_ZRO_1_BET_ADTK = os.getenv("DB_HOST_ZRO_1_BET_ADTK")
DB_PORT_ZRO_1_BET_ADTK = os.getenv("DB_PORT_ZRO_1_BET_ADTK")
DB_USER_ZRO_1_BET_ADTK = os.getenv("DB_USER_ZRO_1_BET_ADTK")
DB_PASS_ZRO_1_BET_ADTK = os.getenv("DB_PASS_ZRO_1_BET_ADTK")

# Cards de usuário usados pela melhoria de proteção de dados pessoais em
# dim_usuario (Frente A/Frente B). Fallback = card de PRODUÇÃO real
# (mesmo valor de MetabaseCard.ZeroUm_Usuarios/EnergiaBet_Usuarios em
# enums.py) -- se a variável não existir no .env (como em produção), o
# comportamento é idêntico ao de hoje. Em ambiente de validação, definir
# no .env local:
#   METABASE_CARD_USUARIOS_ZEROUM=card__21517
#   METABASE_CARD_USUARIOS_ENERGIABET=card__21518
METABASE_CARD_USUARIOS_ZEROUM = os.getenv("METABASE_CARD_USUARIOS_ZEROUM", "card__14826")
METABASE_CARD_USUARIOS_ENERGIABET = os.getenv("METABASE_CARD_USUARIOS_ENERGIABET", "card__15850")