import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# EDINET API v2 Key
API_KEY = os.getenv("EDINET_API_KEY")

# Database Settings (MSSQL)
SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
if not DATABASE_NAME:
    raise ValueError("データベース名が設定されていません。.envファイルで 'DATABASE_NAME' を設定してください。")
SUBMISSION_TABLE_NAME = 'Submission'

# DSN接続文字列を構築
CONNECTION_STRING = f"mssql+pyodbc:///?odbc_connect=DSN=SQLServerDSN;TrustServerCertificate=Yes;DATABASE={DATABASE_NAME}"