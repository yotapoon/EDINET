import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# --- 共通設定 ---
SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
API_KEY = os.getenv("EDINET_API_KEY")
TABLE_NAME = 'Submission'
DATA_FOLDER = "data"

# --- 共通のデータベースエンジン ---
# DSN接続文字列を構築
CONNECTION_STRING = f"mssql+pyodbc:///?odbc_connect=DSN=SQLServerDSN;TrustServerCertificate=Yes;DATABASE={DATABASE_NAME}"
engine = create_engine(CONNECTION_STRING)


def upload_submission(date_str):
    """
    EDINET APIから指定された日付の提出書類一覧を取得し、データベースにアップロードする。
    """
    url = 'https://disclosure.edinet-fsa.go.jp/api/v2/documents.json'
    params = {
        'date': date_str,
        'type': 2,  # 1はメタデータのみ，2は提出書類一覧及びメタデータを取得
        "Subscription-Key": API_KEY
    }

    try:
        res = requests.get(url, params=params)
        res.raise_for_status()  # 200番台以外のステータスコードで例外を発生させる

        json_data = res.json()

        # 'results'キーが存在しない、または結果が空の場合は処理を終了
        if 'results' not in json_data or not json_data['results']:
            print(f"Info: No submission data found for {date_str}.")
            return

        results = json_data['results']
        structured_data = [
            {
                'dateFile': date_str,
                'seqNumber': item.get('seqNumber'),
                'docID': item.get('docID'),
                'edinetCode': item.get('edinetCode'),
                'secCode': item.get('secCode'),
                'JCN': item.get('JCN'),
                'filerName': item.get('filerName'),
                'fundCode': item.get('fundCode'),
                'ordinanceCode': item.get('ordinanceCode'),
                'formCode': item.get('formCode'),
                'docTypeCode': item.get('docTypeCode'),
                'periodStart': item.get('periodStart'),
                'periodEnd': item.get('periodEnd'),
                'submitDateTime': item.get('submitDateTime'),
                'docDescription': item.get('docDescription'),
                'issuerEdinetCode': item.get('issuerEdinetCode'),
                'subjectEdinetCode': item.get('subjectEdinetCode'),
                'subsidiaryEdinetCode': item.get('subsidiaryEdinetCode'),
                'currentReportReason': item.get('currentReportReason'),
                'parentDocID': item.get('parentDocID'),
                'opeDateTime': item.get('opeDateTime'),
                'withdrawalStatus': item.get('withdrawalStatus'),
                'docInfoEditStatus': item.get('docInfoEditStatus'),
                'disclosureStatus': item.get('disclosureStatus'),
                'xbrlFlag': item.get('xbrlFlag'),
                'pdfFlag': item.get('pdfFlag'),
                'attachDocFlag': item.get('attachDocFlag'),
                'englishDocFlag': item.get('englishDocFlag'),
                'csvFlag': item.get('csvFlag'),
                'legalStatus': item.get('legalStatus'),
                'FlagLoadCsv': 0,
            } for item in results
        ]

        df = pd.DataFrame(structured_data)
        df.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)
        print(f"Success: Uploaded {len(df)} records for {date_str}.")

    except requests.exceptions.HTTPError as http_err:
        print(f"Error: HTTP error occurred while fetching data for {date_str}: {http_err} - {res.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Error: Request failed for {date_str}: {req_err}")
    except Exception as e:
        print(f"Error: An unexpected error occurred during data processing for {date_str}: {e}")