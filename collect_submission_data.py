
import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()  # .envファイルから環境変数を読み込む

server_name = os.getenv("SERVER_NAME")
database_name = os.getenv("DATABASE_NAME")
connection = f"mssql+pyodbc:///?odbc_connect=DSN=SQLServerDSN;TrustServerCertificate=Yes;DATABASE={database_name}"
engine = create_engine(connection)
TABLE_NAME = 'Submission'

API_KEY = os.getenv("EDINET_API_KEY")  # EDINET APIの認証キー

def upload_submission(date_str):
    """
    EDINETにアクセスしてレスポンスを取得する関数
    """
    url = 'https://disclosure.edinet-fsa.go.jp/api/v2/documents.json'
    params = {
        'date': date_str,
        'type': 2,  # 1はメタデータのみ，2は提出書類一覧及びメタデータを取得
        "Subscription-Key": API_KEY
    }

    try:
        res = requests.get(url, params=params)

        if res.status_code == 200:
            json_data = res.json()

            # 必要なデータを抽出して構造化
            if 'results' in json_data:
                results = json_data['results']
                structured_data = []

                for item in results:
                    structured_data.append({
                        'dateFile':         date_str,
                        'seqNumber':        item.get('seqNumber'),
                        'docID':            item.get('docID'),
                        'edinetCode':       item.get('edinetCode'),
                        'secCode':          item.get('secCode'),
                        'JCN':              item.get('JCN'),
                        'filerName':        item.get('filerName'),
                        'fundCode':         item.get('fundCode'),
                        'ordinanceCode':    item.get('ordinanceCode'),
                        'formCode':         item.get('formCode'),
                        'docTypeCode':      item.get('docTypeCode'),
                        'periodStart':      item.get('periodStart'),
                        'periodEnd':        item.get('periodEnd'),
                        'submitDateTime':   item.get('submitDateTime'),
                        'docDescription':   item.get('docDescription'),
                        'issuerEdinetCode': item.get('issuerEdinetCode'),
                        'subjectEdinetCode':item.get('subjectEdinetCode'),
                        'subsidiaryEdinetCode': item.get('subsidiaryEdinetCode'),
                        'currentReportReason':  item.get('currentReportReason'),
                        'parentDocID':      item.get('parentDocID'),
                        'opeDateTime':      item.get('opeDateTime'),
                        'withdrawalStatus': item.get('withdrawalStatus'),
                        'docInfoEditStatus':item.get('docInfoEditStatus'),
                        'disclosureStatus': item.get('disclosureStatus'),
                        'xbrlFlag':         item.get('xbrlFlag'),
                        'pdfFlag':          item.get('pdfFlag'),
                        'attachDocFlag':    item.get('attachDocFlag'),
                        'englishDocFlag':   item.get('englishDocFlag'),
                        'csvFlag':          item.get('csvFlag'),
                        'legalStatus':      item.get('legalStatus'),
                        'FlagLoadCsv': 0,
                    })

                df = pd.DataFrame(structured_data)
                df.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)
                print(f"Data for {date_str} uploaded successfully. Number of records: {len(df)}")
                # return df
            else:
                print("レスポンスに結果が含まれていません。")
        else:
            print(f"Failed to fetch data: {res.status_code}")
    except Exception as e:
        print(f"Error fetching data: {e}")

def collect_date_to_load():
    """
    直近10年間の日付のうち，テーブルに存在しない日付をyyyy-mm-dd形式の文字列のリストで取得する関数
    """
    with engine.connect() as conn:
        query = f"SELECT DISTINCT format(dateFile, 'yyyy-MM-dd') as dateFile FROM {TABLE_NAME}"
        existing_dates = pd.read_sql(query, conn)['dateFile'].tolist()
        print(f"Existing dates in the table: {len(existing_dates)}")

    date_range = pd.date_range(end=pd.Timestamp.today(), periods=365*10, freq='D')
    date_list = [date.strftime('%Y-%m-%d') for date in date_range]
    new_dates = [date for date in date_list if date not in existing_dates]
    
    return new_dates


if __name__ == "__main__":
    new_dates = collect_date_to_load()
    print(f"Collecting data for {len(new_dates)} new dates.")
    # upload_submission("2025-07-16")  # Example date to upload data for
    for date in new_dates:
        # print(f"Fetching data for date: {date}")
        df_date = upload_submission(date)
