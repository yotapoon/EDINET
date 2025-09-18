import datetime
import time
import pandas as pd
from tqdm import tqdm

# 分割・作成した新しいモジュールをインポート
import edinet_api
import database_manager


def main():
    """
    指定した期間の提出書類一覧を取得し、データベースに保存するメイン処理。
    """
    # 取得対象の期間を指定 (今日から100日前まで)
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=100)
    # 

    date_range = pd.date_range(start_date, end_date)
    print(f"Collecting submission lists from {start_date} to {end_date}...")

    # データベースから既存の日付を取得
    existing_dates = database_manager.get_existing_dates()
    print(f"Found {len(existing_dates)} existing dates in the database.")

    for date in tqdm(date_range, desc="Processing dates"):
        date_str = date.strftime('%Y-%m-%d')

        # 日付がすでに存在する場合はスキップ
        if date_str in existing_dates:
            # tqdmの進捗表示を考慮して、何もしないが出力を出す
            # print(f"Skipping {date_str} as it already exists in the database.")
            continue
        
        try:
            # 1. APIモジュールを使って書類一覧(JSON)を取得
            json_data = edinet_api.fetch_submission_list(date_str)

            if not json_data or 'results' not in json_data or not json_data['results']:
                # print(f"Info: No submission data found for {date_str}.")
                time.sleep(1)
                continue

            # 2. JSONをDataFrameに整形
            df = _format_submission_data(json_data['results'], date_str)

            # 3. DB管理モジュールを使ってDBに保存
            database_manager.save_submission_list(df, date_str)
        
        except Exception as e:
            print(f"An unexpected error occurred for date {date_str}: {e}")
        finally:
            # APIサーバーへの負荷を考慮して待機
            time.sleep(1)

def _format_submission_data(results: list, date_str: str) -> pd.DataFrame:
    """
    APIから取得した提出書類一覧のJSON(results)をDataFrameに整形する。
    このロジックは、元のedinet_utils.pyにあったものを移植したものです。
    """
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
            'xbrlFlag': item.get('xbrlFlag', 0),
            'pdfFlag': item.get('pdfFlag', 0),
            'attachDocFlag': item.get('attachDocFlag', 0),
            'englishDocFlag': item.get('englishDocFlag', 0),
            'csvFlag': item.get('csvFlag', 0),
            'legalStatus': item.get('legalStatus'),
            'FlagLoadCsv': 0,
        } for item in results
    ]
    return pd.DataFrame(structured_data)

if __name__ == '__main__':
    main()
