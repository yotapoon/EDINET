import requests
from config import API_KEY

BASE_URL_V2 = "https://disclosure.edinet-fsa.go.jp/api/v2"

def fetch_submission_list(date_str: str) -> dict | None:
    """
    EDINET API v2から指定された日付の提出書類一覧をJSONで取得する。
    """
    url = f"{BASE_URL_V2}/documents.json"
    params = {
        'date': date_str,
        'type': 2,  # 提出書類一覧及びメタデータを取得
        "Subscription-Key": API_KEY
    }

    try:
        res = requests.get(url, params=params, timeout=30)
        res.raise_for_status()  # 200番台以外のステータスコードで例外を発生
        return res.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"Error: HTTP error occurred while fetching data for {date_str}: {http_err} - {res.text}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"Error: Request failed for {date_str}: {req_err}")
        return None
    
    except requests.exceptions.RequestException as req_err:
        print(f"Error: Request failed for {date_str}: {req_err}")
        return None

if __name__ == '__main__':
    import datetime
    import json

    print("--- Testing edinet_api.py ---")

    # 今日の日付でテスト実行
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    print(f"Fetching submission list for today: {today_str}...")

    # 書類一覧を取得
    submission_data = fetch_submission_list(today_str)

    if submission_data and submission_data.get('results'):
        count = submission_data.get('metadata', {}).get('resultset', {}).get('count', 0)
        print(f"Successfully fetched data. Found {count} results.")
        # 取得したJSONデータのうち、最初の3件を整形して表示
        print("\n--- Sample Results (first 3) ---")
        print(json.dumps(submission_data['results'][:3], indent=2, ensure_ascii=False))
    else:
        print("Failed to fetch data or no data available for today.")

