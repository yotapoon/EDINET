import datetime
import time
from tqdm import tqdm

# これまでに作成したモジュールと、これから作成するパーサーをインポート
import database_manager
import edinet_api
import xbrl_parser
from config import SUBMISSION_TABLE_NAME


def main():
    """
    DBに保存された書類一覧から対象を絞り込み、詳細情報を抽出して別テーブルに保存する。
    """
    # --- 処理対象の設定 ---
    target_date = datetime.date(2024, 1, 4)
    # 書類形式コード '030000' は「有価証券報告書」
    target_form_code = '030000'

    # 1. DBから処理対象のdocIDリストを取得
    print(f"Fetching document list for {target_date}...")
    # (注: get_target_doc_ids関数は後ほどdatabase_manager.pyに追加する必要があります)
    doc_ids = database_manager.get_target_doc_ids(target_date, target_form_code)
    if not doc_ids:
        print(f"No documents found for {target_date} with formCode {target_form_code}.")
        return

    print(f"Found {len(doc_ids)} documents to process.")

    # 2. 各書類をループ処理
    for doc_id in tqdm(doc_ids, desc="Processing Documents"):
        try:
            # 3. EDINET APIから書類本体（ZIPファイル）をダウンロード
            # (注: download_document関数は後ほどedinet_api.pyに追加する必要があります)
            zip_content = edinet_api.download_document(doc_id)
            if not zip_content:
                continue

            # 4. XBRLパーサーで「大株主の状況」を抽出
            major_shareholders_df = xbrl_parser.extract_major_shareholders(zip_content)

            # 5. 抽出したデータをDBに保存
            if major_shareholders_df is not None and not major_shareholders_df.empty:
                # (注: save_major_shareholders関数は後ほどdatabase_manager.pyに追加する必要があります)
                database_manager.save_major_shareholders(major_shareholders_df, doc_id)

        except Exception as e:
            print(f"An error occurred while processing {doc_id}: {e}")
        finally:
            time.sleep(1)  # APIへの負荷を考慮

if __name__ == '__main__':
    main()