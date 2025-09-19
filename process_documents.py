import datetime
import database_manager
import document_processor
import pandas as pd

def main():
    """
    メイン処理
    """
    # 処理対象の日付を指定
    # target_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    target_date = '2024-05-31' # テスト用に固定
    print(f"Processing documents for date: {target_date}")

    # ステップ1: 対象とすべきdocIDをDBから抽出する
    doc_ids = database_manager.get_doc_ids_by_date(target_date)

    if not doc_ids:
        print("No target documents found for the specified date.")
        return

    print(f"Found {len(doc_ids)} target documents.")

    # ステップ2: 各docIDについてドキュメント処理を実行し、結果を収集
    # 抽出されたデータタイプごとにDataFrameを格納する辞書
    all_extracted_data = {}

    for doc_id in doc_ids:
        # process_document_dataは辞書を返す
        extracted_for_doc = document_processor.process_document_data(doc_id)
        
        # 抽出されたデータタイプごとに結果を結合
        for data_type, df_result in extracted_for_doc.items():
            if data_type not in all_extracted_data:
                all_extracted_data[data_type] = []
            all_extracted_data[data_type].append(df_result)
    
    # 収集したデータを最終的なDataFrameに結合して表示
    if all_extracted_data:
        print("\n--- Consolidated Processed Data ---")
        for data_type, list_of_dfs in all_extracted_data.items():
            final_df_for_type = pd.concat(list_of_dfs, ignore_index=True)
            print(f"\n--- {data_type} ---")
            print(final_df_for_type)
            print("---------------------------------")
    else:
        print("No data processed from documents.")

    # ステップ3: (今後の実装) 取得したデータをDBに保存する
    # if all_extracted_data:
    #     for data_type, list_of_dfs in all_extracted_data.items():
    #         final_df_for_type = pd.concat(list_of_dfs, ignore_index=True)
    #         database_manager.save_processed_data(data_type, final_df_for_type) # 仮の関数


if __name__ == "__main__":
    main()