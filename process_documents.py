import database_manager
import document_processor
import pandas as pd
import os
import shutil

from definitions import DOCUMENT_TYPE_DEFINITIONS, DATA_PRODUCT_DEFINITIONS


def process_documents(target_data_products: list[str]):
    """
    指定されたデータプロダクトに基づいてドキュメントを処理します。
    ダウンロードはドキュメントごとに1回のみ実行されます。
    """
    print(f"Processing documents for data products: {', '.join(target_data_products)}")

    # ステップ1: 必要な「書類種別」を特定
    required_doc_types = set()
    for product in target_data_products:
        doc_type = DATA_PRODUCT_DEFINITIONS.get(product)
        if doc_type:
            required_doc_types.add(doc_type)
        else:
            print(f"Warning: Data product '{product}' is not defined. Skipping.")

    if not required_doc_types:
        print("No valid data products specified. Nothing to process.")
        return

    # ステップ2: 必要な(form_code, ordinance_code)タプルのセットを作成
    codes_to_fetch = set()
    for doc_type in required_doc_types:
        codes = DOCUMENT_TYPE_DEFINITIONS.get(doc_type)
        if codes:
            codes_to_fetch.update(codes)

    if not codes_to_fetch:
        print("Could not find any document codes for the specified data products.")
        return

    # ステップ3: 対象となるすべてのユニークな書類をDBから取得 (ここで重複ダウンロードが防止される)
    documents_to_process = database_manager.get_documents_by_codes(list(codes_to_fetch))

    if not documents_to_process:
        print(f"No target documents found for the specified data products.")
        return

    # ステップ4: 各書類について処理を実行
    for date_file, doc_id, form_code, ordinance_code, ordinance_code_short, seq_number in documents_to_process:
        print(f"\n--- Processing docID: {doc_id} (Date: {date_file}, Form: {form_code}, Ordinance: {ordinance_code}) ---")
        
        csv_path = None # クリーンアップ処理のためにスコープを広げる
        try:
            # 4a. 書類をダウンロードしてファイルパスを取得
            csv_path = document_processor.fetch_and_save_document(doc_id, ordinance_code_short)
            
            if not csv_path:
                print(f"    Skipping docID {doc_id} due to download/save failure.")
                continue
                
            # 4b. ファイルを解析して複数のデータタイプを抽出
            extracted_data_map = document_processor.parse_document_file(
                csv_path, 
                form_code=form_code, 
                ordinance_code=ordinance_code, 
                ordinance_code_short=ordinance_code_short
            )
            
            if not extracted_data_map:
                print(f"    No data extracted for docID: {doc_id}")
                continue

            # 4c. 要求された各プロダクトについて、抽出結果を確認しDBに保存
            for product_name in target_data_products:
                # この書類が対象としているプロダクトか確認
                doc_type_of_product = DATA_PRODUCT_DEFINITIONS.get(product_name)
                current_doc_type = None
                for dt, codes in DOCUMENT_TYPE_DEFINITIONS.items():
                    if (form_code, ordinance_code) in codes:
                        current_doc_type = dt
                        break
                
                if doc_type_of_product != current_doc_type:
                    continue # この書類は当該プロダクトの対象外

                # データが抽出されたか確認
                df = extracted_data_map.get(product_name)
                if df is None or df.empty:
                    print(f"    Info: No data found for '{product_name}' in docID: {doc_id}")
                    continue

                # 共通のメタデータをDataFrameに追加
                if 'docId' not in df.columns:
                    df['docId'] = doc_id
                if 'seqNumber' not in df.columns:
                    df['seqNumber'] = seq_number
                if 'dateFile' not in df.columns:
                    if 'SubmissionDate' not in df.columns and 'reportObligationDate' not in df.columns:
                        df['dateFile'] = date_file

                # 汎用保存関数を呼び出す
                database_manager.save_data(df, product_name)
        finally:
            # 4d. 処理済みのCSVファイルとフォルダを削除
            if csv_path:
                try:
                    doc_folder_path = os.path.dirname(csv_path)
                    if os.path.exists(doc_folder_path):
                        shutil.rmtree(doc_folder_path)
                        print(f"    Cleaned up temporary folder: {doc_folder_path}")
                except Exception as e:
                    print(f"    Warning: Failed to clean up temporary folder {doc_folder_path}: {e}")

    print(f"\n--- Finished processing for all specified data products. ---")

if __name__ == "__main__":
    # 処理対象のデータプロダクトリスト
    # 'MajorShareholders', 'ShareholderComposition', 'Officer', 'SpecifiedInvestment', 'VotingRights'
    # 'LargeVolumeHoldingReport', 'BuybackStatusReport' から選択
    TARGET_DATA_PRODUCTS = [
        #'MajorShareholders',
        #'ShareholderComposition',
        #'Officer',
        "SpecifiedInvestment",
        "VotingRights",
        # 'BuybackStatusReport'
        # 'LargeVolumeHoldingReport'
    ]

    process_documents(TARGET_DATA_PRODUCTS)