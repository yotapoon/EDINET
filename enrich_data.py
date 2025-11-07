"""
名寄せ処理を実行するメインスクリプト
"""
import pandas as pd
import database_manager
import matching

# --- 定数定義 ---
# 処理対象の情報をここに定義する
ENRICHMENT_TARGETS = {
    "MajorShareholders": {
        "source_table": "MajorShareholders",
        "enriched_table": "EnrichedMajorShareholders",
        "name_column": "MajorShareholderName",
        "primary_key": ["SubmissionDate", "SecuritiesCode", "shareholderId"],
    },
    "SpecifiedInvestment": {
        "source_table": "SpecifiedInvestment",
        "enriched_table": "EnrichedSpecifiedInvestment",
        "name_column": "NameOfSecurities",
        "primary_key": ["SubmissionDate", "SecuritiesCode", "HoldingEntity", "rowId"],
    },
}

def enrich_data(target_name: str, test_mode: bool = False):
    """
    指定されたターゲットの名寄せ処理を実行する汎用関数

    Args:
        target_name (str): ENRICHMENT_TARGETSで定義されたターゲット名
        test_mode (bool): Trueの場合、DB保存せず結果のDataFrameを返す
    
    Returns:
        pd.DataFrame or None: test_modeがTrueの場合、名寄せ結果のDataFrameを返す
    """
    print(f"\n--- Starting enrichment for {target_name} (Test Mode: {test_mode}) ---")
    
    # 0. 設定を取得
    config = ENRICHMENT_TARGETS.get(target_name)
    if not config:
        print(f"Error: Target '{target_name}' not found in ENRICHMENT_TARGETS.")
        return None

    source_table = config["source_table"]
    enriched_table = config["enriched_table"]
    name_column = config["name_column"]
    primary_key = config["primary_key"]

    # 1. 名寄せマスターを準備
    master_df = matching.create_name_code_master()
    if master_df.empty:
        print("Error: Name master is empty. Aborting.")
        return None

    # 2. 名寄せ対象の元データを取得
    source_df = database_manager.get_data_for_enrichment(source_table, name_column)
    if source_df.empty:
        print(f"Info: No data found in {source_table} to enrich.")
        return None

    # 3. 処理済みのキーを取得して、未処理のデータに絞り込む
    processed_keys = database_manager.get_enriched_keys(enriched_table)
    if processed_keys:
        source_df["_key"] = list(source_df[primary_key].itertuples(index=False, name=None))
        unprocessed_df = source_df[~source_df["_key"].isin(processed_keys)].copy()
        unprocessed_df.drop(columns=["_key"], inplace=True)
    else:
        unprocessed_df = source_df

    if unprocessed_df.empty:
        print("Info: All records are already enriched. Nothing to do.")
        # test_modeでも空のDataFrameを返す
        return unprocessed_df if test_mode else None
    
    print(f"Found {len(unprocessed_df)} new records to process.")

    # 4. 名称リストに対して名寄せを実行
    names_to_match = unprocessed_df[name_column].dropna().unique()
    matched_results = matching.match_names(pd.Series(names_to_match), master_df)
    
    # 5. 名寄せ結果を元のDataFrameにマージ
    # originalNameをキーにして結合するために、カラム名を一時的に変更
    matched_results.rename(columns={'originalName': name_column}, inplace=True)
    enriched_df = pd.merge(unprocessed_df, matched_results, on=name_column, how='left')
    enriched_df['matchMethod'] = 'exact' # 今回は完全一致のみ

    # 6. 結果を評価または保存
    if test_mode:
        # テストモード時はDataFrameを返す
        return enriched_df
    else:
        # 通常モード時は結果を新しいテーブルに保存
        database_manager.save_data(enriched_df, enriched_table)
        print(f"--- Finished enrichment for {target_name} ---")
        return None


if __name__ == "__main__":
    # --- モード設定 ---
    # Trueにすると、DBに保存せず、名寄せ結果のプレビューと統計情報を表示します
    TEST_MODE = False
    TARGET_NAME = "SpecifiedInvestment" # テスト対象

    if not TEST_MODE:
        # 通常実行：指定されたターゲットのみを処理
        TARGET_DATA_PRODUCTS = ["SpecifiedInvestment"]
        print(f"--- Running in Normal Mode for: {', '.join(TARGET_DATA_PRODUCTS)} ---")
        for target in TARGET_DATA_PRODUCTS:
            enrich_data(target, test_mode=False)
    else:
        # テスト実行
        print(f"--- Running in Test Mode for: {TARGET_NAME} ---")
        
        # 1. テストモードで名寄せ処理を実行
        results_df = enrich_data(TARGET_NAME, test_mode=True)

        if results_df is not None and not results_df.empty:
            # 2. 結果の統計情報を計算 (正しい列名を使用)
            total_records = len(results_df)
            matched_records = results_df['matchedEdinetCode'].notna().sum()
            unmatched_records = total_records - matched_records
            config = ENRICHMENT_TARGETS[TARGET_NAME]
            name_col = config['name_column']

            print("\n--- Enrichment Test Results ---")
            print(f"Total Records Processed: {total_records}")
            print(f"Successfully Matched:    {matched_records}")
            print(f"Unmatched:               {unmatched_records}")
            print("---------------------------------")

            # 3. マッチした結果のサンプルを表示 (正しい列名を使用)
            if matched_records > 0:
                print("\n[Sample of Matched Records]")
                matched_sample = results_df[results_df['matchedEdinetCode'].notna()].head()
                print(matched_sample[[name_col, 'matchedEdinetCode', 'matchedSecCode']])
            
            # 4. マッチしなかった結果を件数順に上位20件表示 (正しい列名を使用)
            if unmatched_records > 0:
                print("\n[Top 20 Unmatched Records by Frequency]")
                unmatched_counts = results_df[results_df['matchedEdinetCode'].isna()][name_col].value_counts()
                print(unmatched_counts.head(50))

            print("\n--- Test Finished ---")
        elif results_df is not None:
            print("\n--- No new records to process. Test finished. ---")
        else:
            print("\n--- Test failed or was aborted. ---")
