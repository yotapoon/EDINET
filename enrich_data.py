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

def enrich_data(target_name: str):
    """指定されたターゲットの名寄せ処理を実行する汎用関数"""
    print(f"\n--- Starting enrichment for {target_name} ---")
    
    # 0. 設定を取得
    config = ENRICHMENT_TARGETS.get(target_name)
    if not config:
        print(f"Error: Target '{target_name}' not found in ENRICHMENT_TARGETS.")
        return

    source_table = config["source_table"]
    enriched_table = config["enriched_table"]
    name_column = config["name_column"]
    primary_key = config["primary_key"]

    # 1. 名寄せマスターを準備
    master_df = matching.create_name_code_master()
    if master_df.empty:
        print("Error: Name master is empty. Aborting.")
        return

    # 2. 名寄せ対象の元データを取得
    source_df = database_manager.get_data_for_enrichment(source_table, name_column)
    if source_df.empty:
        print(f"Info: No data found in {source_table} to enrich.")
        return

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
        return
    
    print(f"Found {len(unprocessed_df)} new records to process.")

    # 4. 名称リストに対して名寄せを実行
    names_to_match = unprocessed_df[name_column].dropna().unique()
    matched_results = matching.match_names(pd.Series(names_to_match), master_df)
    
    # 5. 名寄せ結果を元のDataFrameにマージ
    # originalNameをキーにして結合するために、カラム名を一時的に変更
    matched_results.rename(columns={'originalName': name_column}, inplace=True)
    enriched_df = pd.merge(unprocessed_df, matched_results, on=name_column, how='left')
    enriched_df['matchMethod'] = 'exact' # 今回は完全一致のみ

    # 6. 結果を新しいテーブルに保存
    database_manager.save_data(enriched_df, enriched_table)

    print(f"--- Finished enrichment for {target_name} ---")


if __name__ == "__main__":
    # 設定（ENRICHMENT_TARGETS）に定義されているキーを指定して実行
    targets_to_run = ["MajorShareholders", "SpecifiedInvestment"]
    
    for target in targets_to_run:
        enrich_data(target)
