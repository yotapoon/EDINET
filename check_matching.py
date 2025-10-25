import pandas as pd
import matching
import database_manager

# --- Configuration ---
# Test target is now SpecifiedInvestment, and we test all unique names
TARGET_TABLE = "SpecifiedInvestment"
TARGET_COLUMN = "NameOfSecurities"

if __name__ == "__main__":
    print(f"--- Full Matching Test for All Unique Names in {TARGET_TABLE} ---")

    # pandasの表示設定
    pd.set_option('display.max_rows', 50)
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 150)

    # 1. 名寄せマスターを準備
    master_df = matching.create_name_code_master()
    if master_df.empty:
        print("\n[Error] Master data is empty. Aborting.")
        exit()

    # 2. 名寄せ対象の元データを取得
    source_df = database_manager.get_data_for_enrichment(TARGET_TABLE, TARGET_COLUMN)
    if source_df.empty:
        print(f"\n[Info] No data found in {TARGET_TABLE} to check.")
        exit()
    
    print(f"\nSuccessfully fetched {len(source_df)} records from {TARGET_TABLE}.")

    # 3. 全てのユニークな名前を取得 (サンプリングなし)
    unique_names = source_df[TARGET_COLUMN].dropna().unique()
    names_to_check = pd.Series(unique_names)

    if len(names_to_check) == 0:
        print("\n[Info] No unique names found to check.")
        exit()

    print(f"\n[Info] Checking matching for all {len(names_to_check)} unique names...")

    # 4. 名寄せを実行
    matched_results = matching.match_names(names_to_check, master_df)

    # 5. 結果を集計して表示
    matched_count = matched_results['matchedEdinetCode'].notna().sum()
    total_count = len(matched_results)
    match_rate = (matched_count / total_count) * 100 if total_count > 0 else 0

    print("\n--- Summary ---")
    print(f"Total unique names tested: {total_count}")
    print(f"Successfully matched:      {matched_count}")
    print(f"Failed to match:           {total_count - matched_count}")
    print(f"Match Rate:                {match_rate:.2f}%")
    
    # オプション: 一致しなかった名前のサンプルを表示
    unmatched_samples = matched_results[matched_results['matchedEdinetCode'].isna()]
    if not unmatched_samples.empty:
        print("\n--- Sample of Unmatched Names ---")
        print(unmatched_samples.head(20).to_string())

    print("\n--- Check finished ---")
