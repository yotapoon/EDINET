import pandas as pd
import matching
import database_manager

if __name__ == "__main__":
    print("--- Analyzing unmatched names from MajorShareholders ---")

    # pandasの表示設定
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 200)

    # 1. 名寄せマスターを準備
    master_df = matching.create_name_code_master()
    if master_df.empty:
        print("\n[Error] Master data is empty. Aborting.")
        exit()

    # 2. 名寄せ対象の元データを取得
    source_df = database_manager.get_data_for_enrichment("MajorShareholders", "MajorShareholderName")
    if source_df.empty:
        print("\n[Info] No data found in MajorShareholders to check.")
        exit()

    # 3. すべてのユニークな名前を取得
    names_to_check = source_df["MajorShareholderName"].dropna().unique()
    if len(names_to_check) == 0:
        print("\n[Info] No names found in MajorShareholders to check.")
        exit()
        
    print(f"\n[Info] Checking matching for {len(names_to_check)} unique names...")

    # 4. 名寄せを実行
    all_names_series = pd.Series(names_to_check)
    matched_df = matching.match_names(all_names_series, master_df, score_cutoff=85)

    # 5. マッチングしなかった結果を抽出
    unmatched_df = matched_df[matched_df['matchedEdinetCode'].isnull()]

    # 6. 結果をファイルに保存
    output_path = "unmatched_names.csv"
    unmatched_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"\n[Result] Unmatched names saved to {output_path}")
    
    print(f"\nTotal unique names: {len(names_to_check)}")
    print(f"Unmatched names count: {len(unmatched_df)}")
    
    print("\n--- Analysis finished ---")
