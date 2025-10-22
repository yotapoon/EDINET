import pandas as pd
import matching

if __name__ == "__main__":
    print("--- Checking name master creation ---")
    
    # pandasの表示設定
    pd.set_option('display.max_rows', 20)
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 150)

    # 名寄せマスターを作成
    master_df = matching.create_name_code_master()

    if not master_df.empty:
        print("\n[Result] Master data head:")
        print(master_df.head(10))
        print(f"\n[Result] Total unique names in master: {len(master_df)}")
    else:
        print("\n[Result] Master data is empty.")

    print("\n--- Check finished ---")

