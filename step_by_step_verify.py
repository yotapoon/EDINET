import pandas as pd
import database_manager
from matching import _normalize_name

if __name__ == "__main__":
    print("--- Step-by-Step Verification of Pandas Operations ---")

    # 1. データベースから全データを取得
    source_df = database_manager.get_data_for_enrichment("SpecifiedInvestment", "NameOfSecurities")
    if source_df.empty:
        print("Database returned no data. Aborting.")
        exit()
    print(f"\nStep 1: Successfully loaded {len(source_df)} records.")

    # 2. 問題の文字列を含む行を特定
    problem_string = '凸版印刷㈱'
    test_series = source_df[source_df['NameOfSecurities'] == problem_string]['NameOfSecurities']
    if test_series.empty:
        print(f"Could not find the exact string '{problem_string}' to test. Aborting.")
        exit()
    print(f"\nStep 2: Isolated the test string: '{problem_string}'")

    # 3. pandasの .str.replace() をテスト
    print("\n--- Testing pandas .str.replace() ---")
    try:
        cleaned_series_str_replace = test_series.str.replace('㈱', '', regex=False)
        print(f"Result of .str.replace(): [ {cleaned_series_str_replace.iloc[0]} ]")
        if '㈱' not in cleaned_series_str_replace.iloc[0]:
            print("SUCCESS: .str.replace() correctly removed the character.")
        else:
            print("FAILURE: .str.replace() FAILED to remove the character.")
    except Exception as e:
        print(f"ERROR during .str.replace(): {e}")

    # 4. _normalize_name 関数を .apply() でテスト
    print("\n--- Testing .apply(_normalize_name) ---")
    try:
        cleaned_series_apply = test_series.apply(_normalize_name)
        print(f"Result of .apply(_normalize_name): [ {cleaned_series_apply.iloc[0]} ]")
        if '㈱' not in cleaned_series_apply.iloc[0]:
            print("SUCCESS: .apply(_normalize_name) correctly removed the character.")
        else:
            # この場合、関数内のreplaceが機能していないことになる
            print("FAILURE: .apply(_normalize_name) FAILED to remove the character.")
    except Exception as e:
        print(f"ERROR during .apply(): {e}")

    print("\n--- Verification finished ---")
