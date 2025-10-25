import pandas as pd
import database_manager

# 比較対象とする、コード上で定義した「正しい」文字
KNOWN_GOOD_CHAR = '㈱'

def get_char_codes(s: str):
    """文字列内の各文字のUnicodeコードポイントを返す"""
    return [f"{char} (U+{ord(char):04X})" for char in s]

if __name__ == "__main__":
    print("--- Character Code Diagnosis ---")

    # 1. データベースからデータを取得
    source_df = database_manager.get_data_for_enrichment("SpecifiedInvestment", "NameOfSecurities")
    if source_df.empty:
        print("Database returned no data. Aborting.")
        exit()

    # 2. 問題の文字列を特定
    problem_string = None
    # 完全一致で文字列を探す
    for name in source_df["NameOfSecurities"].dropna().unique():
        if '凸版印刷㈱' in name and len(name) == 5: # 完全一致するものを探す
            problem_string = name
            break

    if not problem_string:
        print("Could not find the exact string '凸版印刷㈱' in the dataset.")
        exit()

    # 3. 文字コードを分析・表示
    print(f"\nAnalyzing string: '{problem_string}'")
    print("-" * 30)
    
    db_chars = get_char_codes(problem_string)
    print(f"Characters from DB: {db_chars}")

    # 4. コード上の文字と比較
    print("-" * 30)
    print(f"Analyzing known good character: '{KNOWN_GOOD_CHAR}'")
    known_good_code = f"U+{ord(KNOWN_GOOD_CHAR):04X}"
    print(f"Code from script: {KNOWN_GOOD_CHAR} ({known_good_code})")
    print("-" * 30)

    # 5. 結論
    db_kabu_char_code = get_char_codes(problem_string)[-1]
    if known_good_code in db_kabu_char_code:
        print("\nConclusion: The character codes MATCH. The issue is not encoding.")
    else:
        print(f"\nConclusion: The character codes DO NOT MATCH.")
        print(f"- DB Character: {db_kabu_char_code}")
        print(f"- Script Character: {KNOWN_GOOD_CHAR} ({known_good_code})")
        print("This confirms an encoding or data integrity issue.")

    print("\n--- Diagnosis finished ---")
