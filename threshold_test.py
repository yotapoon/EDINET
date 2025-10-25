import pandas as pd
import database_manager
import re
import html
import zenhan

# --- Function from matching.py embedded directly ---
def _normalize_name(name: str) -> str:
    """企業名・株主名の表記揺れを吸収するための正規化処理"""
    if not isinstance(name, str):
        return ""
    name = name.strip()
    name = name.replace('㈱', '')
    name = name.replace('（株）', '')
    name = name.replace('(株)', '')
    name = html.unescape(name)
    name = name.replace('－', '-')
    exclusion_keywords = ['持株会', '従業員持株会', '取引先持株会', '医療法人']
    if any(keyword in name for keyword in exclusion_keywords):
        return ""
    agent_match = re.search(r'(?:常任代理人|常任代理人：)\s*([^)）]*)', name)
    if agent_match:
        name = agent_match.group(1).strip()
    else:
        agent_match = re.search(r'（常任代理人\s*(.*?)）', name)
        if agent_match:
            name = agent_match.group(1).strip()
    name = re.sub(r'\(.*\)|（.*?）', '', name)
    name = re.sub(r'優先株式', '', name)
    name = re.sub(r'第.種', '', name)
    name = zenhan.z2h(name, mode=zenhan.ASCII)
    name = zenhan.z2h(name, mode=zenhan.DIGIT)
    name = zenhan.h2z(name, mode=zenhan.KANA)
    corp_types = r'株式会社|合同会社|有限会社|合資会社|合名会社'
    name = re.sub(corp_types, '', name)
    name = re.sub(r'[・\s\u3000,.]', '', name).lower().strip()
    return name

if __name__ == "__main__":
    print("--- Threshold Test for Normalization Failure ---")

    # 1. データベースから全データを取得
    source_df = database_manager.get_data_for_enrichment("SpecifiedInvestment", "NameOfSecurities")
    if source_df.empty:
        print("Database returned no data. Aborting.")
        exit()
    print(f"\nLoaded {len(source_df)} total records.")

    # 2. テストするチャンクサイズを定義
    chunk_sizes = [100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, len(source_df)]
    problem_string = '凸版印刷㈱'
    
    print(f"\nTarget string to test: '{problem_string}'\n")

    # 3. チャンクサイズを増やしながらテスト
    for size in chunk_sizes:
        if size > len(source_df):
            size = len(source_df)
        
        print(f"--- Testing with chunk size: {size} rows ---")
        chunk_df = source_df.head(size)

        # チャンク内にテスト対象文字列が存在するか確認
        test_series = chunk_df[chunk_df['NameOfSecurities'] == problem_string]['NameOfSecurities']
        if test_series.empty:
            print("Test string not found in this chunk. Skipping.")
            continue

        # .apply() を実行
        try:
            cleaned_series = test_series.apply(_normalize_name)
            result_string = cleaned_series.iloc[0]
            
            print(f"Original: '{test_series.iloc[0]}' -> Normalized: '{result_string}'")
            if '㈱' not in result_string:
                print("SUCCESS: Normalization worked as expected.")
            else:
                print("FAILURE: Normalization FAILED at this chunk size.")
                break # 失敗した時点でループを抜ける
        except Exception as e:
            print(f"ERROR during .apply() at chunk size {size}: {e}")
            break
        print("-" * 40)

    print("\n--- Threshold test finished ---")
