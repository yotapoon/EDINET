import pandas as pd
import database_manager
import re
import html
import zenhan
from rapidfuzz import process, fuzz
from tqdm import tqdm

# --- Functions from matching.py embedded directly ---
def _normalize_name(name: str) -> str:
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

def create_name_code_master() -> pd.DataFrame:
    print("Creating name-code master list...")
    raw_master_df = database_manager.get_name_code_master_data()
    if raw_master_df.empty:
        return pd.DataFrame()
    raw_master_df.dropna(subset=['edinetCode'], inplace=True)
    raw_master_df = raw_master_df[raw_master_df['filerName'].apply(isinstance, args=(str,))]
    raw_master_df['normalizedName'] = raw_master_df['filerName'].apply(_normalize_name)
    raw_master_df.sort_values(by='secCode', ascending=False, na_position='last', inplace=True)
    master_df = raw_master_df.drop_duplicates(subset=['edinetCode'], keep='first').drop_duplicates(subset=['normalizedName'], keep='first')
    master_map = master_df.set_index('normalizedName')[['edinetCode', 'secCode']]
    print(f"Finished creating name-code master list. {len(master_map)} unique names found.")
    return master_map

if __name__ == "__main__":
    print("--- Forensic Analysis of Normalization Failure ---")

    # 1. マスターと元データをロード
    master_df = create_name_code_master()
    source_df = database_manager.get_data_for_enrichment("SpecifiedInvestment", "NameOfSecurities")
    print(f"\nLoaded {len(source_df)} total records.")

    # 2. 問題の文字列を定義
    target_original_name = '凸版印刷㈱'
    print(f"\nTarget String: '{target_original_name}'")
    print("-" * 50)

    # 3. ステップ・バイ・ステップで単体の文字列を処理
    print("[Step A] Applying direct .replace() to the string object:")
    step_a_result = target_original_name.replace('㈱', '')
    print(f"Result: '{step_a_result}'")
    if step_a_result == '凸版印刷':
        print("SUCCESS: Python's native string replace works.")
    else:
        print("FAILURE: Python's native string replace failed.")
        exit()
    print("-" * 50)

    print("[Step B] Applying the full _normalize_name() function to the string object:")
    step_b_result = _normalize_name(target_original_name)
    print(f"Result: '{step_b_result}'")
    if step_b_result == '凸版印刷':
        print("SUCCESS: _normalize_name() function works correctly on the string.")
    else:
        print("FAILURE: _normalize_name() function failed on the string.")
        exit()
    print("-" * 50)

    print("[Step C] Checking if the perfectly normalized name exists in the master data:")
    master_keys = master_df.index
    if step_b_result in master_keys:
        print(f"SUCCESS: The key '{step_b_result}' exists in the master data index.")
    else:
        # マスター側に問題がある可能性
        print(f"FAILURE: The key '{step_b_result}' does NOT exist in the master data index.")
        # マスター側の正規化がどうなっているか確認
        master_source_name = '凸版印刷株式会社'
        master_normalized = _normalize_name(master_source_name)
        print(f"For comparison, normalizing '{master_source_name}' results in '{master_normalized}'.")
    print("-" * 50)

    # 4. 全件処理のパイプラインをシミュレート
    print("[Step D] Simulating the full pipeline as in the analysis script:")
    # 4a. Pandas Seriesを作成
    all_names_series = source_df["NameOfSecurities"].dropna()
    # 4b. .apply()を使って全件を正規化
    print("Applying _normalize_name to the entire Pandas Series...")
    normalized_series = all_names_series.apply(_normalize_name)
    # 4c. 対象の文字列がどうなったかを確認
    original_series_subset = all_names_series[all_names_series == target_original_name]
    if not original_series_subset.empty:
        target_index = original_series_subset.index[0]
        result_in_series = normalized_series.loc[target_index]
        print(f"Result for '{target_original_name}' after full .apply() is: '{result_in_series}'")
        if result_in_series == '凸版印刷':
            print("SUCCESS: The .apply() method correctly normalized the string within the full series.")
        else:
            print("FAILURE: The .apply() method FAILED to normalize the string correctly within the full series.")
    else:
        print("Could not find target string in the series for this step.")

    print("\n--- Forensic analysis finished ---")
