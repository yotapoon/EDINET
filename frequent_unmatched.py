import pandas as pd
import database_manager
import re
import html
import zenhan
from rapidfuzz import process, fuzz
from tqdm import tqdm

# --- Functions from matching.py embedded directly ---

def _normalize_name(name: str) -> str:
    """企業名・株主名の表記揺れを吸収するための正規化処理"""
    if not isinstance(name, str):
        return ""

    # 0. 前処理 (最も頻出するパターンを直接除去)
    name = name.strip()
    name = name.replace('㈱', '')
    name = name.replace('（株）', '')
    name = name.replace('(株)', '')
    name = html.unescape(name)
    name = name.replace('－', '-')

    # 1. 除外対象のキーワードをチェック
    exclusion_keywords = ['持株会', '従業員持株会', '取引先持株会', '医療法人']
    if any(keyword in name for keyword in exclusion_keywords):
        return ""

    # 2. 常任代理人パターンの抽出
    agent_match = re.search(r'(?:常任代理人|常任代理人：)\s*([^)）]*)', name)
    if agent_match:
        name = agent_match.group(1).strip()
    else:
        agent_match = re.search(r'（常任代理人\s*(.*?)）', name)
        if agent_match:
            name = agent_match.group(1).strip()

    # 3. 不要な情報を除去
    name = re.sub(r'\(.*\)|（.*?）', '', name)
    name = re.sub(r'優先株式', '', name)
    name = re.sub(r'第.種', '', name)

    # 4. 全角・半角を統一
    name = zenhan.z2h(name, mode=zenhan.ASCII)
    name = zenhan.z2h(name, mode=zenhan.DIGIT)
    name = zenhan.h2z(name, mode=zenhan.KANA)

    # 5. 法人種別などを削除 (残りのパターン)
    corp_types = r'株式会社|合同会社|有限会社|合資会社|合名会社'
    name = re.sub(corp_types, '', name)

    # 6. 記号や空白を削除し、小文字に変換
    name = re.sub(r'[・\s\u3000,.]', '', name).lower().strip()
    
    return name

def create_name_code_master() -> pd.DataFrame:
    """
    DocumentMetadataテーブルから、名寄せのマスターデータを作成する。
    """
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

def match_names(names_to_match: pd.Series, master: pd.DataFrame, score_cutoff: int = 85) -> pd.DataFrame:
    """
    与えられた名称のリストをマスターと照合し、EDINETコードなどを返す (最終ハイブリッド戦略)。
    """
    print(f"Matching {len(names_to_match)} names using hybrid (reindex + fuzzy token_set_ratio) with cutoff {score_cutoff}...")
    normalized_names = names_to_match.apply(_normalize_name)
    results_df = master.reindex(normalized_names)
    results_df.rename(columns={'edinetCode': 'matchedEdinetCode', 'secCode': 'matchedSecCode'}, inplace=True)
    results_df['originalName'] = names_to_match.values
    results_df.reset_index(inplace=True)

    unmatched_df = results_df[results_df['matchedEdinetCode'].isnull()]
    print(f"{len(unmatched_df)} names did not have an exact match. Applying fuzzy matching...")

    if not unmatched_df.empty:
        master_choices = master.index.tolist()
        fuzzy_matches = {}
        for index, row in tqdm(unmatched_df.iterrows(), total=unmatched_df.shape[0], desc="Fuzzy Matching"):
            normalized_name = row['index']
            if not normalized_name:
                continue
            match = process.extractOne(
                normalized_name, 
                master_choices, 
                scorer=fuzz.token_set_ratio, 
                score_cutoff=score_cutoff
            )
            if match:
                matched_codes = master.loc[match[0]]
                fuzzy_matches[index] = {
                    'matchedEdinetCode': matched_codes['edinetCode'],
                    'matchedSecCode': matched_codes['secCode']
                }
        for index, match_data in fuzzy_matches.items():
            results_df.loc[index, 'matchedEdinetCode'] = match_data['matchedEdinetCode']
            results_df.loc[index, 'matchedSecCode'] = match_data['matchedSecCode']

    final_results = results_df[['originalName', 'matchedEdinetCode', 'matchedSecCode']]
    matched_count = final_results['matchedEdinetCode'].notna().sum()
    print(f"Finished matching names. {matched_count} of {len(names_to_match)} names were matched.")
    return final_results

# --- Main execution logic ---

if __name__ == "__main__":
    print("--- Analyzing top 50 frequent unmatched names from SpecifiedInvestment (Self-Contained) ---")
    pd.set_option('display.max_rows', 100)

    master_df = create_name_code_master()
    if master_df.empty:
        print("\n[Error] Master data is empty. Aborting.")
        exit()

    source_df = database_manager.get_data_for_enrichment("SpecifiedInvestment", "NameOfSecurities")
    if source_df.empty:
        print("\n[Info] No data found in SpecifiedInvestment to check.")
        exit()

    # --- Pre-computation Step ---
    print("\n[Info] Pre-cleaning data...")
    # オリジナル名とクリーン名の対応を保持するDataFrameを作成
    name_map_df = pd.DataFrame({
        'originalName': source_df["NameOfSecurities"].dropna()
    })
    # .astype(str) を挟むことで、非文字列データが原因のエラーを回避
    name_map_df['cleanedName'] = name_map_df['originalName'].astype(str).str.replace('㈱', '', regex=False)
    name_map_df['cleanedName'] = name_map_df['cleanedName'].str.replace('（株）', '', regex=False)
    name_map_df['cleanedName'] = name_map_df['cleanedName'].str.replace('(株)', '', regex=False)

    unique_cleaned_names = name_map_df['cleanedName'].unique()
    unique_names_series = pd.Series(unique_cleaned_names)
        
    print(f"\n[Info] Checking matching for {len(unique_names_series)} unique names...")

    # 4. 名寄せを実行 (対象はクリーンな名前)
    matched_df = match_names(unique_names_series, master_df, score_cutoff=80)

    # 5. マッチングしなかったクリーンな名前を抽出
    unmatched_cleaned_names = matched_df[matched_df['matchedEdinetCode'].isnull()]['originalName']

    # 6. マッチしなかったクリーンな名前に該当する、オリジナルの名前を取得
    unmatched_original_names = name_map_df[name_map_df['cleanedName'].isin(unmatched_cleaned_names)]['originalName']

    # 7. オリジナルの名前で出現回数をカウント
    original_counts = unmatched_original_names.value_counts()

    print("\n--- Top 100 Frequent Unmatched Names (Corrected Count) ---")
    print(original_counts.head(100).to_string())
    
    print("\n--- Analysis finished ---")