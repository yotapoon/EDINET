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
    name = re.sub(r'\(.*?\)|（.*?）', '', name)
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

def match_names(names_to_match: pd.Series, master: pd.DataFrame, score_cutoff: int = 85) -> pd.DataFrame:
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
            match = process.extractOne(normalized_name, master_choices, scorer=fuzz.token_set_ratio, score_cutoff=score_cutoff)
            if match:
                matched_codes = master.loc[match[0]]
                fuzzy_matches[index] = {'matchedEdinetCode': matched_codes['edinetCode'], 'matchedSecCode': matched_codes['secCode']}
        for index, match_data in fuzzy_matches.items():
            results_df.loc[index, 'matchedEdinetCode'] = match_data['matchedEdinetCode']
            results_df.loc[index, 'matchedSecCode'] = match_data['matchedSecCode']
    final_results = results_df[['originalName', 'matchedEdinetCode', 'matchedSecCode']]
    matched_count = final_results['matchedEdinetCode'].notna().sum()
    print(f"Finished matching names. {matched_count} of {len(names_to_match)} names were matched.")
    return final_results

if __name__ == "__main__":
    print("--- Analyzing all unmatched names from SpecifiedInvestment ---")
    master_df = create_name_code_master()
    if master_df.empty:
        exit()
    source_df = database_manager.get_data_for_enrichment("SpecifiedInvestment", "NameOfSecurities")
    if source_df.empty:
        exit()

    print("\n[Info] Pre-cleaning data...")
    name_map_df = pd.DataFrame({'originalName': source_df["NameOfSecurities"].dropna()})
    name_map_df['cleanedName'] = name_map_df['originalName'].astype(str).str.replace('㈱', '', regex=False)
    name_map_df['cleanedName'] = name_map_df['cleanedName'].str.replace('（株）', '', regex=False)
    name_map_df['cleanedName'] = name_map_df['cleanedName'].str.replace('(株)', '', regex=False)

    unique_cleaned_names = name_map_df['cleanedName'].unique()
    unique_names_series = pd.Series(unique_cleaned_names)
        
    print(f"\n[Info] Checking matching for {len(unique_names_series)} unique names...")

    matched_df = match_names(unique_names_series, master_df, score_cutoff=80)

    unmatched_df = matched_df[matched_df['matchedEdinetCode'].isnull()]
    unmatched_cleaned_names = unmatched_df['originalName']
    unmatched_original_names = name_map_df[name_map_df['cleanedName'].isin(unmatched_cleaned_names)]['originalName']

    output_path = "final_unmatched_list.csv"
    unmatched_original_names.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"\n[Result] Saved {len(unmatched_original_names)} unmatched names to {output_path}")
    print("\n--- Analysis finished ---")
