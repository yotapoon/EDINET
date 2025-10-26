"""
名寄せの具体的なロジックを担うモジュール
"""
import pandas as pd
import re
import zenhan
import html
import database_manager # インポートを追加
from rapidfuzz import process, fuzz
from tqdm import tqdm

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

    # 新旧漢字・カナのバリエーションを吸収
    name = name.replace('氣', '気')
    name = name.replace('條', '条')
    name = name.replace('ヱ', 'エ')

    # 1. 除外対象のキーワードをチェック
    exclusion_keywords = ['持株会', '従業員持株会', '取引先持株会', '医療法人', '信託銀行']
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
    name = re.sub(r'\(.*?\)|（.*?）', '', name)
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
    
    # 1. DBから元データを取得
    raw_master_df = database_manager.get_name_code_master_data()
    if raw_master_df.empty:
        print("Warning: Could not retrieve data for name master.")
        return pd.DataFrame()

    # 2. edinetCodeがない、またはfilerNameが文字列でない行を除外
    raw_master_df.dropna(subset=['edinetCode'], inplace=True)
    raw_master_df = raw_master_df[raw_master_df['filerName'].apply(isinstance, args=(str,))]

    # 3. 正規化された名前カラムを追加
    raw_master_df['normalizedName'] = raw_master_df['filerName'].apply(_normalize_name)

    # 4. 過去の名称も利用できるように、edinetCodeでの重複排除を緩める
    # まず、正規化名とedinetCodeの組み合わせで重複を削除
    master_df = raw_master_df.drop_duplicates(subset=['normalizedName', 'edinetCode']).copy()

    # 同じ正規化名が複数のedinetCodeに紐づく場合、secCodeを持つ方を優先する
    master_df.sort_values(by='secCode', ascending=False, na_position='last', inplace=True)
    master_df.drop_duplicates(subset=['normalizedName'], keep='first', inplace=True)

    # 5. 最終的なマスターを作成 (normalizedName -> edinetCode, secCode)
    master_map = master_df.set_index('normalizedName')[['edinetCode', 'secCode']]
    
    print(f"Finished creating name-code master list. {len(master_map)} unique names found.")
    return master_map

def match_names(names_to_match: pd.Series, master: pd.DataFrame, score_cutoff: int = 85) -> pd.DataFrame:
    """
    与えられた名称のリストをマスターと照合し、EDINETコードなどを返す (最終ハイブリッド戦略)。
    1. 全ての名称を正規化する。
    2. 正規化後の名称をキーとして手動マッピング辞書を適用し、処理対象の名称を決定する。
    3. 処理対象の名称に対して、完全一致・あいまい検索の自動処理を適用する。
    """
    print(f"--- Starting Hybrid Matching Process for {len(names_to_match)} total records ---")

    # --- 1. 手動マッピング辞書の読み込み ---
    try:
        manual_map_df = pd.read_csv('mapping.csv', dtype=str)
        # キーをnormalized_nameに変更
        correction_dict = pd.Series(manual_map_df.correct_name.values, index=manual_map_df.normalized_name).to_dict()
        print(f"Loaded {len(correction_dict)} entries from manual mapping file.")
    except FileNotFoundError:
        correction_dict = {}
        print("Warning: mapping.csv not found. Proceeding without manual mapping.")

    # --- 2. 名称の正規化と手動マッピングの適用 ---
    # ユニークなオリジナル名で処理を進める
    process_df = pd.DataFrame({'originalName': names_to_match.dropna().unique()})
    # 全ての名称をまず正規化
    process_df['normalizedName'] = process_df['originalName'].apply(_normalize_name)
    # 正規化後の名称に手動マッピングを適用した結果を、そのままルックアップキーとして使用
    process_df['lookupKey'] = process_df['normalizedName'].map(correction_dict).fillna(process_df['normalizedName'])

    # --- 3. 自動マッチングの実行 ---
    unique_lookup_keys = process_df['lookupKey'].unique()
    print(f"Performing automatic matching for {len(unique_lookup_keys)} unique lookup keys...")

    # 完全一致
    results_df = master.reindex(unique_lookup_keys)
    results_df.rename(columns={'edinetCode': 'matchedEdinetCode', 'secCode': 'matchedSecCode'}, inplace=True)
    results_df.index.name = 'lookupKey'
    results_df.reset_index(inplace=True)

    # --- 3-2. 「ホールディングス」サフィックス検索 ---
    # 完全一致でマッチしなかったもののうち、ホールディングス系の略称である可能性を考慮
    unmatched_for_hd_check = results_df[results_df['matchedEdinetCode'].isnull()]
    if not unmatched_for_hd_check.empty:
        print(f"Performing Holdings suffix check for {len(unmatched_for_hd_check)} unmatched records...")
        
        master_keys = master.index
        
        # ホールディングスの派生パターンを定義
        hd_suffixes = [
            'ホールディングス',
            'グループホールディングス',
            'フィナンシャルホールディングス',
            'グローバルホールディングス',
            'hd',
            'hds',
            'ghd',
            'fhd'
        ]
        
        # 見つかったマッチを一時的に保存する辞書
        hd_matches = {}

        # マッチしなかったlookupKeyごとにループ
        for index, row in unmatched_for_hd_check.iterrows():
            lookup_key = row['lookupKey']
            if not lookup_key or not isinstance(lookup_key, str):
                continue

            # 派生パターンを試す
            potential_matches = []
            for suffix in hd_suffixes:
                potential_key = lookup_key + suffix
                if potential_key in master_keys:
                    potential_matches.append(potential_key)
            
            # ユニークなマッチが1件だけ見つかった場合のみ採用
            if len(potential_matches) == 1:
                matched_key = potential_matches[0]
                matched_codes = master.loc[matched_key]
                hd_matches[index] = {
                    'matchedEdinetCode': matched_codes['edinetCode'],
                    'matchedSecCode': matched_codes['secCode']
                }

        # 見つかったマッチをresults_dfに反映
        if hd_matches:
            print(f"Found {len(hd_matches)} matches via Holdings suffix check.")
            for idx, match_data in hd_matches.items():
                results_df.loc[idx, ['matchedEdinetCode', 'matchedSecCode']] = match_data.values()

    # あいまい検索 (ホールディングス検索後)
    unmatched_df = results_df[results_df['matchedEdinetCode'].isnull()] # 再度未マッチを取得
    if not unmatched_df.empty:
        print(f"{len(unmatched_df)} names still unmatched. Applying fuzzy matching...")
        master_choices = master.index.tolist()
        fuzzy_matches = {}
        for index, row in tqdm(unmatched_df.iterrows(), total=unmatched_df.shape[0], desc="Fuzzy Matching"):
            lookup_key = row['lookupKey']
            if not lookup_key: continue
            
            best_candidate = process.extractOne(lookup_key, master_choices, scorer=fuzz.token_set_ratio)
            if best_candidate and best_candidate[1] >= score_cutoff:
                candidate_name = best_candidate[0]
                matched_codes = master.loc[candidate_name]
                fuzzy_matches[index] = {
                    'matchedEdinetCode': matched_codes['edinetCode'],
                    'matchedSecCode': matched_codes['secCode']
                }
        
        for idx, match_data in fuzzy_matches.items():
            results_df.loc[idx, ['matchedEdinetCode', 'matchedSecCode']] = match_data.values()

    # --- 4. 結果の結合 ---
    # process_df (originalName <-> lookupKey) に自動マッチング結果を結合
    final_process_df = pd.merge(process_df, results_df[['lookupKey', 'matchedEdinetCode', 'matchedSecCode']], on='lookupKey', how='left')

    print("--- Debug: final_process_df for '㈱高島屋' ---")
    print(final_process_df[final_process_df['originalName'] == '㈱高島屋'])
    print("-------------------------------------------------")

    # 最終的に、元の（重複ありの）リストに結果をマージして返す
    final_df = pd.merge(names_to_match.to_frame('originalName'), final_process_df, on='originalName', how='left')

    matched_count = final_df['matchedEdinetCode'].notna().sum()
    print(f"--- Finished Matching. Total matched: {matched_count} of {len(names_to_match)} records. ---")
    
    return final_df[['originalName', 'matchedEdinetCode', 'matchedSecCode']]
