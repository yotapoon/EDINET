"""
名寄せの具体的なロジックを担うモジュール
"""
import pandas as pd
import re
import zenhan
import database_manager # インポートを追加

def _normalize_name(name: str) -> str:
    """企業名・株主名の表記揺れを吸収するための正規化処理"""
    if not isinstance(name, str):
        return ""
    # 1. 全角・半角を統一 (英数カナ)
    name = zenhan.z2h(name, mode=zenhan.ALL_KANA | zenhan.ALL_ASCII)
    name = zenhan.h2z(name, mode=zenhan.ALL_KANA)
    
    # 2. 法人種別などを削除 (株式会社, 合同会社, など)
    name = re.sub(r'[\(（]株[\)）]|株式会社|合同会社|有限会社|合資会社|合名会社', '', name)
    
    # 3. 空白を削除し、小文字に変換
    name = name.replace(' ', '').replace('\u3000', '').lower()
    
    return name

def create_name_code_master() -> pd.DataFrame:
    """
    DocumentMetadataテーブルから、名寄せのマスターデータを作成する
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

    # 4. secCodeがあるものを優先して、edinetCodeごとに代表名を選ぶ
    raw_master_df.sort_values(by='secCode', ascending=False, na_position='last', inplace=True)
    master_df = raw_master_df.drop_duplicates(subset=['edinetCode'], keep='first')

    # 5. 最終的なマスターを作成 (normalizedName -> edinetCode, secCode)
    master_map = master_df.set_index('normalizedName')[[ 'edinetCode', 'secCode']]
    
    print(f"Finished creating name-code master list. {len(master_map)} unique names found.")
    return master_map


def match_names(names_to_match: pd.Series, master: pd.DataFrame) -> pd.DataFrame:
    """
    与えられた名称のリストをマスターと照合し、EDINETコードなどを返す
    """
    print(f"Matching {len(names_to_match)} names...")
    
    # 1. マッチ対象の名称を正規化
    normalized_names = names_to_match.apply(_normalize_name)
    
    # 2. マスターのインデックスを使ってマッチング
    #    masterのインデックスは正規化済みの名称 (normalizedName)
    matched_data = master.reindex(normalized_names)
    
    # 3. 結果を整形
    results = pd.DataFrame({
        'originalName': names_to_match,
        'matchedEdinetCode': matched_data['edinetCode'].values,
        'matchedSecCode': matched_data['secCode'].values
    })
    
    # マッチした件数を報告
    matched_count = results['matchedEdinetCode'].notna().sum()
    print(f"Finished matching names. {matched_count} of {len(names_to_match)} names were matched.")
    
    return results
