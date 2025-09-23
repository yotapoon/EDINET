
'''
議決権の状況（株式数）に関するXBRLデータを解析するモジュール。
'''

import pandas as pd

def parse_voting_rights(df: pd.DataFrame) -> pd.DataFrame:
    """
    議決権の状況（株式数）に関するXBRL CSVデータを解析し、整形されたDataFrameを返す。

    Args:
        df (pd.DataFrame): XBRLをCSVに変換したDataFrame。

    Returns:
        pd.DataFrame: 整形後の議決権情報データ。
    """
    # --- 1. メタデータ抽出 ---
    meta_map = {
        'jpcrp_cor:FilingDateCoverPage': 'SubmissionDate',
        'jpdei_cor:CurrentPeriodEndDateDEI': 'FiscalPeriodEnd',
        'jpdei_cor:SecurityCodeDEI': 'SecuritiesCode'
    }
    metadata = {new_name: df.loc[df['要素ID'] == old_name, '値'].iloc[0] if not df[df['要素ID'] == old_name].empty else None
                for old_name, new_name in meta_map.items()}

    # --- 2. 株式数関連のデータに絞り込み ---
    shares_df = df[df['要素ID'] == 'jpcrp_cor:NumberOfSharesIssuedSharesVotingRights'].copy()

    if shares_df.empty:
        return pd.DataFrame()

    # --- 3. コンテキストIDに基づいて各株式数を抽出 ---
    context_to_column_map = {
        'CurrentYearInstant': 'TotalNumberOfIssuedShares',
        'CurrentYearInstant_OrdinarySharesSharesWithFullVotingRightsOtherMember': 'NumberOfOtherSharesWithFullVotingRights',
        'CurrentYearInstant_OrdinarySharesTreasurySharesSharesWithFullVotingRightsTreasurySharesEtcMember': 'NumberOfTreasurySharesWithFullVotingRights',
        'CurrentYearInstant_OrdinarySharesSharesLessThanOneUnitMember': 'NumberOfSharesLessThanOneUnit'
    }

    shares_data = {}
    for context_id, column_name in context_to_column_map.items():
        row = shares_df[shares_df['コンテキストID'] == context_id]
        if not row.empty:
            value = row['値'].iloc[0]
            shares_data[column_name] = value
        else:
            shares_data[column_name] = None
    
    # --- 4. 1行のDataFrameを作成 ---
    final_data = {**metadata, **shares_data}
    final_df = pd.DataFrame([final_data])

    # --- 5. データ型を変換 ---
    numeric_cols = [
        'TotalNumberOfIssuedShares',
        'NumberOfOtherSharesWithFullVotingRights',
        'NumberOfTreasurySharesWithFullVotingRights',
        'NumberOfSharesLessThanOneUnit'
    ]
    
    for col in numeric_cols:
        if col in final_df.columns:
            # カンマを削除してから数値に変換
            final_df[col] = final_df[col].astype(str).str.replace(',', '', regex=False)
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

    # --- 6. カラムの順序を整える ---
    ordered_columns = [
        'SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode',
        'TotalNumberOfIssuedShares', 'NumberOfOtherSharesWithFullVotingRights',
        'NumberOfTreasurySharesWithFullVotingRights', 'NumberOfSharesLessThanOneUnit'
    ]
    
    final_ordered_columns = [col for col in ordered_columns if col in final_df.columns]
    final_df = final_df[final_ordered_columns]

    return final_df

if __name__ == "__main__":
    try:
        # テスト用のCSVファイルを指定 (MS&ADの例)
        csv_path = r".\data\S100W0ZR\jpcrp030000-asr-001_E03854-000_2025-03-31_01_2025-06-20.csv"
        
        # ファイルを読み込み
        raw_df = pd.read_csv(csv_path, encoding="utf-16", sep='\t', engine='python', on_bad_lines='warn')
        
        # 関数を実行してデータを整形
        parsed_df = parse_voting_rights(raw_df)
        
        # 結果を表示
        print("--- Parsed Voting Rights Information ---")
        if parsed_df.empty:
            print("No voting rights information found.")
        else:
            print(parsed_df.to_string())

        # クリップボードにコピー（必要な場合）
        parsed_df.to_clipboard(index=False, na_rep="=na()")
        print("\nData copied to clipboard.")

    except FileNotFoundError:
        print(f"Error: Test file not found at {csv_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
