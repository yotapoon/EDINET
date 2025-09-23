import pandas as pd
import numpy as np

def parse_specified_investment(df: pd.DataFrame) -> pd.DataFrame:
    """
    特定投資有価証券のXBRL CSVデータを解析し、整形されたDataFrameを返す。
    SQLライクなpandas操作を用いて、必要な情報を抽出・整形する。
    """
    # --- 1. メタデータ抽出 ---
    meta_map = {
        'jpcrp_cor:FilingDateCoverPage': 'SubmissionDate',
        'jpdei_cor:CurrentPeriodEndDateDEI': 'FiscalPeriodEnd',
        'jpdei_cor:SecurityCodeDEI': 'SecuritiesCode'
    }
    metadata = {new_name: df.loc[df['要素ID'] == old_name, '値'].iloc[0] if not df[df['要素ID'] == old_name].empty else None
                for old_name, new_name in meta_map.items()}

    # --- 2. 投資株式関連のデータに絞り込み ---
    investment_df = df[df['要素ID'].str.contains('SpecifiedInvestment', na=False)].copy()
    if investment_df.empty:
        return pd.DataFrame()

    # --- 3. SQLのCASE文のように、項目名から「保有主体」と「項目タイプ」を特定するカラムを追加 ---
    def get_entity_and_type(item_name):
        if not isinstance(item_name, str):
            return None, None
        
        # 保有主体の特定
        if 'SecondLargestHoldingCompany' in item_name:
            entity = 'SecondLargestHoldingCompany'
        elif 'LargestHoldingCompany' in item_name:
            entity = 'LargestHoldingCompany'
        elif 'ReportingCompany' in item_name:
            entity = 'ReportingCompany'
        else:
            entity = None
            
        # 項目タイプの特定
        if 'NameOfSecurities' in item_name:
            item_type = 'NameOfSecurities'
        elif 'NumberOfSharesHeld' in item_name:
            item_type = 'NumberOfSharesHeld'
        elif 'BookValue' in item_name:
            item_type = 'BookValue'
        elif 'PurposeOfShareholdingOverviewOfBusinessAllianceQuantitativeEffectsOfShareholdingAndReasonForIncreaseInNumberOfShares' in item_name:
            item_type = 'HoldingPurpose'
        elif 'WhetherIssuerOfAforementionedSharesHoldsReportingCompanysShares' in item_name:
            item_type = 'CrossShareholdingStatus'
        else:
            item_type = None
            
        return entity, item_type

    investment_df[['HoldingEntity', 'item_type']] = investment_df['要素ID'].apply(
        lambda x: pd.Series(get_entity_and_type(x))
    )
    
    # --- 4. 行を特定するためのIDを追加し、不要なデータを削除 ---
    investment_df['row_id'] = investment_df['コンテキストID'].str.extract(r'_Row(\d+)')
    investment_df.dropna(subset=['HoldingEntity', 'row_id', 'item_type'], inplace=True)
    investment_df.to_clipboard()

    # --- 5. SQLのPIVOTのように、行と列を入れ替えて横持ちデータを作成 ---
    pivot_df = investment_df.pivot_table(
        index=['HoldingEntity', 'row_id'],
        columns=['item_type', '相対年度'],
        values='値',
        aggfunc='first'
    )

    # --- 6. カラム名を整形 ---
    pivot_df.columns = ['_'.join(filter(None, col)).strip() for col in pivot_df.columns.values]
    pivot_df.reset_index(inplace=True)

    # --- 7. 最終的なデータフレームを作成し、カラムを選択・リネーム ---
    column_mapping = {
        'HoldingEntity': 'HoldingEntity',
        'NameOfSecurities_当期末': 'NameOfSecurities',
        'NumberOfSharesHeld_当期末': 'NumberOfSharesHeldCurrentYear',
        'BookValue_当期末': 'BookValueCurrentYear',
        'NumberOfSharesHeld_前期末': 'NumberOfSharesHeldPriorYear',
        'BookValue_前期末': 'BookValuePriorYear',
        'HoldingPurpose_当期末': 'HoldingPurpose',
        'CrossShareholdingStatus_当期末': 'CrossShareholdingStatus'
    } 
    
    final_df = pd.DataFrame()
    for original, new in column_mapping.items():
        if original in pivot_df.columns:
            final_df[new] = pivot_df[original]
        else:
            final_df[new] = None # 元データに存在しない場合はNoneで埋める

    # --- 8. メタデータを結合し、不要な行を削除 ---
    for key, value in metadata.items():
        final_df[key] = value
    final_df.dropna(subset=['NameOfSecurities'], inplace=True)

    # --- 9. データ型を変換 ---
    numeric_cols = [
        'NumberOfSharesHeldCurrentYear', 'BookValueCurrentYear',
        'NumberOfSharesHeldPriorYear', 'BookValuePriorYear'
    ]
    for col in numeric_cols:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

    # --- 10. カラムの順序を整える ---
    ordered_columns = [
        'SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'HoldingEntity',
        'NameOfSecurities', 'NumberOfSharesHeldCurrentYear', 'BookValueCurrentYear',
        'NumberOfSharesHeldPriorYear', 'BookValuePriorYear', 'HoldingPurpose', 'CrossShareholdingStatus'
    ]
    # 存在しないカラムがあってもエラーにならないようにする
    final_df = final_df[[col for col in ordered_columns if col in final_df.columns]]

    return final_df

if __name__ == "__main__":
    try:
        # テスト用のCSVファイルを指定
        csv_path = r".\data\S100W0ZR\jpcrp030000-asr-001_E03854-000_2025-03-31_01_2025-06-20.csv"
        
        # ファイルを読み込み
        raw_df = pd.read_csv(csv_path, encoding="utf-16", sep='\t', engine='python', on_bad_lines='warn')
        
        # 関数を実行してデータを整形
        parsed_df = parse_specified_investment(raw_df)
        
        # 結果を表示
        print("---" + " Parsed Specified Investment Data" + " ---")
        print(parsed_df.to_string())
        
        # クリップボードにコピー（必要な場合）
        parsed_df.to_clipboard(index=False, na_rep="=na()")
        print("\nData copied to clipboard.")

    except FileNotFoundError:
        print(f"Error: Test file not found at {csv_path}")
    except Exception as e:
        print(f"An error occurred: {e}")