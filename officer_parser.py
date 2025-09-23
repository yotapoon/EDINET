
'''
役員の状況に関するXBRLデータを解析するモジュール。
'''

import pandas as pd
import numpy as np

def parse_officer_information(df: pd.DataFrame) -> pd.DataFrame:
    """
    役員の状況に関するXBRL CSVデータを解析し、整形されたDataFrameを返す。

    Args:
        df (pd.DataFrame): XBRLをCSVに変換したDataFrame。

    Returns:
        pd.DataFrame: 整形後の役員情報データ。
    """
    # --- 1. メタデータ抽出 ---
    meta_map = {
        'jpcrp_cor:FilingDateCoverPage': 'SubmissionDate',
        'jpdei_cor:CurrentPeriodEndDateDEI': 'FiscalPeriodEnd',
        'jpdei_cor:SecurityCodeDEI': 'SecuritiesCode'
    }
    metadata = {new_name: df.loc[df['要素ID'] == old_name, '値'].iloc[0] if not df[df['要素ID'] == old_name].empty else None
                for old_name, new_name in meta_map.items()}

    # --- 2. 役員関連のデータに絞り込み ---
    officer_df = df[df['要素ID'].str.contains(
        '(InformationAboutDirectorsAndCorporateAuditors|RemunerationEtcPaidByGroupToEachDirectorOrOtherOfficer)',
        na=False, regex=True
    )].copy()

    if officer_df.empty:
        return pd.DataFrame()

    # --- 3. 項目タイプと新任フラグを特定 ---
    officer_df['normalized_element_id'] = officer_df['要素ID'].str.replace('Proposal', '', regex=False)

    conditions_type = [
        officer_df['normalized_element_id'].str.contains('NameInformationAboutDirectorsAndCorporateAuditors'),
        officer_df['normalized_element_id'].str.contains('DateOfBirthInformationAboutDirectorsAndCorporateAuditors'),
        officer_df['normalized_element_id'].str.contains('OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors'),
        officer_df['normalized_element_id'].str.contains('CareerSummaryInformationAboutDirectorsAndCorporateAuditorsTextBlock'),
        officer_df['normalized_element_id'].str.contains('NumberOfSharesHeldOrdinarySharesInformationAboutDirectorsAndCorporateAuditors'),
        officer_df['normalized_element_id'].str.contains('TermOfOfficeInformationAboutDirectorsAndCorporateAuditors'),
        officer_df['normalized_element_id'].str.contains('TotalAmountOfRemunerationEtcPaidByGroupRemunerationEtcPaidByGroupToEachDirectorOrOtherOfficer')
    ]
    choices_type = [
        'Name',
        'DateOfBirth',
        'Title',
        'CareerSummary',
        'NumberOfSharesHeld',
        'TermOfOffice',
        'TotalRemuneration'
    ]
    officer_df['item_type'] = np.select(conditions_type, choices_type, default=None)
    officer_df['IsNewAppointment'] = officer_df['要素ID'].str.contains('Proposal', na=False)

    # --- 4. 役員IDの抽出と不要データの削除 ---
    # コンテキストIDのプレフィックスの違いを吸収するため、役員固有のMember IDを抽出
    officer_df['OfficerId'] = officer_df['コンテキストID'].str.extract(r'(jpcrp.*Member)')
    
    # 役員IDが取得できなかった行や、項目タイプが不明な行は削除
    officer_df.dropna(subset=['OfficerId', 'item_type'], inplace=True)

    if officer_df.empty:
        return pd.DataFrame()

    # --- 5. PIVOTと集約で横持ちデータを作成 ---
    # 役員ごとの基本情報をPIVOT (indexをOfficerIdに変更)
    pivot_df = officer_df.pivot_table(
        index='OfficerId',
        columns='item_type',
        values='値',
        aggfunc='first'
    )

    # 役員ごとに新任フラグを集約 (groupbyのキーをOfficerIdに変更)
    new_appointment_flags = officer_df.groupby('OfficerId')['IsNewAppointment'].any()

    # PIVOT結果と新任フラグをマージ (mergeのキーをOfficerIdに変更)
    pivot_df = pivot_df.merge(new_appointment_flags, on='OfficerId', how='left').reset_index()

    # --- 6. メタデータを結合 ---
    for key, value in metadata.items():
        pivot_df[key] = value

    # --- 7. データ型を変換 ---
    numeric_cols = ['NumberOfSharesHeld', 'TotalRemuneration']
    for col in numeric_cols:
        if col in pivot_df.columns:
            pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce')

    if 'DateOfBirth' in pivot_df.columns:
        pivot_df['DateOfBirth'] = pd.to_datetime(pivot_df['DateOfBirth'], errors='coerce').dt.date

    # --- 8. カラムの順序を整える ---
    ordered_columns = [
        'SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'Name', 'IsNewAppointment', 'DateOfBirth',
        'Title', 'NumberOfSharesHeld', 'TotalRemuneration', 'TermOfOffice', 'CareerSummary'
    ]
    
    final_ordered_columns = [col for col in ordered_columns if col in pivot_df.columns]
    final_df = pivot_df[final_ordered_columns]

    return final_df

if __name__ == "__main__":
    try:
        # テスト用のCSVファイルを指定 (MS&ADの例)
        csv_path = r".\data\S100W0ZR\jpcrp030000-asr-001_E03854-000_2025-03-31_01_2025-06-20.csv"
        
        # ファイルを読み込み
        raw_df = pd.read_csv(csv_path, encoding="utf-16", sep='\t', engine='python', on_bad_lines='warn')
        
        # 関数を実行してデータを整形
        parsed_df = parse_officer_information(raw_df)
        
        # 結果を表示
        print("--- Parsed Officer Information ---")
        if parsed_df.empty:
            print("No officer information found.")
        else:
            print(parsed_df.to_string())

        # クリップボードにコピー（必要な場合）
        parsed_df.to_clipboard(index=False, na_rep="=na()")
        print("\nData copied to clipboard.")

    except FileNotFoundError:
        print(f"Error: Test file not found at {csv_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
