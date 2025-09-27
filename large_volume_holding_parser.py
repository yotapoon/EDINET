import pandas as pd
import numpy as np
import re

def _extract_lvh_metadata(df: pd.DataFrame) -> dict:
    """大量保有報告書のCSVから基本的なメタデータを抽出する"""
    meta_map = {
        'jplvh_cor:FilingDateCoverPage': 'SubmissionDate',
        'jplvh_cor:NameOfIssuer': 'IssuerName',
        'jplvh_cor:SecurityCodeOfIssuer': 'IssuerSecuritiesCode',
        'jplvh_cor:DateWhenFilingRequirementAroseCoverPage': 'ReportObligationDate'
    }
    metadata = {}
    for old_name, new_name in meta_map.items():
        series = df.loc[df['要素ID'] == old_name, '値']
        metadata[new_name] = series.iloc[0] if not series.empty else None
    
    if metadata.get('IssuerSecuritiesCode'):
        match = re.search(r'\d{4,5}', str(metadata['IssuerSecuritiesCode']))
        if match:
            metadata['IssuerSecuritiesCode'] = match.group(0)
            
    return metadata

def _finalize_lvh_df(df: pd.DataFrame, metadata: dict, ordered_columns: list, numeric_cols: list = [], date_cols: list = []) -> pd.DataFrame:
    """メタデータの付与、データ型変換、カラム順序の整理を行う共通関数"""
    if df.empty:
        return pd.DataFrame(columns=ordered_columns)

    for key, value in metadata.items():
        df[key] = value

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].replace(['－', '-'], np.nan)
            df[col] = df[col].astype(str).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    for col in ordered_columns:
        if col not in df.columns:
            df[col] = None
    
    return df[ordered_columns]

def extract_large_volume_holding_data(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    大量保有報告書のDataFrameから提出者ごとの情報を抽出・整形して返す。
    """
    metadata = _extract_lvh_metadata(df)

    member_contexts = df[df['コンテキストID'].astype(str).str.contains("LargeVolumeHolder.Member")]["コンテキストID"].unique()

    if len(member_contexts) == 0:
        return None

    filer_data_list = []
    for context in member_contexts:
        ctx_df = df[df['コンテキストID'] == context]
        if ctx_df.empty:
            continue

        item_map = {
            'FilerName': 'jplvh_cor:Name',
            'FilerAddress': 'jplvh_cor:ResidentialAddressOrAddressOfRegisteredHeadquarter',
            'IndividualOrCorporation': 'jplvh_cor:IndividualOrCorporation',
            'Occupation': 'jplvh_cor:Occupation',
            'HoldingPurpose': 'jplvh_cor:PurposeOfHolding',
            'NumberOfSharesHeld': 'jplvh_cor:TotalNumberOfStocksEtcHeld',
            'TotalNumberOfIssuedShares': 'jplvh_cor:TotalNumberOfOutstandingStocksEtc',
            'HoldingRatio': 'jplvh_cor:HoldingRatioOfShareCertificatesEtc',
            'PreviousHoldingRatio': 'jplvh_cor:HoldingRatioOfShareCertificatesEtcPerLastReport',
        }

        filer_data = {}
        for item_name, element_id in item_map.items():
            series = ctx_df.loc[ctx_df['要素ID'] == element_id, '値']
            filer_data[item_name] = series.iloc[0] if not series.empty else None
        
        filer_data_list.append(filer_data)

    if not filer_data_list:
        return None

    result_df = pd.DataFrame(filer_data_list)

    return _finalize_lvh_df(
        result_df,
        metadata,
        ordered_columns=[
            'SubmissionDate', 'ReportObligationDate', 'IssuerName', 'IssuerSecuritiesCode',
            'FilerName', 'FilerAddress', 'IndividualOrCorporation', 'Occupation',
            'HoldingRatio', 'PreviousHoldingRatio', 'NumberOfSharesHeld', 'TotalNumberOfIssuedShares',
            'HoldingPurpose'
        ],
        numeric_cols=['HoldingRatio', 'PreviousHoldingRatio', 'NumberOfSharesHeld', 'TotalNumberOfIssuedShares'],
        date_cols=['SubmissionDate', 'ReportObligationDate']
    )

# --- テスト ---
if __name__ == '__main__':
    # テスト用のCSVファイルのパス (適宜変更してください)
    # この例では、カレントディレクトリに 'test_lvh.csv' があることを想定
    TEST_CSV_PATH = "test_lvh.csv" 
    
    try:
        print(f"--- Loading test file: {TEST_CSV_PATH} ---")
        # サンプルデータはタブ区切りなので sep='\t' を指定
        raw_df = pd.read_csv(TEST_CSV_PATH, sep='\t', engine='python')
        print("File loaded successfully.")
        
        print("\n{'='*20} Testing Large Volume Holding Parser {'='*20}")
        parsed_df = extract_large_volume_holding_data(raw_df.copy())
        
        if parsed_df is None or parsed_df.empty:
            print("No large volume holding data found or extracted.")
        else:
            print("--- Extracted Data ---")
            print(parsed_df.to_string())
            
    except FileNotFoundError:
        print(f"Error: Test file not found at {TEST_CSV_PATH}")
    except Exception as e:
        print(f"An error occurred: {e}")