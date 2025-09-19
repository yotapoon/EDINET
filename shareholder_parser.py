import pandas as pd
import re
import io

def extract_shareholder_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    XBRLのCSVデータフレームから「証券コード」と「大株主の状況」を抽出・整形して返す。
    """
    # --- 提出日と決算期を探索 ---
    submission_date_series = df[df['要素ID'] == 'jpcrp_cor:FilingDateCoverPage']
    submission_date = submission_date_series['値'].iloc[0] if not submission_date_series.empty else None

    fiscal_period_end_series = df[df['要素ID'] == 'jpdei_cor:CurrentPeriodEndDateDEI']
    fiscal_period_end = fiscal_period_end_series['値'].iloc[0] if not fiscal_period_end_series.empty else None

    # --- 証券コードを探索 ---
    securities_code_series = df[df['要素ID'] == 'jpdei_cor:SecurityCodeDEI']
    securities_code = securities_code_series['値'].iloc[0] if not securities_code_series.empty else None
    # 証券コードは5桁の数字なので、それに合うように整形
    if isinstance(securities_code, str):
        match = re.search(r'\d{5}', securities_code) # 5桁に修正
        if match:
            securities_code = match.group(0)

    # --- 大株主の状況テーブルを探索 ---
    shareholder_data = []
    member_contexts = df[df['コンテキストID'].astype(str).str.contains("MajorShareholdersMember")]["コンテキストID"].unique()

    for context in member_contexts:
        ctx_df = df[df['コンテキストID'] == context]
        
        # 株主名を取得
        name_series = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NameMajorShareholders']
        name = name_series['値'].iloc[0] if not name_series.empty else None

        # 議決権割合を取得
        ratio_series = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:ShareholdingRatio']
        ratio = ratio_series['値'].iloc[0] if not ratio_series.empty else None

        # 保有株数を取得
        number_of_shares_series = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NumberOfSharesHeld']
        number_of_shares = number_of_shares_series['値'].iloc[0] if not number_of_shares_series.empty else None

        if name and ratio and number_of_shares: # 保有株数も条件に追加
            shareholder_data.append({
                "SubmissionDate": submission_date,
                "FiscalPeriodEnd": fiscal_period_end,
                "SecuritiesCode": securities_code,
                "MajorShareholderName": name,
                "VotingRightsRatio": pd.to_numeric(ratio, errors='coerce') * 100, # %に変換
                "NumberOfSharesHeld": pd.to_numeric(number_of_shares, errors='coerce') # 数値に変換
            })

    return pd.DataFrame(shareholder_data)




