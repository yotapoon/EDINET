import pandas as pd
import re
import io

def extract_shareholder_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    XBRLのCSVデータフレームから「証券コード」と「大株主の状況」を抽出・整形して返す。
    """
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
    member_contexts = df[df['コンテキストID'].astype(str).str.contains("MajorShareholdersMember")]['コンテキストID'].unique()

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
                "証券コード": securities_code,
                "大株主名": name,
                "議決権割合": pd.to_numeric(ratio, errors='coerce') * 100, # %に変換
                "保有株数": pd.to_numeric(number_of_shares, errors='coerce') # 数値に変換
            })

    return pd.DataFrame(shareholder_data)


if __name__ == '__main__':
    """
    このファイル単体で実行した際の、動作確認用のコード。
    ダミーデータを使って、抽出処理をテストする。
    """
    print("--- Running shareholder_parser.py test with dummy data ---")

    # --- サンプルのダミーデータを作成 ---
    # 実際のEDINET CSVの9列構造を模倣
    dummy_data = {
        '要素ID': [
            'jpdei_cor:SecurityCodeDEI',
            'jpcrp_cor:NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc',
            'jpcrp_cor:NameMajorShareholders',
            'jpcrp_cor:ShareholdingRatio',
            'jpcrp_cor:NumberOfSharesHeld',
            'jpcrp_cor:NameMajorShareholders',
            'jpcrp_cor:ShareholdingRatio',
            'jpcrp_cor:NumberOfSharesHeld',
        ],
        '項目名': [
            '証券コード、DEI',
            '事業年度末現在発行数（株）、発行済株式、株式の総数等',
            '氏名又は名称、大株主の状況',
            '発行済株式（自己株式を除く。）の総数に対する所有株式数の割合',
            '所有株式数',
            '氏名又は名称、大株主の状況',
            '発行済株式（自己株式を除く。）の総数に対する所有株式数の割合',
            '所有株式数',
        ],
        'コンテキストID': [
            'FilingDateInstant',
            'FilingDateInstant_OrdinaryShareMember',
            'CurrentYearInstant_No1MajorShareholdersMember',
            'CurrentYearInstant_No1MajorShareholdersMember',
            'CurrentYearInstant_No1MajorShareholdersMember',
            'CurrentYearInstant_No2MajorShareholdersMember',
            'CurrentYearInstant_No2MajorShareholdersMember',
            'CurrentYearInstant_No2MajorShareholdersMember',
        ],
        '相対年度': ['－', '提出日時点', '当期末', '当期末', '当期末', '当期末', '当期末', '当期末'],
        '連結・個別': ['－', 'その他', 'その他', 'その他', 'その他', 'その他', 'その他', 'その他'],
        '期間・時点': ['時点', '時点', '時点', '時点', '時点', '時点', '時点', '時点'],
        'ユニットID': ['－', 'shares', '－', 'pure', 'shares', '－', 'pure', 'shares'],
        '単位': ['－', '－', '－', '－', '－', '－', '－', '－'],
        '値': [
            '72030',
            '15794987460',
            'ダミー株主１',
            '0.15',
            '10000000',
            'ダミー株主２',
            '0.05',
            '5000000',
        ]
    }
    dummy_df = pd.DataFrame(dummy_data)

    print("\n--- Sample Input DataFrame (Dummy Data) ---")
    print(dummy_df)

    # --- 抽出関数を実行 ---
    extracted_df = extract_shareholder_data(dummy_df)

    print("\n--- Extracted DataFrame ---")
    print(extracted_df)

    # --- 検証 ---
    assert not extracted_df.empty, "抽出結果が空です"
    assert "証券コード" in extracted_df.columns
    assert extracted_df["証券コード"].iloc[0] == "72030", f'証券コードが不正です: {extracted_df["証券コード"].iloc[0]}'
    assert extracted_df["大株主名"].iloc[0] == "ダミー株主１", f'大株主名が不正です: {extracted_df["大株主名"].iloc[0]}'
    assert extracted_df["議決権割合"].iloc[0] == 15.0, f'議決権割合が不正です: {extracted_df["議決権割合"].iloc[0]}'
    assert extracted_df["保有株数"].iloc[0] == 10000000, f'保有株数が不正です: {extracted_df["保有株数"].iloc[0]}'
    print("\nTest passed!")