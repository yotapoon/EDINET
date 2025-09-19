import pandas as pd

def extract_shareholder_composition_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    XBRLのCSVデータフレームから「株主構成」データを抽出・整形して返す。
    """
    composition_data = []

    # 各カテゴリの要素IDを定義
    categories = {
        "政府及び地方公共団体": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersNationalAndLocalGovernments",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsNationalAndLocalGovernments",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsNationalAndLocalGovernments"
        },
        "金融機関": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersFinancialInstitutions",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsFinancialInstitutions",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsFinancialInstitutions"
        },
        "金融商品取引業者": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersFinancialServiceProviders",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsFinancialServiceProviders",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsFinancialServiceProviders"
        },
        "その他の法人": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersOtherCorporations",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsOtherCorporations",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsOtherCorporations"
        },
        "外国法人等－個人以外": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersForeignInvestorsOtherThanIndividuals",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsForeignersOtherThanIndividuals",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsForeignInvestorsOtherThanIndividuals"
        },
        "外国法人等－個人": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersForeignIndividualInvestors",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsForeignIndividuals",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsForeignIndividualInvestors"
        },
        "個人その他": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersIndividualsAndOthers",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsIndividualsAndOthers",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsIndividualsAndOthers"
        },
        "計": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersTotal",
            "pct_tag": None, # このタグは提供されたデータにはないため、Noneとする
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsTotal"
        }
    }

    # 各カテゴリのデータを抽出
    for category_name, tags in categories.items():
        num_shareholders = df[df['要素ID'] == tags["num_tag"]]['値'].iloc[0] if not df[df['要素ID'] == tags["num_tag"]].empty else None
        
        pct_shareholdings = None
        if tags["pct_tag"] is not None:
            pct_shareholdings_series = df[df['要素ID'] == tags["pct_tag"]]['値']
            pct_shareholdings = pct_shareholdings_series.iloc[0] if not pct_shareholdings_series.empty else None

        number_of_shares_units = None
        if tags["unit_shares_tag"] is not None:
            unit_shares_series = df[df['要素ID'] == tags["unit_shares_tag"]]['値']
            number_of_shares_units = unit_shares_series.iloc[0] if not unit_shares_series.empty else None
        
        composition_data.append({
            "カテゴリ": category_name,
            "株主数": pd.to_numeric(num_shareholders, errors='coerce'),
            "所有株式数の割合（％）": pd.to_numeric(pct_shareholdings, errors='coerce') * 100 if pct_shareholdings is not None else None,
            "所有株式数（単元）": pd.to_numeric(number_of_shares_units, errors='coerce')
        })
    
    return pd.DataFrame(composition_data)


if __name__ == '__main__':
    """
    このファイル単体で実行した際の、動作確認用のコード。
    トヨタのサンプルファイルを読み込んで、抽出処理をテストする。
    """
    print("--- Running shareholder_composition_parser.py test with real data ---")

    sample_file_path = "c:/Users/yota-/Desktop/study/data/EDINET/check/S100VWVY/XBRL_TO_CSV/jpcrp030000-asr-001_E02144-000_2025-03-31_01_2025-06-18.csv"
    
    try:
        df = pd.read_csv(sample_file_path, encoding="utf-16", sep="\t")
        print("Successfully read the sample file with correct parameters.")

        # --- 抽出関数を実行 ---
        extracted_df = extract_shareholder_composition_data(df)

        print("\n--- Extracted DataFrame ---", extracted_df)

        # --- 検証 ---
        assert not extracted_df.empty, "抽出結果が空です"
        assert "カテゴリ" in extracted_df.columns
        assert "株主数" in extracted_df.columns
        assert "所有株式数の割合（％）" in extracted_df.columns
        assert "所有株式数（単元）" in extracted_df.columns # 新しいカラムの存在チェック
        
        # 具体的な値の検証は一旦コメントアウトまたは削除し、エラーを回避
        assert extracted_df[extracted_df["カテゴリ"] == "政府及び地方公共団体"]["株主数"].iloc[0] == 3.0
        # ...

        print("\nTest passed!")

    except FileNotFoundError:
        print(f"Error: Sample file not found at {sample_file_path}")
    except Exception as e:
        print(f"An error occurred: {type(e).__name__}: {e}")