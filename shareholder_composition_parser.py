import pandas as pd

def extract_shareholder_composition_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    XBRLのCSVデータフレームから「株主構成」データを抽出・整形して返す。
    """
    # --- 提出日と決算期を探索 ---
    submission_date_series = df[df['要素ID'] == 'jpcrp_cor:FilingDateCoverPage']
    submission_date = submission_date_series['値'].iloc[0] if not submission_date_series.empty else None

    fiscal_period_end_series = df[df['要素ID'] == 'jpdei_cor:CurrentPeriodEndDateDEI']
    fiscal_period_end = fiscal_period_end_series['値'].iloc[0] if not fiscal_period_end_series.empty else None

    composition_data = []

    # 各カテゴリの要素IDを定義
    categories = {
        "NationalAndLocalGovernments": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersNationalAndLocalGovernments",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsNationalAndLocalGovernments",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsNationalAndLocalGovernments"
        },
        "FinancialInstitutions": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersFinancialInstitutions",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsFinancialInstitutions",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsFinancialInstitutions"
        },
        "FinancialServiceProviders": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersFinancialServiceProviders",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsFinancialServiceProviders",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsFinancialServiceProviders"
        },
        "OtherCorporations": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersOtherCorporations",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsOtherCorporations",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsOtherCorporations"
        },
        "ForeignInvestorsOtherThanIndividuals": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersForeignInvestorsOtherThanIndividuals",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsForeignersOtherThanIndividuals",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsForeignInvestorsOtherThanIndividuals"
        },
        "ForeignIndividualInvestors": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersForeignIndividualInvestors",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsForeignIndividuals",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsForeignIndividualInvestors"
        },
        "IndividualsAndOthers": {
            "num_tag": "jpcrp_cor:NumberOfShareholdersIndividualsAndOthers",
            "pct_tag": "jpcrp_cor:PercentageOfShareholdingsIndividualsAndOthers",
            "unit_shares_tag": "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsIndividualsAndOthers"
        },
        "Total": {
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
        
        # 「計」カテゴリの割合は100%とする
        if category_name == "Total":
            final_pct = 100.0
        else:
            final_pct = pd.to_numeric(pct_shareholdings, errors='coerce') * 100 if pct_shareholdings is not None else None

        composition_data.append({
            "SubmissionDate": submission_date,
            "FiscalPeriodEnd": fiscal_period_end,
            "Category": category_name,
            "NumberOfShareholders": pd.to_numeric(num_shareholders, errors='coerce'),
            "PercentageOfShareholdings": final_pct,
            "NumberOfSharesHeldUnits": pd.to_numeric(number_of_shares_units, errors='coerce')
        })
    
    return pd.DataFrame(composition_data)



