
import pandas as pd
import numpy as np
import re

# --- 共通ヘルパー関数 ---

def _extract_metadata(df: pd.DataFrame) -> dict:
    """XBRL CSVから基本的なメタデータ（提出日、決算期、証券コード）を抽出する"""
    meta_map = {
        'jpcrp_cor:FilingDateCoverPage': 'SubmissionDate',
        'jpdei_cor:CurrentPeriodEndDateDEI': 'FiscalPeriodEnd',
        'jpdei_cor:SecurityCodeDEI': 'SecuritiesCode'
    }
    metadata = {new_name: df.loc[df['要素ID'] == old_name, '値'].iloc[0] if not df[df['要素ID'] == old_name].empty else None
                for old_name, new_name in meta_map.items()}
    
    # 証券コードの整形
    if metadata.get('SecuritiesCode'):
        match = re.search(r'\d{5}', str(metadata['SecuritiesCode']))
        if match:
            metadata['SecuritiesCode'] = match.group(0)
            
    return metadata

def _finalize_df(df: pd.DataFrame, metadata: dict, ordered_columns: list, numeric_cols: list = [], date_cols: list = []) -> pd.DataFrame:
    """メタデータの付与、データ型変換、カラム順序の整理を行う共通関数"""
    if df.empty:
        return pd.DataFrame(columns=ordered_columns)

    # メタデータを結合
    for key, value in metadata.items():
        df[key] = value

    # データ型を変換
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    # カラムの順序を整える
    final_ordered_columns = [col for col in ordered_columns if col in df.columns]
    # 存在しないカラムも追加
    for col in ordered_columns:
        if col not in final_ordered_columns:
            df[col] = None
    
    return df[ordered_columns]

# --- 大株主パーサー ---

def extract_shareholder_data(df: pd.DataFrame) -> pd.DataFrame:
    """大株主の状況を抽出・整形して返す。"""
    metadata = _extract_metadata(df)

    shareholder_data = []
    member_contexts = df[df['コンテキストID'].astype(str).str.contains("MajorShareholdersMember")]["コンテキストID"].unique()

    for context in member_contexts:
        ctx_df = df[df['コンテキストID'] == context] 
        
        name = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NameMajorShareholders']['値'].iloc[0] if not ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NameMajorShareholders'].empty else None
        ratio = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:ShareholdingRatio']['値'].iloc[0] if not ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:ShareholdingRatio'].empty else None
        num_shares = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NumberOfSharesHeld']['値'].iloc[0] if not ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NumberOfSharesHeld'].empty else None

        if name and ratio and num_shares:
            shareholder_data.append({
                "MajorShareholderName": name,
                "VotingRightsRatio": float(ratio),
                "NumberOfSharesHeld": num_shares
            })

    result_df = pd.DataFrame(shareholder_data)
    
    return _finalize_df(
        result_df, metadata,
        ordered_columns=['SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'MajorShareholderName', 'VotingRightsRatio', 'NumberOfSharesHeld'],
        numeric_cols=['VotingRightsRatio', 'NumberOfSharesHeld']
    )

# --- 株主構成パーサー ---

def extract_shareholder_composition_data(df: pd.DataFrame) -> pd.DataFrame:
    """株主構成データを抽出・整形して返す。"""
    metadata = _extract_metadata(df)
    
    categories = {
        "NationalAndLocalGovernments": ("jpcrp_cor:NumberOfShareholdersNationalAndLocalGovernments", "jpcrp_cor:PercentageOfShareholdingsNationalAndLocalGovernments", "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsNationalAndLocalGovernments"),
        "FinancialInstitutions": ("jpcrp_cor:NumberOfShareholdersFinancialInstitutions", "jpcrp_cor:PercentageOfShareholdingsFinancialInstitutions", "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsFinancialInstitutions"),
        "FinancialServiceProviders": ("jpcrp_cor:NumberOfShareholdersFinancialServiceProviders", "jpcrp_cor:PercentageOfShareholdingsFinancialServiceProviders", "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsFinancialServiceProviders"),
        "OtherCorporations": ("jpcrp_cor:NumberOfShareholdersOtherCorporations", "jpcrp_cor:PercentageOfShareholdingsOtherCorporations", "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsOtherCorporations"),
        "ForeignInvestorsOtherThanIndividuals": ("jpcrp_cor:NumberOfShareholdersForeignInvestorsOtherThanIndividuals", "jpcrp_cor:PercentageOfShareholdingsForeignersOtherThanIndividuals", "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsForeignInvestorsOtherThanIndividuals"),
        "ForeignIndividualInvestors": ("jpcrp_cor:NumberOfShareholdersForeignIndividualInvestors", "jpcrp_cor:PercentageOfShareholdingsForeignIndividuals", "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsForeignIndividualInvestors"),
        "IndividualsAndOthers": ("jpcrp_cor:NumberOfShareholdersIndividualsAndOthers", "jpcrp_cor:PercentageOfShareholdingsIndividualsAndOthers", "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsIndividualsAndOthers"),
        "Total": ("jpcrp_cor:NumberOfShareholdersTotal", None, "jpcrp_cor:NumberOfSharesHeldNumberOfUnitsTotal")
    }

    composition_data = []
    for category_name, (num_tag, pct_tag, unit_tag) in categories.items():
        num_shareholders = df[df['要素ID'] == num_tag]['値'].iloc[0] if not df[df['要素ID'] == num_tag].empty else None
        pct_shareholdings = df[df['要素ID'] == pct_tag]['値'].iloc[0] if pct_tag and not df[df['要素ID'] == pct_tag].empty else None
        num_units = df[df['要素ID'] == unit_tag]['値'].iloc[0] if not df[df['要素ID'] == unit_tag].empty else None
        
        composition_data.append({
            "Category": category_name,
            "NumberOfShareholders": num_shareholders,
            "PercentageOfShareholdings": float(pct_shareholdings) if pct_shareholdings else (1.0 if category_name == "Total" else None),
            "NumberOfSharesHeldUnits": num_units
        })

    result_df = pd.DataFrame(composition_data)

    return _finalize_df(
        result_df, metadata,
        ordered_columns=['SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'Category', 'NumberOfShareholders', 'PercentageOfShareholdings', 'NumberOfSharesHeldUnits'],
        numeric_cols=['NumberOfShareholders', 'PercentageOfShareholdings', 'NumberOfSharesHeldUnits']
    )

# --- 役員情報パーサー ---

def parse_officer_information(df: pd.DataFrame) -> pd.DataFrame:
    """役員の状況に関するデータを解析し、整形されたDataFrameを返す。"""
    metadata = _extract_metadata(df)
    
    officer_df = df[df['要素ID'].str.contains('(InformationAboutDirectorsAndCorporateAuditors|RemunerationEtcPaidByGroupToEachDirectorOrOtherOfficer)', na=False, regex=True)].copy()
    if officer_df.empty: return pd.DataFrame()

    officer_df['normalized_element_id'] = officer_df['要素ID'].str.replace('Proposal', '', regex=False)
    
    item_type_map = {
        'NameInformationAboutDirectorsAndCorporateAuditors': 'Name',
        'DateOfBirthInformationAboutDirectorsAndCorporateAuditors': 'DateOfBirth',
        'OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors': 'Title',
        'CareerSummaryInformationAboutDirectorsAndCorporateAuditorsTextBlock': 'CareerSummary',
        'NumberOfSharesHeldOrdinarySharesInformationAboutDirectorsAndCorporateAuditors': 'NumberOfSharesHeld',
        'TermOfOfficeInformationAboutDirectorsAndCorporateAuditors': 'TermOfOffice',
        'TotalAmountOfRemunerationEtcPaidByGroupRemunerationEtcPaidByGroupToEachDirectorOrOtherOfficer': 'TotalRemuneration'
    }
    conditions = [officer_df['normalized_element_id'].str.contains(k) for k in item_type_map.keys()]
    officer_df['item_type'] = np.select(conditions, list(item_type_map.values()), default=None)
    officer_df['IsNewAppointment'] = officer_df['要素ID'].str.contains('Proposal', na=False)
    officer_df['OfficerId'] = officer_df['コンテキストID'].str.extract(r'(jpcrp.*Member)')
    officer_df.dropna(subset=['OfficerId', 'item_type'], inplace=True)

    if officer_df.empty: return pd.DataFrame()

    pivot_df = officer_df.pivot_table(index='OfficerId', columns='item_type', values='値', aggfunc='first')
    new_appointment_flags = officer_df.groupby('OfficerId')['IsNewAppointment'].any()
    pivot_df = pivot_df.merge(new_appointment_flags, on='OfficerId', how='left').reset_index()

    return _finalize_df(
        pivot_df, metadata,
        ordered_columns=['SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'Name', 'IsNewAppointment', 'DateOfBirth', 'Title', 'NumberOfSharesHeld', 'TotalRemuneration', 'TermOfOffice', 'CareerSummary'],
        numeric_cols=['NumberOfSharesHeld', 'TotalRemuneration'],
        date_cols=['DateOfBirth']
    )

# --- 政策保有株式パーサー ---

def parse_specified_investment(df: pd.DataFrame) -> pd.DataFrame:
    """特定投資有価証券のデータを解析し、整形されたDataFrameを返す。"""
    metadata = _extract_metadata(df)
    
    investment_df = df[df['要素ID'].str.contains('SpecifiedInvestment', na=False)].copy()
    if investment_df.empty: return pd.DataFrame()

    def get_entity_and_type(item_name):
        if not isinstance(item_name, str): return None, None
        entity = 'ReportingCompany'
        if 'SecondLargestHoldingCompany' in item_name: entity = 'SecondLargestHoldingCompany'
        elif 'LargestHoldingCompany' in item_name: entity = 'LargestHoldingCompany'
        
        
        item_type = None
        if 'NameOfSecurities' in item_name: item_type = 'NameOfSecurities'
        elif 'NumberOfSharesHeld' in item_name: item_type = 'NumberOfSharesHeld'
        elif 'BookValue' in item_name: item_type = 'BookValue'
        elif 'PurposeOfShareholding' in item_name: item_type = 'HoldingPurpose'
        elif 'WhetherIssuerOfAforementionedSharesHoldsReportingCompanysShares' in item_name: item_type = 'CrossShareholdingStatus'
        return entity, item_type

    investment_df[['HoldingEntity', 'item_type']] = investment_df['要素ID'].apply(lambda x: pd.Series(get_entity_and_type(x)))
    investment_df['row_id'] = investment_df['コンテキストID'].str.extract(r'_Row(\d+)')
    investment_df.dropna(subset=['HoldingEntity', 'row_id', 'item_type'], inplace=True)

    pivot_df = investment_df.pivot_table(index=['HoldingEntity', 'row_id'], columns=['item_type', '相対年度'], values='値', aggfunc='first')
    pivot_df.columns = ['_'.join(filter(None, col)).strip() for col in pivot_df.columns.values]
    pivot_df.reset_index(inplace=True)

    column_mapping = {
        'HoldingEntity': 'HoldingEntity', 'NameOfSecurities_当期末': 'NameOfSecurities',
        'NumberOfSharesHeld_当期末': 'NumberOfSharesHeldCurrentYear', 'BookValue_当期末': 'BookValueCurrentYear',
        'NumberOfSharesHeld_前期末': 'NumberOfSharesHeldPriorYear', 'BookValue_前期末': 'BookValuePriorYear',
        'HoldingPurpose_当期末': 'HoldingPurpose', 'CrossShareholdingStatus_当期末': 'CrossShareholdingStatus'
    }
    result_df = pd.DataFrame({new: pivot_df.get(original) for original, new in column_mapping.items()})
    result_df.dropna(subset=['NameOfSecurities'], inplace=True)

    return _finalize_df(
        result_df, metadata,
        ordered_columns=['SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'HoldingEntity', 'NameOfSecurities', 'NumberOfSharesHeldCurrentYear', 'BookValueCurrentYear', 'NumberOfSharesHeldPriorYear', 'BookValuePriorYear', 'HoldingPurpose', 'CrossShareholdingStatus'],
        numeric_cols=['NumberOfSharesHeldCurrentYear', 'BookValueCurrentYear', 'NumberOfSharesHeldPriorYear', 'BookValuePriorYear']
    )

# --- 議決権パーサー ---

def parse_voting_rights(df: pd.DataFrame) -> pd.DataFrame:
    """議決権の状況（株式数）に関するデータを解析し、整形されたDataFrameを返す。"""
    metadata = _extract_metadata(df)
    
    shares_df = df[df['要素ID'] == 'jpcrp_cor:NumberOfSharesIssuedSharesVotingRights'].copy()
    if shares_df.empty: return pd.DataFrame()

    context_map = {
        'CurrentYearInstant': 'TotalNumberOfIssuedShares',
        'CurrentYearInstant_OrdinarySharesSharesWithFullVotingRightsOtherMember': 'NumberOfOtherSharesWithFullVotingRights',
        'CurrentYearInstant_OrdinarySharesTreasurySharesSharesWithFullVotingRightsTreasurySharesEtcMember': 'NumberOfTreasurySharesWithFullVotingRights',
        'CurrentYearInstant_OrdinarySharesSharesLessThanOneUnitMember': 'NumberOfSharesLessThanOneUnit'
    }
    
    shares_data = {col_name: shares_df.loc[shares_df['コンテキストID'] == ctx_id, '値'].iloc[0] if not shares_df[shares_df['コンテキストID'] == ctx_id].empty else None
                   for ctx_id, col_name in context_map.items()}
    
    result_df = pd.DataFrame([shares_data])

    return _finalize_df(
        result_df, metadata,
        ordered_columns=['SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'TotalNumberOfIssuedShares', 'NumberOfOtherSharesWithFullVotingRights', 'NumberOfTreasurySharesWithFullVotingRights', 'NumberOfSharesLessThanOneUnit'],
        numeric_cols=['TotalNumberOfIssuedShares', 'NumberOfOtherSharesWithFullVotingRights', 'NumberOfTreasurySharesWithFullVotingRights', 'NumberOfSharesLessThanOneUnit']
    )

# --- 統合テスト ---

if __name__ == "__main__":
    TEST_CSV_PATH = r".\data\S100W0ZR\jpcrp030000-asr-001_E03854-000_2025-03-31_01_2025-06-20.csv"
    
    try:
        print(f"--- Loading test file: {TEST_CSV_PATH} ---")
        raw_df = pd.read_csv(TEST_CSV_PATH, encoding="utf-16", sep='\t', engine='python', on_bad_lines='warn')
        print("File loaded successfully.")
    except FileNotFoundError:
        print(f"Error: Test file not found at {TEST_CSV_PATH}")
        exit()
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        exit()

    parsers_to_test = {
        "Shareholder": extract_shareholder_data,
        "Shareholder Composition": extract_shareholder_composition_data,
        "Officer": parse_officer_information,
        "Specified Investment": parse_specified_investment,
        "Voting Rights": parse_voting_rights,
    }

    for name, parser_func in parsers_to_test.items():
        print(f"\n{'='*20} Testing {name} Parser {'='*20}")
        try:
            parsed_df = parser_func(raw_df.copy())
            if parsed_df.empty:
                print(f"No {name.lower()} data found or extracted.")
            else:
                print(parsed_df.to_string())
            if name == "Voting Rights":
                parsed_df.to_clipboard(na_rep = "=na()")
        except Exception as e:
            print(f"An error occurred during {name} parsing: {e}")


    print("\n--- All parser tests finished. ---")
