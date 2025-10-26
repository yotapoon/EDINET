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

# --- 大量保有報告書パーサー ---

def parse_large_shareholding_report(df: pd.DataFrame, doc_id: str) -> pd.DataFrame:
    """
    大量保有報告書のDataFrameから、提出者および各保有者の詳細情報を抽出・整形してDataFrameとして返す。
    キーは docId と member とする。
    """
    # --- データ定義 ---
    # 提出者属性 (ドキュメントレベル)
    SUBMITTER_MAP = {
        'submitterName': 'jplvh_cor:NameCoverPage',
        'submitterEdinetCode': 'jpdei_cor:EDINETCodeDEI',
        'submitterSecurityCode': 'jpdei_cor:SecurityCodeDEI',
        'submitterLvhSecurityCode': 'jplvh_cor:SecurityCodeDEI',
        'dateFile': 'jplvh_cor:FilingDateCoverPage',
        'obligationDate': 'jplvh_cor:DateWhenFilingRequirementAroseCoverPage',
        'isAmendment': 'jpdei_cor:AmendmentFlagDEI',
        'submissionCount': 'jpdei_cor:NumberOfSubmissionDEI',
    }
    # 発行者属性 (ドキュメントレベル)
    ISSUER_MAP = {
        'issuerSecurityCode': 'jplvh_cor:SecurityCodeOfIssuer',
        'issuerName': 'jplvh_cor:NameOfIssuer',
    }
    # 保有者情報 (メンバーレベル)
    HOLDER_MAP = {
        'holderEdinetCode': 'jplvh_cor:EDINETCodeDEI',
        'holderName': 'jplvh_cor:Name',
        'holderNameJp': 'jplvh_cor:FilerNameInJapaneseDEI',
        'holderNameEn': 'jplvh_cor:FilerNameInEnglishDEI',
        'holderAddress': 'jplvh_cor:ResidentialAddressOrAddressOfRegisteredHeadquarter',
        'establishmentDate': 'jplvh_cor:DateOfEstablishment',
        'businessDescription': 'jplvh_cor:DescriptionOfBusiness',
        'contactPerson': 'jplvh_cor:ContactInformationAndPerson',
        'phoneNumber': 'jplvh_cor:TelephoneNumber',
        'formerName': 'jplvh_cor:FormerName',
        'formerAddress': 'jplvh_cor:FormerResidentialAddressOrAddressOfRegisteredHeadquarter',
        'representativeName': 'jplvh_cor:NameOfRepresentative',
        'representativeTitle': 'jplvh_cor:JobTitleOfRepresentative',
        'birthDate': 'jplvh_cor:DateOfBirth',
        'occupation': 'jplvh_cor:Occupation',
        'employerName': 'jplvh_cor:NameOfEmployer',
        'employerAddress': 'jplvh_cor:AddressOfEmployer',
        'hasImportantProposal': 'jplvh_cor:ActOfMakingImportantProposalEtc',
        'noImportantProposal': 'jplvh_cor:ActOfMakingImportantProposalEtcNA',
        'holdingPurpose': 'jplvh_cor:PurposeOfHolding',
        'baseDate': 'jplvh_cor:BaseDate',
        'totalOutstandingShares': 'jplvh_cor:TotalNumberOfOutstandingStocksEtc',
        'totalSharesHeld': 'jplvh_cor:TotalNumberOfStocksEtcHeld',
        'holdingRatio': 'jplvh_cor:HoldingRatioOfShareCertificatesEtc',
        'previousHoldingRatio': 'jplvh_cor:HoldingRatioOfShareCertificatesEtcPerLastReport',
        'ownFunds': 'jplvh_cor:AmountOfOwnFund',
        'totalBorrowings': 'jplvh_cor:TotalAmountOfBorrowings',
        'otherFunds': 'jplvh_cor:TotalAmountFromOtherSources',
        'totalAcquisitionFunds': 'jplvh_cor:TotalAmountOfFundingForAcquisition',
    }

    # --- ヘルパー関数 ---
    def get_member_id(context_id: str) -> int | None:
        """コンテキストIDから 'member' の番号を抽出する"""
        if not isinstance(context_id, str):
            return None
        # FilerLargeVolumeHolder1Member -> 1
        # _E40896-000JointHolder1Member -> 1
        match = re.search(r'(?:FilerLargeVolumeHolder|JointHolder)(\d+)Member', context_id)
        if match:
            return int(match.group(1))
        return None

    def get_value(element_id: str, context_id: str | None = None) -> str | None:
        """指定された要素IDとコンテキストIDに一致する値を取得する"""
        filtered_df = df[df['要素ID'] == element_id]
        if context_id:
            filtered_df = filtered_df[filtered_df['コンテキストID'] == context_id]
        
        if not filtered_df.empty:
            value = filtered_df['値'].iloc[0]
            return value if pd.notna(value) and str(value).strip() not in ['－', '-'] else None
        return None

    # --- メイン処理 ---
    
    # 1. ドキュメントレベルの情報を抽出
    doc_level_data = {}
    all_doc_maps = {**SUBMITTER_MAP, **ISSUER_MAP}
    for key, element_id in all_doc_maps.items():
        # ドキュメントレベルの情報はコンテキストを一意に特定しづらいため、最初に見つかった値を取得
        series = df.loc[df['要素ID'] == element_id, '値']
        doc_level_data[key] = series.iloc[0] if not series.empty else None

    # 2. member IDを各行に付与
    df['member'] = df['コンテキストID'].apply(get_member_id)
    
    # 3. memberごとの情報を抽出
    all_members_data = []
    # member IDを持つ行のみを対象
    member_df = df.dropna(subset=['member']).copy()
    member_df['member'] = member_df['member'].astype(int)
    
    unique_members = member_df['member'].unique()

    for member_id in unique_members:
        member_data = {'docId': doc_id, 'member': member_id}
        
        # このmemberに関連するコンテキストIDのリストを取得
        member_contexts = member_df[member_df['member'] == member_id]['コンテキストID'].unique()
        
        for key, element_id in HOLDER_MAP.items():
            # 複数のコンテキストがありうるため、最初に見つかった有効な値を取得
            value = None
            for ctx_id in member_contexts:
                val = get_value(element_id, ctx_id)
                if val is not None:
                    value = val
                    break
            member_data[key] = value
            
        all_members_data.append(member_data)

    if not all_members_data:
        return pd.DataFrame()

    final_df = pd.DataFrame(all_members_data)

    # ドキュメントレベルの情報を各行に結合
    for key, value in doc_level_data.items():
        final_df[key] = value

    # hasImportantProposalとnoImportantProposalをマージして新しいカラムを作成
    if 'hasImportantProposal' in final_df.columns and 'noImportantProposal' in final_df.columns:
        final_df['importantProposal'] = final_df['hasImportantProposal'].fillna(final_df['noImportantProposal'])
    elif 'hasImportantProposal' in final_df.columns:
        final_df['importantProposal'] = final_df['hasImportantProposal']
    elif 'noImportantProposal' in final_df.columns:
        final_df['importantProposal'] = final_df['noImportantProposal']
    else:
        final_df['importantProposal'] = None
        
    # ユーザー指定のカラムリスト (キャメルケース)
    ordered_columns = [
        'docId', 'member', 'submitterName', 'submitterEdinetCode',
        'dateFile', 'obligationDate', 'isAmendment', 'submissionCount',
        'issuerSecurityCode', 'issuerName', 'holderEdinetCode', 'holderName',
        'holdingPurpose', 'importantProposal', 'baseDate', 'totalOutstandingShares',
        'totalSharesHeld', 'holdingRatio', 'previousHoldingRatio',
        'ownFunds', 'totalBorrowings', 'otherFunds', 'totalAcquisitionFunds'
    ]

    # 存在しないカラムをNoneで追加
    for col in ordered_columns:
        if col not in final_df.columns:
            final_df[col] = None
            
    # 指定されたカラムのみを正しい順序で返す
    return final_df[ordered_columns]


# --- 大株主パーサー ---

def extract_shareholder_data(df: pd.DataFrame) -> pd.DataFrame:
    """大株主の状況を抽出・整形して返す。"""
    metadata = _extract_metadata(df)

    shareholder_data = []
    member_contexts = df[df['コンテキストID'].astype(str).str.contains("MajorShareholdersMember")]["コンテキストID"].unique()

    for context in member_contexts:
        ctx_df = df[df['コンテキストID'] == context]
        
        # shareholderIdをコンテキストから抽出
        shareholder_id_match = re.search(r'No(\d+)MajorShareholdersMember', context)
        shareholder_id = int(shareholder_id_match.group(1)) if shareholder_id_match else None

        name = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NameMajorShareholders']['値'].iloc[0] if not ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NameMajorShareholders'].empty else None
        ratio = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:ShareholdingRatio']['値'].iloc[0] if not ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:ShareholdingRatio'].empty else None
        num_shares = ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NumberOfSharesHeld']['値'].iloc[0] if not ctx_df[ctx_df['要素ID'] == 'jpcrp_cor:NumberOfSharesHeld'].empty else None

        # '－'をNoneに変換
        if str(name).strip() == '－': name = None
        if str(ratio).strip() == '－': ratio = None
        if str(num_shares).strip() == '－': num_shares = None

        if name and shareholder_id is not None:
            shareholder_data.append({
                "shareholderId": shareholder_id,
                "MajorShareholderName": name,
                "VotingRightsRatio": ratio,
                "NumberOfSharesHeld": num_shares
            })

    result_df = pd.DataFrame(shareholder_data)
    
    return _finalize_df(
        result_df, metadata,
        ordered_columns=['SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'shareholderId', 'MajorShareholderName', 'VotingRightsRatio', 'NumberOfSharesHeld'],
        numeric_cols=['shareholderId', 'VotingRightsRatio', 'NumberOfSharesHeld']
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

    def get_clean_value(element_id):
        """要素IDから値を取得し、無効な値をNoneに変換する"""
        if not element_id or df[df['要素ID'] == element_id].empty:
            return None
        value = df[df['要素ID'] == element_id]['値'].iloc[0]
        if pd.isna(value) or str(value).strip() in ['－', '-']:
            return None
        return value

    composition_data = []
    for category_name, (num_tag, pct_tag, unit_tag) in categories.items():
        num_shareholders = get_clean_value(num_tag)
        pct_shareholdings = get_clean_value(pct_tag)
        num_units = get_clean_value(unit_tag)
        
        composition_data.append({
            "Category": category_name,
            "NumberOfShareholders": num_shareholders,
            "PercentageOfShareholdings": pct_shareholdings if pct_shareholdings is not None else (1.0 if category_name == "Total" else None),
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
    
    officer_df = df[df['要素ID'].str.contains('(?:InformationAboutDirectorsAndCorporateAuditors|RemunerationEtcPaidByGroupToEachDirectorOrOtherOfficer)', na=False, regex=True)].copy()
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
    officer_df['officerId'] = officer_df['コンテキストID'].str.extract(r'(jpcrp.*Member)')
    officer_df.dropna(subset=['officerId', 'item_type'], inplace=True)

    if officer_df.empty: return pd.DataFrame()

    pivot_df = officer_df.pivot_table(index='officerId', columns='item_type', values='値', aggfunc='first')
    new_appointment_flags = officer_df.groupby('officerId')['IsNewAppointment'].any()
    pivot_df = pivot_df.merge(new_appointment_flags, on='officerId', how='left').reset_index()

    return _finalize_df(
        pivot_df, metadata,
        ordered_columns=['SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'officerId', 'Name', 'IsNewAppointment', 'DateOfBirth', 'Title', 'NumberOfSharesHeld', 'TotalRemuneration', 'TermOfOffice', 'CareerSummary'],
        numeric_cols=['NumberOfSharesHeld', 'TotalRemuneration'],
        date_cols=['DateOfBirth']
    )

# --- 政策保有株式パーサー ---

def parse_specified_investment(df: pd.DataFrame) -> pd.DataFrame:
    """特定投資有価証券のデータを解析し、整形されたDataFrameを返す。"""
    metadata = _extract_metadata(df)
    
    investment_df = df[df['要素ID'].str.contains('SpecifiedInvestment', na=False)].copy()
    if investment_df.empty: return pd.DataFrame()

    def _get_single_value(element_id: str) -> str | None:
        """DataFrameから単一の要素IDの値を取得する。"""
        series = df.loc[df['要素ID'] == element_id, '値']
        if not series.empty:
            val = series.iloc[0]
            return val if pd.notna(val) and str(val).strip() not in ['－', '-'] else None
        return None

    largest_holder_name = _get_single_value('jpcrp_cor:NameOfGroupCompanyHoldingLargestAmountOfInvestmentSharesInGroup')
    second_largest_holder_name = _get_single_value('jpcrp_cor:NameOfGroupCompanyHoldingSecondLargestAmountOfInvestmentSharesInGroup')

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
    investment_df['rowId'] = investment_df['コンテキストID'].str.extract(r'_Row(\d+)')
    investment_df.dropna(subset=['HoldingEntity', 'rowId', 'item_type'], inplace=True)

    pivot_df = investment_df.pivot_table(index=['HoldingEntity', 'rowId'], columns=['item_type', '相対年度'], values='値', aggfunc='first')
    pivot_df.columns = ['_'.join(filter(None, col)).strip() for col in pivot_df.columns.values]
    pivot_df.reset_index(inplace=True)

    column_mapping = {
        'rowId': 'rowId', # 主キーのために追加
        'HoldingEntity': 'HoldingEntity', 'NameOfSecurities_当期末': 'NameOfSecurities',
        'NumberOfSharesHeld_当期末': 'NumberOfSharesHeldCurrentYear', 'BookValue_当期末': 'BookValueCurrentYear',
        'NumberOfSharesHeld_前期末': 'NumberOfSharesHeldPriorYear', 'BookValue_前期末': 'BookValuePriorYear',
        'HoldingPurpose_当期末': 'HoldingPurpose', 'CrossShareholdingStatus_当期末': 'CrossShareholdingStatus'
    }
    result_df = pd.DataFrame({new: pivot_df.get(original) for original, new in column_mapping.items()})
    result_df.dropna(subset=['NameOfSecurities'], inplace=True)

    # HoldingEntityName を条件に応じて設定
    conditions = [
        result_df['HoldingEntity'] == 'LargestHoldingCompany',
        result_df['HoldingEntity'] == 'SecondLargestHoldingCompany'
    ]
    choices = [
        largest_holder_name,
        second_largest_holder_name
    ]
    result_df['HoldingEntityName'] = np.select(conditions, choices, default=None)

    return _finalize_df(
        result_df, metadata,
        ordered_columns=['SubmissionDate', 'FiscalPeriodEnd', 'SecuritiesCode', 'HoldingEntity', 'HoldingEntityName', 'rowId', 'NameOfSecurities', 'NumberOfSharesHeldCurrentYear', 'BookValueCurrentYear', 'NumberOfSharesHeldPriorYear', 'BookValuePriorYear', 'HoldingPurpose', 'CrossShareholdingStatus'],
        numeric_cols=['rowId', 'NumberOfSharesHeldCurrentYear', 'BookValueCurrentYear', 'NumberOfSharesHeldPriorYear', 'BookValuePriorYear']
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

# --- 自己株券買付状況報告書パーサー ---

def parse_buyback_status_report(df: pd.DataFrame, ordinance_code: str = "crp") -> pd.DataFrame:
    """
    自己株券買付状況報告書（府令コード指定）のデータを解析し、整形されたDataFrameを返す。
    ordinance_code に基づいて、一般企業(crp)とREIT(sps)の形式に対応する。

    Args:
        df (pd.DataFrame): XBRLをCSVに変換したデータフレーム。
        ordinance_code (str, optional): 府令コードの略号 ('crp' or 'sps'). Defaults to "crp".

    Returns:
        pd.DataFrame: 抽出・整形されたデータを含むDataFrame。

    Raises:
        ValueError: ordinance_codeが 'crp' または 'sps' でない場合。
    """
    if ordinance_code not in ["crp", "sps"]:
        raise ValueError("ordinance_code must be 'crp' or 'sps'")

    def _get_value(element_id: str) -> str | None:
        """DataFrameから指定された要素IDの値を取得し、無効な値をNoneに変換する。"""
        try:
            value = df.loc[df['要素ID'] == element_id, '値'].iloc[0]
            if pd.isna(value) or str(value).strip() in ['－', '#N/A', '-']:
                return None
            return str(value)
        except (IndexError, KeyError):
            return None

    # 府令コードに基づいてプレフィックスを決定
    sbr_prefix = f"jp{ordinance_code}-sbr_cor"

    # メタデータを抽出
    metadata = {
        'dateFile': _get_value(f"{sbr_prefix}:FilingDateCoverPage"),
        # 'FiscalPeriodEnd': _get_value('jpdei_cor:CurrentPeriodEndDateDEI'), # この報告書では通常None
        'secCode': _get_value('jpdei_cor:SecurityCodeDEI')
    }
    # 証券コードの整形
    if metadata.get('secCode'):
        match = re.search(r'\d{5}', str(metadata['secCode']))
        if match:
            metadata['secCode'] = match.group(0)

    # 取得状況の要素IDを条件分岐で決定
    if ordinance_code == 'crp':
        acquisition_id = f"{sbr_prefix}:AcquisitionsByResolutionOfBoardOfDirectorsMeetingTextBlock"
    else: # sps
        acquisition_id = f"{sbr_prefix}:AcquisitionsOfTreasurySharesTextBlock"

    # 主要なデータを抽出
    report_data = {
        'ordinanceCode': _get_value('jpdei_cor:CabinetOfficeOrdinanceDEI'),
        'formCode': _get_value('jpdei_cor:DocumentTypeDEI'),
        'acquisitionStatus': _get_value(acquisition_id),
        'disposalStatus': _get_value(f"{sbr_prefix}:DisposalsOfTreasurySharesTextBlock"),
        'holdingStatus': _get_value(f"{sbr_prefix}:HoldingOfTreasurySharesTextBlock"),
    }
    
    result_df = pd.DataFrame([report_data])

    ordered_columns = [
        'dateFile', 'secCode',
        'ordinanceCode', 'formCode', 'acquisitionStatus',
        'disposalStatus', 'holdingStatus'
    ]
    
    # _finalize_df を使ってメタデータ結合と整形を行う
    return _finalize_df(
        result_df, metadata,
        ordered_columns=ordered_columns
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
