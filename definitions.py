# --- 設定定義 ---

# 1. 書類種別の定義
# 書類の大きなカテゴリと、それに該当する(form_code, ordinance_code)の組み合わせを定義
# check_文書コードを使って確認する
DOCUMENT_TYPE_DEFINITIONS = {
    'AnnualSecuritiesReport': [
        ('030000', '010'), ('030001', '010') # 有価証券報告書, 訂正有価証券報告書
    ],
    'LargeVolumeHoldingReport': [
        ('010002', '060'), ('030002', '060'), ('090001', '060'), 
        ('030000', '060'), ('010000', '060'), ('020002', '060')
    ],
    'BuybackStatusReport': [
        ('170000', '010'), ('170001', '010'), ('253000', '030')
    ],
}

# 2. データプロダクトの定義
# 最終的に取得したいデータ項目（プロダクト）と、それがどの「書類種別」から得られるかをマッピング
# キーは `parsers.py` の実装（パーサーが返すdictのキー）と一致させる
DATA_PRODUCT_DEFINITIONS = {
    # AnnualSecuritiesReportから取得
    'MajorShareholders':      'AnnualSecuritiesReport',
    'ShareholderComposition': 'AnnualSecuritiesReport',
    'Officer':                'AnnualSecuritiesReport',
    'SpecifiedInvestment':    'AnnualSecuritiesReport',
    'VotingRights':           'AnnualSecuritiesReport',

    # 他の書類から取得
    'LargeVolumeHoldingReport': 'LargeVolumeHoldingReport',
    'BuybackStatusReport':      'BuybackStatusReport',
}

