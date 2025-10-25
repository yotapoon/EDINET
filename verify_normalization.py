import pandas as pd
# matching.pyから正規化関数と、それに必要なライブラリを直接インポート
from matching import _normalize_name

if __name__ == "__main__":
    print("--- Verifying _normalize_name function ---")

    # 問題となっている代表的なテストケース
    test_cases = [
        '日本製鉄㈱', 
        '凸版印刷㈱',
        'パナソニック㈱',
        '㈱関西スーパーマーケット',
        '東洋インキＳＣホールディングス㈱',
        '㈱ＬＩＸＩＬグループ',
        '（株）阿波銀行（注）１',
        '新日鐵住金株式会社 (現・日本製鉄株式会社）',
        '㈱伊藤園優先株式',
        'AFLAC Inc．(アフラック)'
    ]

    results = []
    for name in test_cases:
        normalized = _normalize_name(name)
        results.append({
            "Original": name,
            "Normalized": normalized
        })
    
    # 結果をDataFrameで見やすく表示
    results_df = pd.DataFrame(results)
    print(results_df.to_string())

    print("\n--- Verification finished ---")

