
import pandas as pd
import database_manager

def analyze_specified_investment_accuracy():
    """
    特定投資株式の名寄せ精度を分析し、結果を出力する。
    """
    print("--- Analyzing enrichment accuracy for SpecifiedInvestment ---")

    # 1. EnrichedSpecifiedInvestment テーブルからデータを取得
    table_name = "EnrichedSpecifiedInvestment"
    df = database_manager.get_data_for_enrichment(table_name, "NameOfSecurities")

    if df.empty:
        print(f"No data found in {table_name}. Aborting analysis.")
        return

    # 2. 名寄せ精度の計算
    total_records = len(df)
    # matchedNameがNULL(None)または空文字列でないものを成功とカウント
    successful_matches = df["matchedName"].notna() & (df["matchedName"] != '').sum()
    accuracy = (successful_matches / total_records) * 100 if total_records > 0 else 0

    print(f"\n[Accuracy Metrics]")
    print(f"Total records: {total_records}")
    print(f"Successful matches: {successful_matches}")
    print(f"Accuracy: {accuracy:.2f}%")

    # 3. 失敗したデータの特定と集計
    # matchedNameがNULL(None)または空文字列のものを失敗と定義
    failed_df = df[df["matchedName"].isna() | (df["matchedName"] == '')].copy()

    if failed_df.empty:
        print("\nNo failed matches found. Excellent!")
    else:
        print("\n[Top 20 Failed Matches (by occurrence)]")
        failure_counts = failed_df["NameOfSecurities"].value_counts().nlargest(20)
        
        # 結果を整形してDataFrameで表示
        result_df = pd.DataFrame({
            "名寄せ前の名前": failure_counts.index,
            "名寄せ後の名前": "N/A",
            "出現回数": failure_counts.values
        })
        
        print(result_df.to_string(index=False))

    print("\n--- Analysis complete ---")

if __name__ == "__main__":
    analyze_specified_investment_accuracy()
