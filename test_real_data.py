import pandas as pd
import glob
import os
import sys

# モジュール検索パスにカレントディレクトリを追加
sys.path.append(os.path.dirname(__file__))

# parsersモジュールからテスト対象の関数をインポート
from parsers import parse_specified_investment

# テスト対象のCSVファイルを検索
target_dir = r"c:\Users\yota-\Desktop\study\data\EDINET\data"

# .gitignoreを考慮せずに全てのCSVファイルを検索
# globはデフォルトでgit-ignoreを尊重する場合があるため、手動で全ファイルをリストアップする
all_csv_files = []
for root, dirs, files in os.walk(target_dir):
    for file in files:
        if file.endswith('.csv'):
            all_csv_files.append(os.path.join(root, file))

if not all_csv_files:
    print("No CSV files found in data directory.")
    exit()

# 'jpcrp' を含むファイル（有価証券報告書の可能性が高い）を優先
prioritized_files = [f for f in all_csv_files if 'jpcrp' in os.path.basename(f)]
other_files = [f for f in all_csv_files if 'jpcrp' not in os.path.basename(f)]

# 優先ファイルとそれ以外を結合し、最大10件に絞る
files_to_test = (prioritized_files + other_files)[:10]

if not files_to_test:
    print("No suitable test CSV files found.")
    exit()

print(f"Found {len(files_to_test)} files to test (prioritizing 'jpcrp' files):")
for f in files_to_test:
    print(f"- {os.path.basename(f)}")
print("---")

# 各ファイルを処理して結果を表示
for file_path in files_to_test:
    print(f"--- Testing file: {os.path.basename(file_path)} ---")
    try:
        # parsers.pyのテストコードを参考に、ファイルを読み込む
        df = pd.read_csv(file_path, encoding="utf-16", sep='\t', engine='python', on_bad_lines='warn')

        # パーサーを実行
        result_df = parse_specified_investment(df)

        # 結果の表示
        if result_df is not None and not result_df.empty:
            # HoldingSubsidiaryName に値がある行だけフィルタして表示
            filtered_result = result_df[result_df['HoldingSubsidiaryName'].notna()]
            if not filtered_result.empty:
                print("\n+++ Found Result with HoldingSubsidiaryName +++")
                print(filtered_result.to_string())
                print("++++++++++++++++++++++++++++++++++++++++++++++")
            else:
                print("-> No 'HoldingSubsidiaryName' value found in the parsed result.")

        else:
            print("-> No specified investment data was extracted from this file.")

    except Exception as e:
        print(f"!!! An error occurred while processing the file: {e}")
    print("\n" + "="*70 + "\n")