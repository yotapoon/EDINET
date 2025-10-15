import pandas as pd
import edinet_api
import io
import os
import zipfile
import parsers

# --- パーサー定義の共通化 ---
# 有価証券報告書およびその訂正報告書用のパーサー
SECURITIES_REPORT_PARSERS = [
    ("MajorShareholders", parsers.extract_shareholder_data),
    ("ShareholderComposition", parsers.extract_shareholder_composition_data),
    ("SpecifiedInvestment", parsers.parse_specified_investment),
    ("Officer", parsers.parse_officer_information),
    ("VotingRights", parsers.parse_voting_rights),
]

# 自己株券買付状況報告書およびその訂正報告書用のパーサー
BUYBACK_STATUS_REPORT_PARSERS = [
    ("BuybackStatusReport", parsers.parse_buyback_status_report)
]

# 大量保有報告書およびその変更報告書用のパーサー
LARGE_VOLUME_HOLDING_PARSERS = [
    ("LargeVolumeHoldingReport", parsers.parse_large_shareholding_report),
]

# --- 書類コード(formCode)のグループ化 ---
# 有価証券報告書グループ
SECURITIES_REPORT_FORM_CODES = [
    '030000',  # 有価証券報告書
    '043000',  # 訂正有価証券報告書
]

# 自己株券買付状況報告書グループ
BUYBACK_STATUS_REPORT_FORM_CODES = [
    '170000',  # 自己株券買付状況報告書
    '170001',  # 自己株券買付状況報告書（訂正）
    '253000',  # 自己株券買付状況報告書（訂正，特定有価証券）
]

# 大量保有報告書グループ
LARGE_VOLUME_HOLDING_FORM_CODES = [
    '050210',  # 大量保有報告書
    '050220',  # 変更報告書
]

# --- PARSER_REGISTRYの動的構築 ---
PARSER_REGISTRY = {}

# 各グループのパーサーを登録
for code in SECURITIES_REPORT_FORM_CODES:
    PARSER_REGISTRY[code] = SECURITIES_REPORT_PARSERS

for code in BUYBACK_STATUS_REPORT_FORM_CODES:
    PARSER_REGISTRY[code] = BUYBACK_STATUS_REPORT_PARSERS

for code in LARGE_VOLUME_HOLDING_FORM_CODES:
    PARSER_REGISTRY[code] = LARGE_VOLUME_HOLDING_PARSERS


def fetch_and_save_document(doc_id: str, ordinanceCodeShort: str) -> str | None:
    """
    指定されたdocIDの書類をAPIから取得し、CSVをファイルに保存してそのパスを返す。
    """
    print(f"Fetching document for docID: {doc_id}")
    zip_content = edinet_api.fetch_document(doc_id)
    if not zip_content:
        return None

    # APIから返されたコンテンツがzipファイルであるかを確認 (マジックナンバー 'PK' で判定)
    if not zip_content.startswith(b'PK'):
        # zipでなければ、APIからのエラーメッセージである可能性が高い
        try:
            error_message = zip_content.decode('utf-8')
            print(f"Error: Content for docID {doc_id} is not a zip file. API response: {error_message}")
        except UnicodeDecodeError:
            print(f"Error: Content for docID {doc_id} is not a zip file and could not be decoded.")
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            # 書類内のCSVファイルを探す (XBRL_TO_CSVフォルダ以下にあるものを想定)
            target_csv_name = None
            # print(z.namelist())
            num_target = 0
            for filename in z.namelist():
                if filename.startswith(f"XBRL_TO_CSV/jp{ordinanceCodeShort}") and filename.endswith('.csv'):
                    target_csv_name = filename
                    num_target += 1
            
            if num_target > 1:
                print(f"Warning: several files found", z.namelist())
            if not target_csv_name:
                print(f"No CSV file found in XBRL_TO_CSV for docID: {doc_id}")
                return None

            # 保存先ディレクトリを作成
            save_dir = os.path.join("data", doc_id)
            os.makedirs(save_dir, exist_ok=True)
            
            # ファイルを保存
            # zip内のパスからファイル名だけを抽出
            base_filename = os.path.basename(target_csv_name)
            csv_path = os.path.join(save_dir, base_filename)

            with z.open(target_csv_name) as csv_file:
                with open(csv_path, 'wb') as f:
                    f.write(csv_file.read())
            
            print(f"Saved CSV to: {csv_path}")
            return csv_path

    except zipfile.BadZipFile:
        # zipfile.ZipFileで開けなかった場合も考慮
        print(f"Error: Content for docID {doc_id} is not a valid zip file.")
        return None
    except Exception as e:
        print(f"An error occurred while saving the file for {doc_id}: {e}")
        return None


def parse_document_file(csv_path: str, form_code: str, ordinance_code_short: str = None) -> dict:
    """
    指定されたCSVファイルを、form_codeにもとづいて適切なパーサーで解析する。
    """
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return {}

    # form_codeに対応するパーサーを取得
    parsers_to_use = PARSER_REGISTRY.get(form_code)
    if not parsers_to_use:
        # print(f"No parsers registered for formCode: {form_code}. Skipping.")
        return {}

    print(f"Parsing {csv_path} with parsers for formCode {form_code}...")

    # --- ファイル読み込み処理 ---
    df = None
    encodings_to_try = ['utf-16', 'utf-8', 'cp932']
    for encoding in encodings_to_try:
        try:
            # engine='python' は、区切り文字が1文字でない場合に発生する警告を回避するために使用
            df = pd.read_csv(csv_path, encoding=encoding, sep='\t', engine='python', on_bad_lines='warn')
            print(f"Successfully read file with encoding: {encoding}")
            break
        except Exception:
            continue
    
    if df is None:
        print(f"Failed to read file {csv_path} with any of the attempted encodings.")
        return {}

    # --- データ抽出処理 ---
    extracted_results = {}
    for data_type_name, parser_func in parsers_to_use:
        print(f"Attempting to extract {data_type_name}...")
        try:
            # パーサーごとに追加の引数を渡す
            if parser_func == parsers.parse_buyback_status_report and ordinance_code_short:
                extracted_data = parser_func(df, ordinance_code=ordinance_code_short)
            elif parser_func == parsers.parse_large_shareholding_report:
                # csv_pathからdoc_idを抽出 (e.g., "data\\S100XXXX\\file.csv")
                try:
                    doc_id = os.path.normpath(csv_path).split(os.sep)[1]
                    extracted_data = parser_func(df, doc_id=doc_id)
                except IndexError:
                    print(f"Could not extract doc_id from path: {csv_path}")
                    continue
            else:
                extracted_data = parser_func(df)

            if extracted_data is not None and not extracted_data.empty:
                extracted_results[data_type_name] = extracted_data
                print(f"Successfully extracted {len(extracted_data)} records for {data_type_name}.")
            else:
                # データがなかった場合は何も表示しない（ログが冗長になるため）
                pass
        except Exception as e:
            print(f"Error extracting {data_type_name}: {e}")
            
    return extracted_results


if __name__ == "__main__":
    # doc_id, form_code = "S100W9ZC", "053000"
    # doc_id, form_code = "S100W9YI", "030000"
    # doc_id, form_code = "S100W9Y3", "030000"
    doc_id, form_code = "S100W0ZR", "030000" # MS&AD

    # doc_id, form_code = "S100VWVY", "030000" # トヨタ自動車
    doc_id, form_code = "S100WKUZ", "010000" #
    doc_id, form_code, ordinanceCodeShort = "S100WPGX", "170000", "crp" # 網屋の自己株
    # curl "https://disclosure.edinet-fsa.go.jp/api/v2/documents/S100WKUZ?type=5&Subscription-Key=YOUR_API_KEY" -o S100WKUZ.zip

    
    
    csv_path = fetch_and_save_document(doc_id, ordinanceCodeShort)
    if csv_path:
        print(parse_document_file(csv_path, form_code = form_code)["BuybackStatusReport"])
        parse_document_file(csv_path, form_code = form_code)["BuybackStatusReport"].to_clipboard()