import pandas as pd
import edinet_api
import io
import os
import zipfile
import parsers
from definitions import DOCUMENT_TYPE_DEFINITIONS

# --- パーサーと書類種別のマッピング ---
# どの「書類種別」がどのパーサー（群）を実行すべきかを定義する
DOC_TYPE_PARSERS = {
    'AnnualSecuritiesReport': [
        ("MajorShareholders", parsers.extract_shareholder_data),
        ("ShareholderComposition", parsers.extract_shareholder_composition_data),
        ("SpecifiedInvestment", parsers.parse_specified_investment),
        ("Officer", parsers.parse_officer_information),
        ("VotingRights", parsers.parse_voting_rights),
    ],
    'LargeVolumeHoldingReport': [
        ("LargeVolumeHoldingReport", parsers.parse_large_shareholding_report),
    ],
    'BuybackStatusReport': [
        ("BuybackStatusReport", parsers.parse_buyback_status_report),
    ]
}

# --- PARSER_REGISTRYの動的生成 ---
# 上記の定義を基に、具体的な(form_code, ordinance_code)とパーサーの対応辞書を自動生成する
PARSER_REGISTRY = {}
for doc_type, parsers_list in DOC_TYPE_PARSERS.items():
    codes = DOCUMENT_TYPE_DEFINITIONS.get(doc_type, [])
    for code_tuple in codes:
        PARSER_REGISTRY[code_tuple] = parsers_list


def fetch_and_save_document(doc_id: str, ordinanceCodeShort: str) -> str | None:
    """
    指定されたdocIDの書類をAPIから取得し、CSVをファイルに保存してそのパスを返す。
    """
    zip_content = edinet_api.fetch_document(doc_id)
    if not zip_content:
        return None

    # APIから返されたコンテンツがzipファイルであるかを確認 (マジックナンバー 'PK' で判定)
    if not zip_content.startswith(b'PK'):
        # zipでなければ、APIからのエラーメッセージである可能性が高い
        try:
            error_message = zip_content.decode('utf-8')
            print(f"    Error: Content for docID {doc_id} is not a zip file. API response: {error_message}")
        except UnicodeDecodeError:
            print(f"    Error: Content for docID {doc_id} is not a zip file and could not be decoded.")
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            # 書類内のCSVファイルを探す (XBRL_TO_CSVフォルダ以下にあるものを想定)
            target_csv_name = None
            num_target = 0
            for filename in z.namelist():
                if filename.startswith(f"XBRL_TO_CSV/jp{ordinanceCodeShort}") and filename.endswith('.csv'):
                    target_csv_name = filename
                    num_target += 1
            
            if num_target > 1:
                print(f"    Warning: several files found in zip for docID {doc_id}")
            if not target_csv_name:
                print(f"    No CSV file found in XBRL_TO_CSV for docID: {doc_id}")
                return None

            # 保存先ディレクトリを作成
            save_dir = os.path.join("data", doc_id)
            os.makedirs(save_dir, exist_ok=True)
            
            # ファイルを保存
            base_filename = os.path.basename(target_csv_name)
            csv_path = os.path.join(save_dir, base_filename)

            with z.open(target_csv_name) as csv_file:
                with open(csv_path, 'wb') as f:
                    f.write(csv_file.read())
            
            return csv_path

    except zipfile.BadZipFile:
        print(f"    Error: Content for docID {doc_id} is not a valid zip file.")
        return None
    except Exception as e:
        print(f"    An error occurred while saving the file for {doc_id}: {e}")
        return None


def parse_document_file(csv_path: str, form_code: str, ordinance_code: str, ordinance_code_short: str = None) -> dict:
    """
    指定されたCSVファイルを、(form_code, ordinance_code)にもとづいて適切なパーサーで解析する。
    """
    if not os.path.exists(csv_path):
        print(f"    File not found: {csv_path}")
        return {}

    # (form_code, ordinance_code)のタプルをキーとしてパーサーを取得
    parsers_to_use = PARSER_REGISTRY.get((form_code, ordinance_code))
    if not parsers_to_use:
        return {}

    # --- ファイル読み込み処理 ---
    df = None
    encodings_to_try = ['utf-16', 'utf-8', 'cp932']
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(csv_path, encoding=encoding, sep='\t', engine='python', on_bad_lines='warn')
            break
        except Exception:
            continue
    
    if df is None:
        print(f"    Failed to read file {csv_path} with any of the attempted encodings.")
        return {}

    # --- データ抽出処理 ---
    extracted_results = {}
    for data_type_name, parser_func in parsers_to_use:
        try:
            # パーサーごとに追加の引数を渡す
            if parser_func == parsers.parse_buyback_status_report and ordinance_code_short:
                extracted_data = parser_func(df, ordinance_code=ordinance_code_short)
            elif parser_func == parsers.parse_large_shareholding_report:
                try:
                    doc_id = os.path.normpath(csv_path).split(os.sep)[1]
                    extracted_data = parser_func(df, doc_id=doc_id)
                except IndexError:
                    print(f"    Could not extract doc_id from path: {csv_path}")
                    continue
            else:
                extracted_data = parser_func(df)

            if extracted_data is not None and not extracted_data.empty:
                extracted_results[data_type_name] = extracted_data
            else:
                pass
        except Exception as e:
            print(f"    Error extracting {data_type_name}: {e}")
            
    return extracted_results


if __name__ == "__main__":
    # doc_id, form_code = "S100W9ZC", "053000"
    # doc_id, form_code = "S100W9YI", "030000"
    # doc_id, form_code = "S100W9Y3", "030000"
    doc_id, form_code = "S100W0ZR", "030000" # MS&AD

    # doc_id, form_code = "S100VWVY", "030000" # トヨタ自動車
    # doc_id, form_code = "S100WKUZ", "010000" #
    # doc_id, form_code, ordinanceCodeShort = "S100WPGX", "170000", "crp" # 網屋の自己株
    # curl "https://disclosure.edinet-fsa.go.jp/api/v2/documents/S100WKUZ?type=5&Subscription-Key=YOUR_API_KEY" -o S100WKUZ.zip

    
    
    csv_path = fetch_and_save_document(doc_id, ordinanceCodeShort)
    if csv_path:
        print(parse_document_file(csv_path, form_code = form_code)[ "BuybackStatusReport"])
        parse_document_file(csv_path, form_code = form_code)[ "BuybackStatusReport"].to_clipboard()
