import pandas as pd
import edinet_api
import io
import os
import zipfile
import large_volume_holding_parser
import parsers

# formCode にもとづいて使用するパーサーを定義するレジストリ
PARSER_REGISTRY = {
    # 有価証券報告書
    '030000': [
        ("MajorShareholders", parsers.extract_shareholder_data),
        ("ShareholderComposition", parsers.extract_shareholder_composition_data),
        ("SpecifiedInvestment", parsers.parse_specified_investment),
        ("Officer", parsers.parse_officer_information),
        ("VotingRights", parsers.parse_voting_rights),
    ],
    # 大量保有報告書
    '050210': [
        ("LargeVolumeHolding", large_volume_holding_parser.extract_large_volume_holding_data),
    ],
    # 変更報告書
    '050220': [
        ("LargeVolumeHoldingChange", large_volume_holding_parser.extract_large_volume_holding_data), # 同じパーサーを仮に利用
    ],
}

def fetch_and_save_document(doc_id: str) -> str | None:
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
            for filename in z.namelist():
                if filename.startswith("XBRL_TO_CSV/jpcrp") and filename.endswith('.csv'): # 企業内容等の開示に関する内閣府令
                    target_csv_name = filename
                    break
                if filename.startswith("XBRL_TO_CSV/jpsps") and filename.endswith('.csv'):# 特定有価証券の内容等の開示に関する内閣府令	
                    target_csv_name = filename
                    break
            
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


def parse_document_file(csv_path: str, form_code: str) -> dict:
    """
    指定されたCSVファイルを、form_codeにもとづいて適切なパーサーで解析する。
    """
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return {}

    # form_codeに対応するパーサーを取得
    parsers = PARSER_REGISTRY.get(form_code)
    if not parsers:
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
    for data_type_name, parser_func in parsers:
        print(f"Attempting to extract {data_type_name}...")
        try:
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
    # curl "https://disclosure.edinet-fsa.go.jp/api/v2/documents/S100WKUZ?type=5&Subscription-Key=YOUR_API_KEY" -o S100WKUZ.zip

    
    
    csv_path = fetch_and_save_document(doc_id)
    if csv_path:
        print(parse_document_file(csv_path, form_code = form_code))