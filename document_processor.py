import pandas as pd
import edinet_api
import io
import zipfile
import shareholder_parser # Import the new parser
import shareholder_composition_parser # 新しい株主構成パーサーをインポート

# 抽出器のレジストリ
DATA_EXTRACTORS = [
    ("MajorShareholders", shareholder_parser.extract_shareholder_data),
    ("ShareholderComposition", shareholder_composition_parser.extract_shareholder_composition_data), # 新しいパーサーを追加
]

def process_document_data(doc_id: str) -> dict:
    """
    指定されたdocIDの書類をAPIから取得し、登録されたパーサーで解析して結果を辞書で返す。
    有価証券報告書（jpcrp030000）のみを処理対象とする。
    """
    print(f"Processing docID: {doc_id}")
    
    zip_content = edinet_api.fetch_document(doc_id)
    if not zip_content:
        return {}

    extracted_results = {}
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            target_csv = None
            for filename in z.namelist():
                if 'jpcrp' in filename and filename.endswith('.csv'):
                    target_csv = filename
                    break
            
            if not target_csv:
                print(f"No corporate report CSV file (jpcrp*) found in zip for docID: {doc_id}")
                return {}

            # 有価証券報告書(formCode: 030000)でなければスキップ
            if 'jpcrp030000' not in target_csv:
                print(f"Skipping non-annual report: {target_csv}")
                return {}

            print(f"Found annual report CSV: {target_csv}")

            # --- ファイル読み込み処理 ---
            with z.open(target_csv) as csv_file:
                content_bytes = csv_file.read()
                df = None
                encodings_to_try = ['utf-16', 'utf-8', 'cp932']
                
                for encoding in encodings_to_try:
                    try:
                        df = pd.read_csv(io.BytesIO(content_bytes), encoding=encoding, sep='\t')
                        print(f"Successfully read file with encoding: {encoding}")
                        break
                    except Exception: # pd.errors.ParserError も含む
                        continue
                
                if df is None: # dfがNoneのままなら、どのエンコーディングでも読み込めなかった
                    print(f"Failed to read file with any of the attempted encodings.")
                    return {}

            # --- データ抽出処理 (登録されたパーサーを呼び出す) ---
            for data_type_name, parser_func in DATA_EXTRACTORS:
                print(f"Attempting to extract {data_type_name} for {doc_id}...")
                try:
                    extracted_data = parser_func(df)
                    if extracted_data is not None and not extracted_data.empty:
                        extracted_results[data_type_name] = extracted_data
                        print(f"Successfully extracted {len(extracted_data)} records for {data_type_name}.")
                    else:
                        print(f"No {data_type_name} data found or extracted for {doc_id}.")
                except Exception as e:
                    print(f"Error extracting {data_type_name} for {doc_id}: {e}")
    
    except Exception as e:
        print(f"An error occurred while processing the file for {doc_id}: {e}")
    
    return extracted_results
