import sys
import os
import io
import zipfile
import time
import pandas as pd
from tqdm import tqdm
import database_manager
import edinet_api
from definitions import DOCUMENT_TYPE_DEFINITIONS, DATA_PRODUCT_DEFINITIONS

def find_target_documents(target_data_product: str, limit: int = 100):
    """
    指定されたデータプロダクトに合致する最新の書類をDBから見つける。
    """
    print(f"--- Searching for documents related to data product: '{target_data_product}' ---")

    doc_type = DATA_PRODUCT_DEFINITIONS.get(target_data_product)
    if not doc_type:
        print(f"Error: Data product '{target_data_product}' is not defined in definitions.py.")
        return []

    codes_to_fetch = DOCUMENT_TYPE_DEFINITIONS.get(doc_type)
    if not codes_to_fetch:
        print(f"Error: Document type '{doc_type}' has no associated codes in definitions.py.")
        return []

    print(f"Found document type '{doc_type}'. Searching for documents with form/ordinance codes: {codes_to_fetch}")

    documents = database_manager.get_documents_by_codes(codes_to_fetch)
    if not documents:
        print("Error: No matching document with a CSV file found in the database.")
        return []

    # 上位limit件に絞る
    return documents[:limit]

def main():
    if len(sys.argv) < 2:
        print("Usage: python get_sample_document.py <DataProductName>")
        print("Example: python get_sample_document.py MajorShareholders")
        print("\nAvailable data products:")
        for product in DATA_PRODUCT_DEFINITIONS.keys():
            print(f"- {product}")
        sys.exit(1)
    
    target_data_product = sys.argv[1]

    documents_to_process = find_target_documents(target_data_product, limit=100)
    if not documents_to_process:
        sys.exit(1)

    print(f"\nFound {len(documents_to_process)} documents to process. Starting download and processing...")

    all_dfs = []
    for doc_info in tqdm(documents_to_process, desc="Processing Documents"):
        date_file, doc_id, form_code, ordinance_code, ordinance_code_short, seq_number = doc_info
        
        zip_content = edinet_api.fetch_document(doc_id)
        if not zip_content or not zip_content.startswith(b'PK'):
            tqdm.write(f"Warning: Failed to fetch a valid zip file for docID: {doc_id}. Skipping.")
            time.sleep(1)
            continue
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
                target_csv_name = None
                for filename in z.namelist():
                    if filename.startswith('XBRL_TO_CSV/') and filename.endswith('.csv'):
                        target_csv_name = filename
                        break
                
                if not target_csv_name:
                    tqdm.write(f"Warning: No CSV file found in zip for docID: {doc_id}. Skipping.")
                    continue

                with z.open(target_csv_name) as csv_file:
                    # ファイルを読み込む際のエンコーディングを試す
                    df = None
                    encodings_to_try = ['utf-16', 'utf-8', 'cp932']
                    # BytesIOを介してエンコーディングを指定
                    csv_content = csv_file.read()
                    for encoding in encodings_to_try:
                        try:
                            df = pd.read_csv(io.BytesIO(csv_content), encoding=encoding, sep='\t', engine='python', on_bad_lines='warn')
                            break # 成功したらループを抜ける
                        except Exception:
                            # seek(0)はBytesIOには不要
                            pass
                    
                    if df is None:
                        tqdm.write(f"Warning: Failed to read CSV for docID {doc_id} with any encoding. Skipping.")
                        continue

                    # 必須カラムを追加
                    df['docId'] = doc_id
                    df['formCode'] = form_code
                    df['ordinanceCode'] = ordinance_code
                    all_dfs.append(df)

        except Exception as e:
            tqdm.write(f"An error occurred while processing docID {doc_id}: {e}")
        finally:
            # APIサーバーへの負荷を考慮
            time.sleep(1)

    if not all_dfs:
        print("\nNo data was successfully processed. Exiting.")
        sys.exit(1)

    # すべてのDataFrameを結合
    final_df = pd.concat(all_dfs, ignore_index=True)

    # 保存
    save_dir = "samples"
    os.makedirs(save_dir, exist_ok=True)
    output_filename = f"{target_data_product}_dataset_{len(documents_to_process)}docs.csv"
    output_path = os.path.join(save_dir, output_filename)
    
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\nSuccessfully created dataset with {len(final_df)} rows.")
    print(f"Saved to: {os.path.abspath(output_path)}")

if __name__ == "__main__":
    main()