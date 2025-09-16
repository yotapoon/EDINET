
import os
import zipfile
import glob
import requests
import pandas as pd
from edinet_utils import engine, TABLE_NAME, API_KEY, DATA_FOLDER

def download_and_extract_zip(docID):
    """
    指定されたdocIDのZIPファイルをダウンロードし、解凍してCSVファイルを取得する関数
    """
    url = f'https://api.edinet-fsa.go.jp/api/v2/documents/{docID}?type=5&Subscription-Key={API_KEY}'  # type=5はcsv
    zip_path = os.path.join(DATA_FOLDER, f"{docID}.zip")
    extract_folder = os.path.join(DATA_FOLDER, docID)

    try:
        print(f"Downloading ZIP file for {docID}...")
        response = requests.get(url)
        if response.status_code == 200:
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"ZIP file {zip_path} downloaded successfully.")

            # ZIPファイルを解凍
            if zipfile.is_zipfile(zip_path):
                with zipfile.ZipFile(zip_path) as zip_f:
                    zip_f.extractall(extract_folder)
                print(f"Extracted {docID} in {extract_folder}")
                os.remove(zip_path)  # ZIPファイルを削除
                print(f"ZIP file {zip_path} deleted.")
                return extract_folder
            else:
                print(f"{zip_path} is not a valid ZIP file.")
                return None
        else:
            print(f"Failed to download ZIP file for {docID}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading or extracting ZIP file for {docID}: {e}")
        return None

def collect_docID_to_load(docTypeCode):
    """
    指定されたdocTypeCodeのドキュメントIDを取得し、FlagLoadCsvが0のものを返す関数
    """
    with engine.connect() as conn:
        query = f"SELECT DISTINCT docID FROM {TABLE_NAME} WHERE docTypeCode = '{docTypeCode}' AND FlagLoadCsv = 0"
        docIDs_to_load = pd.read_sql(query, conn)['docID'].tolist()

    return docIDs_to_load


def upload_csv_to_sql_and_delete(docID, csv_file_path):
    """
    CSVファイルをSQLにアップロードし、アップロード後にファイルを削除する関数
    """
    try:
        # CSVファイルを読み込む
        df = pd.read_csv(csv_file_path, encoding="utf-8")
        print(df)
        
        # データをSQLにアップロード
        # df.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)
        # print(f"Data for docID {docID} uploaded successfully. Number of records: {len(df)}")
        
        # アップロード後にCSVファイルを削除
        os.remove(csv_file_path)
        print(f"CSV file {csv_file_path} deleted successfully.")
    except Exception as e:
        print(f"Error processing CSV file {csv_file_path}: {e}")

if __name__ == "__main__":
    docID_list = collect_docID_to_load("230") # 自己株券買付状況報告書 
    print(f"Collecting data for {len(docID_list)} new docIDs.")
    # upload_submission("2025-07-16")  # Example date to upload data for
    docID_list_sample = docID_list[0:100]
    df = pd.DataFrame()
    for docID in docID_list_sample:
        print(f"Processing docID: {docID}")
        extract_folder = download_and_extract_zip(docID)
        csv_file_path = f'./data/{docID}/XBRL_TO_CSV/*.csv'
        csv_file = glob.glob(csv_file_path)[0]
        df_temp = pd.read_csv(csv_file, encoding="utf-16",sep="\t")
        df_temp['docID'] = docID
        df = pd.concat([df, df_temp], ignore_index=True)
        
    df.to_clipboard(index=False)

    '''
    if csv_file_path:
        upload_csv_to_sql_and_delete(docID, csv_file_path)
    '''
    
    extract_folder = os.path.join(DATA_FOLDER, docID)

    try:
        print(f"Downloading ZIP file for {docID}...")
        response = requests.get(url)
        if response.status_code == 200:
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"ZIP file {zip_path} downloaded successfully.")

            # ZIPファイルを解凍
            if zipfile.is_zipfile(zip_path):
                with zipfile.ZipFile(zip_path) as zip_f:
                    zip_f.extractall(extract_folder)
                print(f"Extracted {docID} in {extract_folder}")
                os.remove(zip_path)  # ZIPファイルを削除
                print(f"ZIP file {zip_path} deleted.")
                return extract_folder
            else:
                print(f"{zip_path} is not a valid ZIP file.")
                return None
        else:
            print(f"Failed to download ZIP file for {docID}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading or extracting ZIP file for {docID}: {e}")
        return None

def collect_docID_to_load(docTypeCode):
    """
    指定されたdocTypeCodeのドキュメントIDを取得し、FlagLoadCsvが0のものを返す関数
    """
    with engine.connect() as conn:
        query = f"SELECT DISTINCT docID FROM {TABLE_NAME} WHERE docTypeCode = '{docTypeCode}' AND FlagLoadCsv = 0"
        docIDs_to_load = pd.read_sql(query, conn)['docID'].tolist()

    return docIDs_to_load


def upload_csv_to_sql_and_delete(docID, csv_file_path):
    """
    CSVファイルをSQLにアップロードし、アップロード後にファイルを削除する関数
    """
    try:
        # CSVファイルを読み込む
        df = pd.read_csv(csv_file_path, encoding="utf-8")
        print(df)
        
        # データをSQLにアップロード
        # df.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)
        # print(f"Data for docID {docID} uploaded successfully. Number of records: {len(df)}")
        
        # アップロード後にCSVファイルを削除
        os.remove(csv_file_path)
        print(f"CSV file {csv_file_path} deleted successfully.")
    except Exception as e:
        print(f"Error processing CSV file {csv_file_path}: {e}")

if __name__ == "__main__":
    docID_list = collect_docID_to_load("230") # 自己株券買付状況報告書 
    print(f"Collecting data for {len(docID_list)} new docIDs.")
    # upload_submission("2025-07-16")  # Example date to upload data for
    docID_list_sample = docID_list[0:100]
    df = pd.DataFrame()
    for docID in docID_list_sample:
        print(f"Processing docID: {docID}")
        extract_folder = download_and_extract_zip(docID)
        csv_file_path = f'./data/{docID}/XBRL_TO_CSV/*.csv'
        csv_file = glob.glob(csv_file_path)[0]
        df_temp = pd.read_csv(csv_file, encoding="utf-16",sep="\t")
        df_temp['docID'] = docID
        df = pd.concat([df, df_temp], ignore_index=True)
        
    df.to_clipboard(index=False)

    '''
    if csv_file_path:
        upload_csv_to_sql_and_delete(docID, csv_file_path)
    '''
    
