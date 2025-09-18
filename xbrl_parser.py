import io
import zipfile
import pandas as pd
import xbrr


def _find_xbrl_in_zip(zip_content: bytes) -> bytes | None:
    """
    ZIPファイルの中身(bytes)を受け取り、XBRLファイルの中身(bytes)を返すヘルパー関数。
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            # ZIPファイル内のファイルリストを探索
            for file_name in zf.namelist():
                # PublicDocフォルダ内のXBRLファイル(*.xbrl or *.ixbrl)を対象とする
                if 'PublicDoc' in file_name and (file_name.endswith('.xbrl') or file_name.endswith('.ixbrl')):
                    return zf.read(file_name)
    except zipfile.BadZipFile:
        print("Error: Failed to read ZIP file. It may be corrupted.")
        return None
    return None


def extract_major_shareholders(zip_content: bytes) -> pd.DataFrame | None:
    """
    書類のZIPファイル(bytes)から「大株主の状況」を抽出し、DataFrameで返す。
    """
    xbrl_data = _find_xbrl_in_zip(zip_content)
    if not xbrl_data:
        # print("XBRL file not found in the ZIP.")
        return None

    try:
        # xbrrライブラリでXBRLデータをパース
        xbrl_obj = xbrr.edinet.reader.read_xbrl(xbrl_data)

        # 'MajorShareholders'というタクソノミのロール（役割）を指定してデータを取得
        df = xbrl_obj.get_data_by_role("MajorShareholders")
        return df
    except Exception as e:
        # print(f"Could not parse MajorShareholders: {e}")
        return None

# --- 今後、他の情報を抽出するための関数を追加する例 ---
# def extract_stock_distribution(zip_content: bytes) -> pd.DataFrame | None:
#     """株式の分布状況を抽出する"""
#     # ... ここに実装 ...
#     pass