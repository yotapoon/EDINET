# EDINET Data Collector and Parser

EDINET API v2 を利用して、提出された書類のメタデータやXBRLファイルを取得し、解析するためのツール群です。

## 機能概要

このリポジトリは、以下のPythonスクリリプトで構成されています。

- **`collect_submission_data.py`**
  - 指定した日付に提出された書類のメタデータ一覧をEDINET APIから取得し、JSONファイルとして保存します。

- **`collect_documents.py`**
  - `collect_submission_data.py`で取得したメタデータに基づき、提出書類本体（XBRLファイルなどを含むZIPファイル）をダウンロードします。
  - **(注意: このスクリプトは今後修正が予定されています)**

- **`edinet_parser.py`**
  - ダウンロードしたXBRLファイル（ZIP展開後）を解析し、企業名や証券コードなどの情報を抽出するパーサーを提供します。

- **`edinet_utils.py`**
  - EDINET APIへのリクエスト送信やファイル保存など、各スクリリプトで共通して利用されるユーティリティ関数を提供します。

## 依存関係

このプロジェクトは以下のライブラリに依存しています。

- `requests`
- `lxml`

以下のコマンドでインストールできます。
```bash
pip install requests lxml
```

## 使い方

### 1. 提出書類メタデータの取得

`collect_submission_data.py` を実行して、特定の日付に提出された書類のメタデータを取得します。

```bash
python collect_submission_data.py --date YYYY-MM-DD --output_dir path/to/save/metadata
```

### 2. 提出書類本体のダウンロード

`collect_documents.py` を実行して、メタデータに対応する書類本体（ZIPファイル）をダウンロードします。

```bash
python collect_documents.py --input_dir path/to/save/metadata --output_dir path/to/save/documents
```

### 3. XBRLファイルの解析 (利用例)

`edinet_parser.py` を利用して、ダウンロード・展開したXBRLファイルから情報を抽出できます。

```python
from edinet_parser import EdinetParser

# XBRLファイルのパスを指定
parser = EdinetParser("path/to/your/XBRL/file.xbrl")

company_name = parser.get_company_name()
sec_code = parser.get_sec_code()

print(f"企業名: {company_name}")
print(f"証券コード: {sec_code}")
```