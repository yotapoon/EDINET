# EDINET データ収集ツール (EDINET Data Collector)

金融庁が提供するEDINET API v2を利用して、提出された書類のメタデータや特定の報告書（CSV形式）を収集し、SQL Serverデータベースに格納するためのPythonスクリプト群です。

## 概要

このツールは、2つの主要なスクリプトで構成されています。

1.  **メタデータ収集**: `collect_submission_data.py` を実行すると、EDINETで日々公開される提出書類のメタデータ（いつ、誰が、どんな書類を提出したか等）を取得し、SQL Serverの`Submission`テーブルに保存します。過去10年間のうち、データベースに未登録の日付のデータを自動で収集します。
2.  **個別書類収集**: `collect_documents.py` を実行すると、`Submission`テーブルに保存されたメタデータの中から、特定の書類（デフォルトでは「自己株券買付状況報告書」）を対象に、そのCSVファイルをダウンロード・展開します。

## コンポーネント

*   `collect_submission_data.py`: 提出書類メタデータをEDINETから取得し、DBに格納するスクリプト。
*   `collect_documents.py`: 特定の書類のCSVファイルをダウンロードし、処理するスクリプト。
*   `edinet_utils.py`: 環境変数やDB接続設定、APIリクエストなど、共通の機能を提供するモジュール。
*   `.env.example`: 環境変数のテンプレートファイル。
*   `requirements.txt`: 依存ライブラリリスト。
*   `data/`: ダウンロードした書類のZIPファイルが展開されるディレクトリ。

## 動作環境

*   Python 3.x
*   Microsoft SQL Server
*   SQL Serverへの接続用ODBCデータソース名(DSN)

## セットアップ手順

1.  **リポジトリのクローン**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **依存ライブラリのインストール**
    ```bash
    pip install -r requirements.txt
    ```

3.  **環境変数の設定**
    `.env.example` ファイルをコピーして `.env` ファイルを作成します。
    ```bash
    cp .env.example .env
    ```
    作成した `.env` ファイルをエディタで開き、ご自身の環境に合わせて以下の値を設定してください。
    *   `EDINET_API_KEY`: EDINET APIの認証キー
    *   `SERVER_NAME`: 接続するSQL Serverのサーバー名
    *   `DATABASE_NAME`: 使用するデータベース名

4.  **データベースの準備**
    *   接続先のSQL Serverに、`Submission`テーブルを作成してください。このテーブルはメタデータを格納するために使用されます。（テーブル作成用のSQLは`sql/`ディレクトリなどを参照してください）
    *   このテーブルには、`docID` (主キー), `dateFile`, `docTypeCode`, `filerName` などのカラムの他に、個別書類のダウンロード管理用フラグとして `FlagLoadCsv` (integer) が必要です。

5.  **データベース接続設定**
    このスクリプトはODBC DSN (`DSN=SQLServerDSN`) を使用してデータベースに接続します。お使いの環境で `SQLServerDSN` という名前のODBCデータソースを設定してください。

## ワークフロー（使い方）

### Step 1: 提出書類メタデータの収集

以下のコマンドを実行すると、データベースに未登録の日付の書類メタデータを自動で取得し、`Submission`テーブルに格納します。この作業を定期的に実行することで、日々の提出書類情報を蓄積できます。

```bash
python collect_submission_data.py
```

### Step 2: 個別書類(CSV)のダウンロードと処理

以下のコマンドを実行すると、`Submission`テーブルに保存されているメタデータの中から、特定の書類をダウンロードします。

```bash
python collect_documents.py
```

**現在のスクリプトの動作:**
*   **対象書類**: `docTypeCode = '230'`（自己株券買付状況報告書）のうち、まだ処理されていないもの（`FlagLoadCsv = 0`）が対象となります。
*   **処理件数**: 処理対象が多数ある場合、先頭から100件の書類を処理します。
*   **処理内容**:
    1.  対象書類のZIPファイルをダウンロードし、`data/<docID>/` 以下に展開します。
    2.  展開されたCSVファイル（`XBRL_TO_CSV/*.csv`）を読み込みます。
    3.  読み込んだ全件のデータを一つのデータフレームに結合し、**クリップボードにコピーします。**

**注意:** 現在のバージョンでは、収集したCSVデータはデータベースに保存されず、クリップボードにコピーされる仕様です。データベースへの保存処理は実装途中です。

## カスタマイズ

収集したい書類の種類を変更するには、`collect_documents.py`の以下の行を編集します。

```python
# 変更前
docID_list = collect_docID_to_load("230") # 自己株券買付状況報告書 

# 変更後 (例: 有価証券報告書 '120')
# docID_list = collect_docID_to_load("120") 
```
