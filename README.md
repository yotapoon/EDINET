# EDINET データ収集・分析ツール

このプロジェクトは、EDINET APIを利用して金融商品取引法に基づく開示書類の情報を収集し、特定の情報を抽出してデータベースに保存するためのツールです。

## 主な機能

- **提出書類一覧の取得**: 指定した期間の日々の提出書類メタデータをEDINET API v2から取得し、データベースに保存します。
- **詳細情報の抽出**: 提出書類の中から特定の書類（例：有価証券報告書）を対象とし、書類本体（XBRL）をダウンロードします。
- **データ整形と保存**: XBRLファイルから「大株主の状況」などの特定の情報を抽出し、整形して専用のデータベーステーブルに格納します。

## ファイル構成

プロジェクトは、機能ごとにファイルが分割されています。

```
edinet_project/
├── collect_submission_data.py  # 日々の書類一覧を取得・DB保存するスクリプト
├── process_documents.py        # 書類詳細を抽出し、情報別にDB保存するスクリプト
├── edinet_api.py               # EDINET APIとの通信を担当するモジュール
├── xbrl_parser.py              # XBRLファイルの解析とデータ整形を担当するモジュール
├── database_manager.py         # データベース接続と操作を担当するモジュール
├── config.py                   # APIキーやDB接続情報などの設定を管理するモジュール
├── .env                        # APIキーなどの機密情報を格納するファイル (Git管理外)
└── README.md                   # このファイル
```

### 各ファイルの役割

- **`collect_submission_data.py`**
  - 指定された期間の提出書類メタデータ一覧をEDINET APIから取得します。
  - 取得した一覧を `Submission` テーブルに保存します。
  - 定期的に実行し、日々の提出状況を蓄積することを想定しています。

- **`process_documents.py`**
  - `Submission` テーブルから処理対象の書類（例：有価証券報告書）を特定します。
  - 対象書類のXBRLファイルをダウンロードし、`xbrl_parser.py` を使って情報を抽出します。
  - 抽出した情報を情報別のテーブル（例：`major_shareholders`）に保存します。

- **`edinet_api.py`**
  - EDINET APIへのリクエスト（書類一覧取得、書類ダウンロード）を抽象化します。

- **`xbrl_parser.py`**
  - `xbrr` ライブラリを利用して、XBRLファイルから特定の情報（例：「大株主の状況」）をDataFrameとして抽出します。
  - 新しい情報を抽出したい場合は、このファイルに関数を追加します。

- **`database_manager.py`**
  - `SQLAlchemy` を用いてデータベースエンジンを管理します。
  - 各種DataFrameを対応するテーブルに保存する関数を提供します。

- **`config.py`**
  - `.env` ファイルから設定を読み込み、プロジェクト全体で利用できるように定数として提供します。

- **`.env`**
  - APIキーやデータベースの接続情報など、外部に公開すべきでない値を記述します。

## 必要なライブラリ

このプロジェクトを実行するには、以下のライブラリが必要です。

```
pandas
requests
sqlalchemy
pyodbc
python-dotenv
xbrr
tqdm
```

以下のコマンドで一括インストールできます。

```bash
pip install pandas requests sqlalchemy pyodbc python-dotenv xbrr tqdm
```

## セットアップ

1.  **リポジトリのクローン**:
    ```bash
    git clone <repository_url>
    cd edinet_project
    ```

2.  **ODBCドライバのインストール**:
    使用しているOSに合わせて、SQL Serverに接続するためのODBCドライバをインストールしてください。

3.  **`.env` ファイルの作成**:
    プロジェクトのルートに `.env` ファイルを作成し、以下のように環境変数を設定します。

    ```.env
    # .env.example

    # EDINET API v2のサブスクリプションキー
    EDINET_API_KEY="YOUR_API_KEY"

    # データベース情報
    SERVER_NAME="your_server_name"
    DATABASE_NAME="your_database_name"
    ```

4.  **DSNの設定**:
    `config.py` 内の `CONNECTION_STRING` でDSN（データソース名）を指定しています。お使いの環境に合わせてODBCデータソースアドミニストレーターで `SQLServerDSN` という名前のDSNを設定するか、`config.py` の接続文字列を直接編集してください。

## 使い方

処理は2つのステップで行います。

### Step 1: 提出書類一覧の取得

まず、`collect_submission_data.py` を実行して、指定した期間の書類メタデータをデータベースに保存します。スクリプト内の `start_date` と `end_date` を適宜変更してください。

```bash
python collect_submission_data.py
```

### Step 2: 書類詳細情報の抽出

次に、`process_documents.py` を実行して、Step 1で保存したデータの中から対象の書類を抽出し、詳細情報を取得します。スクリプト内の `target_date` や `target_form_code` を変更することで、処理対象を絞り込めます。

```bash
python process_documents.py
```