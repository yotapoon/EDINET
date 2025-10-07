# EDINET データ収集・分析ツール

このプロジェクトは、EDINET APIを利用して金融商品取引法に基づく開示書類の情報を収集し、XBRLデータから特定の財務・非財務情報を抽出して整形するためのツールです。

## 主な機能

- **提出書類一覧の取得**: 指定した期間の日々の提出書類メタデータをEDINET API v2から取得し、データベースに保存します。
- **詳細情報の抽出**: 提出書類の中から対象の書類（有価証券報告書、大量保有報告書など）をダウンロードし、XBRLをCSV形式に変換して読み込みます。
- **モジュール化されたデータ抽出**: `parsers.py` に含まれる情報タイプごとのパーサーを用いて、以下の情報を抽出・整形します。
    - 大株主の状況
    - 株主構成
    - 役員の状況
    - 政策保有株式
    - 議決権の状況
    - 大量保有報告書
    - 自己株券買付状況報告書
- **データのエクスポート**: 抽出したデータはDataFrameとして返され、さらなる分析やデータベースへの保存が可能です。

## ディレクトリ構成

```
./
├── collect_submission_data.py  # 日々の書類一覧を取得・DB保存するスクリプト
├── process_documents.py        # 書類詳細を抽出し、情報別に処理するメインスクリプト
├── edinet_api.py               # EDINET APIとの通信を担当するモジュール
├── database_manager.py         # データベース接続と操作を担当するモジュール
├── document_processor.py       # 個別書類の読込とパーサー呼出を管理するモジュール
├── parsers.py                  # 各種データ抽出ロジック（パーサー）を格納するモジュール
├── config.py                   # APIキーやDB接続情報などの設定を管理するモジュール
├── requirements.txt            # 依存ライブラリ一覧
├── .env.example                # 環境変数ファイルの見本
├── sql/                        # (オプション) テーブル作成などのSQLファイルを格納
└── README.md                   # このファイル
```

## セットアップ

### 1. リポジトリのクローン
```bash
git clone <repository_url>
cd <project_directory>
```

### 2. 依存ライブラリのインストール
`requirements.txt` を使用して、必要なライブラリをインストールします。

```bash
pip install -r requirements.txt
```

### 3. ODBCドライバのインストール
（SQL Serverを使用する場合）
お使いのOSに合わせて、SQL Serverに接続するためのODBCドライバをインストールしてください。

### 4. 環境変数の設定
`.env.example` をコピーして `.env` ファイルを作成し、お使いの環境に合わせて内容を編集します。

```bash
cp .env.example .env
```

`.env` ファイルに必要な情報を記述します。
```dotenv
# .env

# EDINET API v2のサブスクリプションキー
EDINET_API_KEY="YOUR_API_KEY"

# データベース情報 (pyodbc用)
# config.py の接続文字列に合わせて適宜変更してください
SERVER_NAME="your_server_name"
DATABASE_NAME="your_database_name"
```

## 使い方

### Step 1: 提出書類一覧の取得
`collect_submission_data.py` を実行して、指定した期間の書類メタデータをデータベースに保存します。スクリプト内の `start_date` と `end_date` を適宜変更してください。

```bash
python collect_submission_data.py
```

### Step 2: 書類詳細情報の抽出
`process_documents.py` を実行して、Step 1で保存したデータの中から対象の書類を抽出し、詳細情報を取得します。スクリプト内の `target_date` などを変更することで、処理対象を絞り込めます。

```bash
python process_documents.py
```

## 各モジュールの役割

- **`collect_submission_data.py`**: EDINET APIから指定期間の提出書類メタデータを取得し、DBに保存します。
- **`process_documents.py`**: DBから処理対象の書類を特定し、`document_processor` を使って詳細情報を抽出する実行スクリプトです。
- **`edinet_api.py`**: EDINET APIへのリクエスト（書類一覧取得、書類ダウンロード）を抽象化します。
- **`database_manager.py`**: `SQLAlchemy` を用いてデータベースエンジンを管理し、データの読み書きを担います。
- **`config.py`**: `.env` ファイルから設定を読み込み、プロジェクト全体で利用できる定数として提供します。
- **`document_processor.py`**: 書類IDを受け取り、書類のダウンロード、CSV変換、および `parsers.py` 内の適切なパーサーの呼び出しを管理します。
- **`parsers.py`**: XBRL(CSV)データから具体的な情報を抽出する関数群です。以下のパーサーが含まれます。
    - `extract_shareholder_data`: 大株主の状況
    - `extract_shareholder_composition_data`: 株主の構成
    - `parse_officer_information`: 役員の状況
    - `parse_specified_investment`: 政策保有株式
    - `parse_voting_rights`: 議決権の状況
    - `extract_large_volume_holding_data`: 大量保有報告書
    - `parse_buyback_status_report`: 自己株券買付状況報告書

## 今後の開発予定 (TODO)

-   **データベースへの保存**: 抽出した各データを、正規化されたデータベーステーブルに保存する機能の実装。
-   **エラーハンドリングの強化**: API通信、ファイルI/O、データ抽出におけるエラー処理をより堅牢にします。
-   **データ検証とクリーンアップ**: 抽出データの妥当性検証（例: 割合の合計が100%になるか）や、整形処理を追加します。
-   **ロギング**: `print`文の代わりに、`logging`モジュールを導入し、処理状況やエラーを詳細に記録できるようにします。