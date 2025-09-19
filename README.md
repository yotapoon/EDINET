# EDINET データ収集・分析ツール

このプロジェクトは、EDINET APIを利用して金融商品取引法に基づく開示書類の情報を収集し、特定の情報を抽出してデータベースに保存するためのツールです。

## 主な機能

- **提出書類一覧の取得**: 指定した期間の日々の提出書類メタデータをEDINET API v2から取得し、データベースに保存します。
- **詳細情報の抽出**: 提出書類の中から特定の書類（例：有価証券報告書）を対象とし、書類本体（XBRLから変換されたCSV形式）をダウンロードします。
- **モジュール化されたデータ抽出**: 「大株主の状況」や「株主構成」など、情報タイプごとにモジュール化されたパーサーを用いてデータを抽出し、整形します。
- **データ整形と保存**: 抽出した情報をデータベースに格納するための準備を行います。

## ファイル構成

プロジェクトは、機能ごとにファイルが分割されています。

```
./
├── collect_submission_data.py        # 日々の書類一覧を取得・DB保存するスクリプト
├── process_documents.py              # 書類詳細を抽出し、情報別に処理するメインスクリプト
├── edinet_api.py                     # EDINET APIとの通信を担当するモジュール
├── database_manager.py               # データベース接続と操作を担当するモジュール
├── config.py                         # APIキーやDB接続情報などの設定を管理するモジュール
├── document_processor.py             # 個別書類の読み込みと、登録されたパーサーの呼び出しを管理するモジュール
├── shareholder_parser.py             # 「大株主の状況」のデータ抽出を担当するモジュール
├── shareholder_composition_parser.py # 「株主構成」のデータ抽出を担当するモジュール
├── .env                              # APIキーなどの機密情報を格納するファイル (Git管理外)
└── README.md                         # このファイル
```

### 各ファイルの役割

- **`collect_submission_data.py`**
  - 指定された期間の提出書類メタデータ一覧をEDINET APIから取得します。
  - 取得した一覧を `Submission` テーブルに保存します。
  - 定期的に実行し、日々の提出状況を蓄積することを想定しています。

- **`process_documents.py`**
  - `Submission` テーブルから処理対象の書類（例：有価証券報告書）を特定します。
  - `document_processor.py` を使って書類の詳細情報を取得・抽出し、結果を表示します。

- **`edinet_api.py`**
  - EDINET APIへのリクエスト（書類一覧取得、書類ダウンロード）を抽象化します。

- **`database_manager.py`**
  - `SQLAlchemy` を用いてデータベースエンジンを管理します。
  - 各種DataFrameを対応するテーブルに保存する関数を提供します。

- **`config.py`**
  - `.env` ファイルから設定を読み込み、プロジェクト全体で利用できるように定数として提供します。

- **`document_processor.py`**
  - 個別の書類ID（`docID`）を受け取り、EDINET APIから書類をダウンロードし、CSV形式で読み込みます。
  - 登録された複数のパーサー（`shareholder_parser.py`など）を呼び出し、様々な種類のデータを抽出します。
  - 抽出結果をデータタイプごとの辞書として返します。

- **`shareholder_parser.py`**
  - 読み込まれたDataFrameから「大株主の状況」に関するデータを抽出・整形します。

- **`shareholder_composition_parser.py`**
  - 読み込まれたDataFrameから「株主構成」に関するデータを抽出・整形します。

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
```

以下のコマンドで一括インストールできます。

```bash
pip install pandas requests sqlalchemy pyodbc python-dotenv
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

次に、`process_documents.py` を実行して、Step 1で保存したデータの中から対象の書類を抽出し、詳細情報を取得します。スクリプト内の `target_date` を変更することで、処理対象を絞り込めます。

```bash
python process_documents.py
```

## 今後の開発予定 (TODO)

-   **データベースへの保存**: 抽出した「大株主の状況」や「株主構成」などのデータを、適切なデータベーステーブルに保存する機能を実装します。
-   **エラーハンドリングの強化**: API通信、ファイル読み込み、データ抽出におけるエラー処理をより堅牢にします。
-   **データ検証とクリーンアップ**: 抽出されたデータの妥当性検証（例: 割合が0-100%の範囲内か）や、整形処理を追加します。
-   **設定の外部化**: ターゲット日付や出力パスなど、より多くの設定項目を`config.py`などの設定ファイルで管理できるようにします。
-   **ロギング**: `print`文の代わりに、適切なロギング機構を導入し、処理状況やエラーを詳細に記録できるようにします。
-   **その他のデータ抽出**: 「大量保有報告書」など、他の種類の開示書類や情報からのデータ抽出機能を追加します。
