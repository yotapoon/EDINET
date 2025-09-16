# EDINET データ収集ツール (EDINET Data Collector)

金融庁が提供するEDINET APIを利用して、提出された書類のメタデータや特定の報告書（CSV形式）を収集し、SQL Serverデータベースに格納するためのPythonスクリプト群です。

## 機能

*   **`collect_submission_data.py`**: 指定された期間の提出書類一覧（メタデータ）を取得し、データベースに保存します。
*   **`collect_documents.py`**: 収集したメタデータに基づき、特定の書類（例：自己株券買付状況報告書）のCSVファイルをダウンロード・処理します。

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
    プロジェクトのルートディレクトリで以下のコマンドを実行し、必要なPythonライブラリをインストールします。
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

    **注意:** `.env` ファイルは機密情報を含むため、Gitの管理対象外となっています。

4.  **データベース接続設定**
    このスクリプトはODBC DSN (`DSN=SQLServerDSN`) を使用してデータベースに接続します。お使いの環境で `SQLServerDSN` という名前のODBCデータソースを設定するか、スクリプト内の接続文字列を直接編集してください。

## 使い方

1.  **提出書類メタデータの収集**
    以下のコマンドを実行すると、データベースに未登録の日付の書類メタデータを自動で取得し、`Submission`テーブルに格納します。
    ```bash
    python collect_submission_data.py
    ```

2.  **個別書類(CSV)のダウンロード**
    以下のコマンドを実行すると、`Submission`テーブルに保存されているメタデータの中から、特定の書類（デフォルトでは「自己株券買付状況報告書」）をダウンロードし、`data`フォルダに展開します。
    ```bash
    python collect_documents.py
    ```
    *現在のスクリプトでは、ダウンロードしたCSVデータは最終的にクリップボードにコピーされます。データベースへの保存処理は実装途中です。*