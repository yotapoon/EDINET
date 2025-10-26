# EDINET データパイプライン

EDINET APIから開示情報を取得し、整形、名寄せ処理を行った上でデータベースに格納するためのデータパイプラインです。

## 主な機能

- **メタデータ収集**: 日々の提出書類メタデータをEDINET APIから取得し、ローカルDBに蓄積します。
- **データ抽出**: 有価証券報告書や大量保有報告書など、XBRL形式で提供される書類から主要な財務・非財務情報を抽出・整形します。
- **モジュール化されたパーサー**: `parsers.py`に必要な抽出ロジックを追加するだけで、新しいデータ項目（データプロダクト）に容易に対応できます。
- **名称辞書の自動構築と名寄せ**: 提出者情報から名称のマスター辞書を自動で構築し、大株主名や投資先銘柄名の名寄せ（表記ゆれや常任代理人情報のクレンジング）を行います。
- **ユーティリティスクリプト**: 特定の種類のサンプルデータを容易に取得するためのスクリプトを提供します。

## ワークフロー概要

このパイプラインは、大きく分けて3つのステップで構成されます。

1.  **Step 1: 書類メタデータの収集**
    - `collect_submission_data.py` を実行し、日々の提出書類の基本情報（書類ID, 提出者名, 書類種別など）をDBに保存します。

2.  **Step 2: データ抽出**
    - `process_documents.py` を実行し、指定したデータプロダクト（例: `MajorShareholders`）に必要な書類をDBから特定します。
    - 対象書類をEDINET APIからダウンロードし、`parsers.py` 内の適切なパーサーを用いてデータを抽出、整形して各テーブルに保存します。

3.  **Step 3: データの名寄せ**
    - `enrich_data.py` を実行し、Step 2で抽出したデータ（例: 大株主の名称）に対して名寄せ処理を行います。
    - 書類提出者の名称リストをマスターデータとして利用し、表記ゆれを吸収した上でEDINETコードや証券コードを付与します。結果は `Enriched...` という接頭辞のテーブルに保存されます。

## セットアップ

### 1. 依存ライブラリのインストール
```bash
pip install -r requirements.txt
```

### 2. ODBCドライバのインストール
（SQL Serverを使用する場合）
お使いのOSに合わせて、SQL Serverに接続するためのODBCドライバをインストールしてください。

### 3. 環境変数の設定
`.env.example` をコピーして `.env` ファイルを作成し、APIキーとDB情報を編集します。

```bash
cp .env.example .env
```

```dotenv
# .env
EDINET_API_KEY="YOUR_API_KEY"
SERVER_NAME="your_server_name"
DATABASE_NAME="your_database_name"
```

### 4. データベースの初期化
`sql/` フォルダ内の `create_table_...` スクリプトを実行し、データ格納に必要なテーブルをDBに作成します。

## 使い方

### パイプラインの実行

```bash
# Step 1: 最新の書類メタデータを取得
python collect_submission_data.py

# Step 2: 指定したデータプロダクトを抽出し、DBに保存
# (process_documents.py内のTARGET_DATA_PRODUCTSリストを編集)
python process_documents.py

# Step 3: 指定したターゲットの名寄せ処理を実行
# (enrich_data.py内のENRICHMENT_TARGETSと実行モードを設定)
python enrich_data.py
```

### ユーティリティスクリプト

#### サンプルCSVデータセットの取得

`get_sample_document.py` を使うと、指定したデータプロダクトに関連する書類を最大100件まで取得し、生のCSVデータを結合して1つのファイルとして `samples/` ディレクトリに保存します。パーサーを新規開発する際のデータ分析に役立ちます。

```bash
# 「大株主」に関連する書類のデータセットを作成
python get_sample_document.py MajorShareholders

# 「株式公開買付」に関連する書類のデータセットを作成
python get_sample_document.py TenderOffer
```

## プロジェクト構成

```
./
├── .env.example
├── README.md
├── requirements.txt
|
├── collect_submission_data.py  # [Step 1] 書類メタデータ収集
├── process_documents.py        # [Step 2] データ抽出処理の実行
├── enrich_data.py              # [Step 3] 名寄せ処理の実行
|
├── config.py                   # 設定管理
├── definitions.py              # データプロダクトと書類種別の定義
├── database_manager.py         # DB操作
├── edinet_api.py               # EDINET API通信
├── document_processor.py       # 書類ダウンロードとパーサーの振り分け
├── parsers.py                  # データ抽出ロジック
├── matching.py                 # 名寄せロジック
|
├── get_sample_document.py      # [Util] サンプルデータ取得スクリプト
├── analyze_enrichment_accuracy.py # [Util] 名寄せ精度分析スクリプト
|
└── sql/                        # テーブル作成用SQL
```

## 新しいデータ抽出処理の追加方法

このプロジェクトは、新しい種類のデータ（データプロダクト）を簡単に追加できるように設計されています。株式公開買付（TenderOffer）を追加した際の手順は以下の通りです。

1.  **`definitions.py` の更新**
    - `DOCUMENT_TYPE_DEFINITIONS` に新しい書類種別（例: `TenderOfferDocuments`）と、それに対応する `(formCode, ordinanceCode)` のリストを追加します。
    - `DATA_PRODUCT_DEFINITIONS` に新しいデータプロダクト名（例: `TenderOffer`）を追加し、先ほど定義した書類種別にマッピングします。

2.  **DBテーブルの作成**
    - `sql/` ディレクトリに、新しいデータを格納するための `create_table_... .sql` ファイルを作成します。

3.  **`parsers.py` へのパーサー追加**
    - `get_sample_document.py` を使ってサンプルCSVを取得し、データ構造（特に `要素ID`）を分析します。
    - `parsers.py` に、新しいデータを抽出・整形するための関数（例: `parse_tender_offer`）を追加します。

4.  **`document_processor.py` へのパーサー登録**
    - `DOC_TYPE_PARSERS` 辞書に、新しい書類種別と、ステップ3で作成したパーサー関数を紐づけます。

以上の手順で、パイプラインに新しいデータ抽出処理を組み込むことができます。