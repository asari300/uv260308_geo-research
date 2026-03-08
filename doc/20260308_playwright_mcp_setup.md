# Playwright MCP のセットアップ・テスト手順

本プロジェクトにおける Streamlit アプリケーションの動作確認（ブラウザ自動テスト）を Playwright MCP 経由で行うための導入手順です。

## 1. 動作環境・前提条件

- Python 3.12 以上がインストールされていること。
- パッケージ管理ツール `uv` がセットアップされていること。
- Node.js (npm / npx) コマンドが利用可能であること（MCPサーバーの起動に使用）。

## 2. Playwright のインストール

プロジェクトルート (`/home/mermaidscandalous/monorepo/uv260308_geo-research` など) で以下のコマンドを実行し、Playwright のブラウザバイナリをインストールします。

```bash
# プロジェクト内に playwright を追加 (Python用依存関係)
uv add playwright pytest-playwright

# Playwrightが使用するブラウザ本体をインストール
uv run playwright install --with-deps
```

## 3. Playwright MCP Server の起動

AI アシスタントにブラウザを操作させるために、Playwright をバックエンドとする MCP (Minimum Context Protocol) サーバーを立ち上げる必要があります。
別タブのターミナルを開き、以下のコマンドで MCP サーバーを起動してください。

```bash
npx -y @modelcontextprotocol/server-playwright
```

## 4. MCP Server の連携と AI への指示

MCPサーバーが正常に起動したら、AI（Claude 等のコンテキスト）設定の「MCP連携」にて上記のプロセスを追加/許可します。（Cline や Cursor など、使用しているツールの設定方法に準拠して Playwright MCP を有効化してください）。

有効化後、本チャットにて以下のような指示を出すことで自動テストが可能です。

### 自動テストのプロンプト例
>
> Playwright MCP を使用して以下のテストを実行して。
>
> 1. `uv run streamlit run src/app.py` でアプリを起動
> 2. `http://localhost:8501` にアクセス
> 3. プルダウンの「モード選択」から「都道府県」を選ぶ
> 4. 「気象データ (指標一覧)」から「史上最高気温」と「史上最低気温」を切り替えて、リストが更新されるか確認する
> 5. 結果として「詳細」列の "開く 🌐" リンクがクリック可能か確認し、完了したら教えて。

上記手順で導入とテストが完了します。
