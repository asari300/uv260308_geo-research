# Goal Description
現在のリポジトリをリファクタリングし、`pandas` から `polars.LazyFrame` を使用するように変更します。また、気象庁の実際の地点名称（実データ）に基づいてデータを構成し、ソースコードを `src/` ディレクトリに整理し、JMAの地点へのリンクを追加します。

## Proposed Changes
### Project Structure
- `src/` ディレクトリを作成し、モジュールを配置します。
- `data/` ディレクトリを作成し、csv を配置します。

### [NEW] `src/` modules
- `src/data_generator.py`: JMA の Web サイトから地点リスト (`prec_no`, `block_no`, 地点名, 都道府県名) をスクレイピングなどで取得し、現在のでたらめな市町村名ではなく実際の気象庁の地点名を使用して `weather_data.csv` を生成するように変更します。気象庁にデータのない架空の地点は含まれなくなり、要件を満たします。
- `src/app.py`: Streamlit アプリのメインコード。`pandas` ではなく `polars.LazyFrame` を使用してデータを読み込み・処理します。

### [MODIFY] `main.py`
これからは `src/app.py` を呼び出すエントリーポイントにするか、単に削除して `app.py` を新たなエントリーポイントにします（今回は `src/app.py` を Streamlit から起動する想定で進めますが、便宜上 `main.py` で `src` 内の関数を呼ぶようにもできます）。

### JMA Data Fetching
気象庁の `https://www.data.jma.go.jp/stats/etrn/select/prefecture00.php` などをクロールし、`prec_no` と `block_no`、都道府県名、地点名を取得するスクリプトを `src/jma_scraper.py` に作成し、そこから `weather_data.csv` のベースとなる地点を作成。実在しない市町村は自然と生成されなくなります。

### DataFrame Modifications
- データのヘッダから「月」を削除 ("01月" -> "01")
- `polars.scan_csv("data/weather_data.csv")` (LazyFrame) を使用。
- Streamlit の `st.dataframe` の `column_config` オプションを使って地点名セルをマークダウンリンク (`[地点名](URL)`) に変換することで、クリックして飛べるようにします。

## Verification Plan
1. `src/jma_scraper.py` (または同等のロジック) を実行し、実際の地点リストが取得できるかテスト。
2. `src/data_generator.py` を実行し、`data/weather_data.csv` が正しく作られるかテスト。
3. `streamlit run main.py` または `src/app.py` を実行し、`polars` で正常に処理され、ヘッダの文字が消え、リンクが機能しているか動作確認。
