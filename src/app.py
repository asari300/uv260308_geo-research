"""
Streamlitメインアプリケーションモジュール。
Main Streamlit application module.

This module initializes the Streamlit app, handles user inputs, and displays
the ranking of weather data using polars.LazyFrame operations.
"""

import streamlit as st
import polars as pl
import os

st.set_page_config(page_title="日本の気象データランキング", layout="wide", initial_sidebar_state="expanded")

# Define the metrics directly to avoid missing ones
ALL_METRICS = [
    "平年降水量(mm)",
    "平年平均気温(℃)",
    "平年日最高気温(℃)",
    "平年日最低気温(℃)",
    "平年日照時間(時間)",
]

def load_precalculated_tsv(mode: str, selected_metric: str) -> pl.DataFrame:
    """事前に計算されたTSVファイル（上位5件・下位5件）を読み込みます。
    Load the pre-calculated TSV file containing top 5 and bottom 5 records.

    Args:
        mode (str): The display mode ("市町村" or "都道府県").
        selected_metric (str): The metric to load.

    Returns:
        pl.DataFrame: The loaded DataFrame, empty if file not found.
    """
    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", f"top5_bottom5_{mode}_{selected_metric}.tsv")
    if not os.path.exists(file_path):
        return pl.DataFrame()
    return pl.read_csv(file_path, separator='\t')

def process_data_for_search(mode: str, selected_metric: str, search_query: str) -> pl.DataFrame:
    """検索機能用に全体のデータを読み込み、集計・ソートして該当する結果を抽出します。
    Load and process full data to find search results.

    Args:
        mode (str): The display mode.
        selected_metric (str): The metric.
        search_query (str): The query string.

    Returns:
        pl.DataFrame: Processed dataframe containing search results.
    """
    from data_loader import get_lazy_data
    lf = get_lazy_data()
    filtered_lf = lf.filter(pl.col("Metric") == selected_metric)

    if mode == "都道府県":
        agg_exprs = []
        months = [f"{m:02d}" for m in range(1, 13)]
        cols_to_agg = ["年間"] + months
        for c in cols_to_agg:
            if "最高" in selected_metric:
                agg_exprs.append(pl.col(c).max())
            elif "最低" in selected_metric:
                agg_exprs.append(pl.col(c).min())
            else:
                agg_exprs.append(pl.col(c).mean().round(1))
                
        filtered_lf = (
            filtered_lf.group_by("Prefecture")
            .agg(agg_exprs)
            .with_columns([
                pl.lit("全域").alias("Municipality"),
                pl.lit("").alias("URL")
            ])
        )

    ascending_sort = "最低" in selected_metric
    filtered_lf = filtered_lf.sort("年間", descending=not ascending_sort)
    df = filtered_lf.collect()
    df = df.with_row_index("Rank", offset=1)
    
    search_mask = (
        df["Prefecture"].str.contains(f"(?i){search_query}") | 
        df["Municipality"].str.contains(f"(?i){search_query}")
    )
    return df.filter(search_mask)


def main() -> None:
    """Streamlitアプリのメインエントリポイントです。
    Main entry point for the Streamlit app.
    
    Renders the UI, receives inputs, and formats the output dataframe using st.dataframe.
    """
    st.title("日本の気象データランキング")

    st.sidebar.header("設定項目")
    
    # Mode selection
    mode = st.sidebar.radio("モード選択", options=["都道府県", "市町村"])
    selected_metric = st.sidebar.selectbox("気象データ (指標一覧)", options=ALL_METRICS)
    search_query = st.sidebar.text_input("市町村/都道府県名で検索して挿入", placeholder="例：東京")

    # Load from TSV explicitly explicitly
    display_df = load_precalculated_tsv(mode, selected_metric)

    if display_df.is_empty():
        st.warning(f"データが見つかりませんでした。先に `uv run python src/data_generator.py` を実行してください。")
        return

    # Search and Insert
    if search_query:
        search_results = process_data_for_search(mode, selected_metric, search_query)
        
        if not search_results.is_empty():
            display_df = pl.concat([display_df, search_results]).unique(subset=["Rank"])
            st.toast(f"「{search_query}」の検索結果をランキングに挿入しました！", icon="🔍")
        else:
            st.sidebar.warning(f"「{search_query}」は見つかりませんでした。")

    # Re-sort by Rank
    display_df = display_df.sort("Rank")

    st.subheader(f"【{mode}】{selected_metric} の BEST 5 & WORST 5")

    # Convert to pandas just for streamlit components if we need explicit column_config,
    # but Streamlit >= 1.28 supports Polars natively.
    # We will format URL to markdown link for Municipality if URL exists.
    # Instead of building literal markdown, Streamlit LinkColumn is perfect.
    
    month_cols = [f"{m:02d}" for m in range(1, 13)]
    display_cols = ["Rank", "Prefecture", "Municipality", "年間"] + month_cols
    
    # Convert URL to display cleanly using Streamlit LinkColumn behavior
    # Instead of wrapping in [], we leave Municipality as text and have URL separate
    # Streamlit column_config.LinkColumn allows specifying display text.
    # However since the names vary per row, we can just use URL but make the display_text Regex group the name out of it? No.
    # The clean way is display DataFrame where we specify LinkColumn on URL but hide the URL behind generic text, 
    # OR since Streamlit doesn't support row-based dynamic display text cleanly without markdown... Actually, the markdown bracket was `[name](url)`.
    # Let's fix the user's parsing issue by ensuring the column config makes it a true LinkColumn of the URL, and Municipality is its own TextColumn string.
    # But wait, user wanted "no URI or []". If we just provide the municipality name as text, and add a small generic link icon column, that works beautifully and is robust!
    
    display_df = display_df.with_columns(
        pl.when(pl.col("URL").str.len_chars() > 0)
        .then(pl.col("URL"))
        .otherwise(pl.lit(None))
        .alias("URI")
    )
    
    pd_df = display_df.to_pandas()
    
    display_cols = ["Rank", "Prefecture", "Municipality", "URI", "年間"] + month_cols
    
    st.dataframe(
        pd_df,
        column_order=display_cols,
        use_container_width=True,
        hide_index=True,
        height=500,
        column_config={
            "Municipality": st.column_config.TextColumn(
                "地点名",
                help="市町村・地点の名前"
            ),
            "URI": st.column_config.LinkColumn(
                "URI",
                help="気象庁の該当ページへ飛びます",
                display_text="🌐"
            ),
        }
    )


if __name__ == "__main__":
    main()
