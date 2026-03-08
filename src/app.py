"""
Streamlitメインアプリケーションモジュール。
Main Streamlit application module.
"""
import streamlit as st
import polars as pl
from src.config import ALL_METRICS
from src.data_loader import get_lazy_heads_tails, get_lazy_metric_data

st.set_page_config(page_title="日本の気象データランキング", layout="wide")

def process_data_for_search(mode: str, selected_metric: str, search_query: str) -> pl.DataFrame:
    try:
        lf = get_lazy_metric_data(selected_metric)
    except FileNotFoundError:
        return pl.DataFrame()

    if mode == "都道府県":
        agg_exprs = []
        cols_to_agg = ["年間"] + [f"{m:02d}" for m in range(1, 13)]
        for c in cols_to_agg:
            if "最高" in selected_metric:
                agg_exprs.append(pl.col(c).max())
            elif "最低" in selected_metric:
                agg_exprs.append(pl.col(c).min())
            else:
                agg_exprs.append(pl.col(c).mean().round(1))
                
        lf = (
            lf.group_by("Prefecture")
            .agg(agg_exprs)
            .with_columns([pl.lit("全域").alias("Municipality"), pl.lit("").alias("URL")])
        )

    ascending_sort = "最低" in selected_metric
    
    # 検索対象を絞ってからcollect()することでメモリ消費を抑えます
    df = lf.sort("年間", descending=not ascending_sort).collect().with_row_index("Rank", offset=1)
    
    search_mask = (
        df["Prefecture"].str.contains(f"(?i){search_query}") | 
        df["Municipality"].str.contains(f"(?i){search_query}")
    )
    return df.filter(search_mask)

def main():
    st.title("日本の気象データランキング")
    st.sidebar.header("設定項目")
    
    mode = st.sidebar.radio("モード選択", options=["都道府県", "市町村"])
    selected_metric = st.sidebar.selectbox("気象データ (指標一覧)", options=ALL_METRICS)
    search_query = st.sidebar.text_input("市町村/都道府県名で検索して挿入", placeholder="例：東京")

    # LazyFrameからDataFrameとして実体化して表示
    display_df = get_lazy_heads_tails(mode, selected_metric).collect()

    if display_df.is_empty():
        st.warning(f"データが見つかりませんでした。`python src/data_generator.py` を実行してください。")
        return

    if search_query:
        search_results = process_data_for_search(mode, selected_metric, search_query)
        if not search_results.is_empty():
            display_df = pl.concat([display_df, search_results]).unique(subset=["Rank"])
            st.toast(f"「{search_query}」の検索結果を挿入しました！", icon="🔍")
        else:
            st.sidebar.warning(f"「{search_query}」は見つかりませんでした。")

    display_df = display_df.sort("Rank")
    st.subheader(f"【{mode}】{selected_metric} の BEST 5 & WORST 5")

    display_df = display_df.with_columns(
        pl.when(pl.col("URL").str.len_chars() > 0)
        .then(pl.col("URL"))
        .otherwise(pl.lit(None))
        .alias("URI")
    )
    
    month_cols = [f"{m:02d}" for m in range(1, 13)]
    display_cols = ["Rank", "Prefecture", "Municipality", "URI", "年間"] + month_cols
    
    st.dataframe(
        display_df.to_pandas(),
        column_order=display_cols,
        use_container_width=True,
        hide_index=True,
        height=500,
        column_config={
            "Municipality": st.column_config.TextColumn("地点名", help="市町村・地点の名前"),
            "URI": st.column_config.LinkColumn("URI", help="気象庁ページへ飛びます", display_text="🌐"),
        }
    )

if __name__ == "__main__":
    main()