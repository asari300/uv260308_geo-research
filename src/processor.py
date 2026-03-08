"""
データ処理およびファイルI/Oを担当するモジュール。
Data processing and file I/O operations using Polars LazyFrames.
"""
import polars as pl
from pathlib import Path
from src.config import ALL_METRICS, STRUCTURE_TSV_PATH, METRICS_DIR, HEADS_TAILS_DIR

def is_ascending(metric: str) -> bool:
    """指標名から、小さい方が上位（昇順）かどうかを判定します。"""
    return "最低" in metric

def process_and_save_data(df: pl.DataFrame, stations_df: pl.DataFrame):
    """取得した全データを解析し、所定のディレクトリ構成でTSV出力します。

    Args:
        df (pl.DataFrame): スクレイピングしたすべての指標データを含むDataFrame
        stations_df (pl.DataFrame): 全観測所のメタデータ
    """
    STRUCTURE_TSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    HEADS_TAILS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. stracture.tsv の生成 (データの有無をTrue/Falseでフラグ化)
    if not df.is_empty():
        metric_flags = df.group_by(["prec_no", "block_no"]).agg(pl.col("Metric"))
        structure_df = stations_df.join(metric_flags, on=["prec_no", "block_no"], how="left")
        for m in ALL_METRICS:
            structure_df = structure_df.with_columns(
                pl.col("Metric").list.contains(m).fill_null(False).alias(m)
            )
        structure_df = structure_df.drop("Metric")
    else:
        structure_df = stations_df.with_columns([pl.lit(False).alias(m) for m in ALL_METRICS])

    struct_cols = ["Prefecture", "Municipality", "prec_no", "block_no", "URL"] + ALL_METRICS
    structure_df.select(struct_cols).write_csv(STRUCTURE_TSV_PATH, separator='\t', include_bom=True)

    if df.is_empty():
        return

    # 2. 各指標のTSVと、上下5位（heads_tails）の計算と保存
    lf = df.lazy()
    months = [f"{m:02d}" for m in range(1, 13)]
    cols_to_agg = ["年間"] + months

    for metric in ALL_METRICS:
        metric_lf = lf.filter(pl.col("Metric") == metric)
        metric_file_path = METRICS_DIR / f"{metric}.tsv"
        
        # 実体化せずにそのままファイルへストリーム書き込み
        metric_lf.sink_csv(metric_file_path, separator='\t', include_bom=True)

        # heads_tails用はソートが必要なためDataFrameとして実体化
        df_metric_city = metric_lf.collect()
        if df_metric_city.is_empty():
            continue

        asc_sort = is_ascending(metric)

        # 【市町村】上下5位
        df_sorted = df_metric_city.drop_nulls(subset=["年間"]).sort("年間", descending=not asc_sort).with_row_index("Rank", offset=1)
        if len(df_sorted) > 0:
            combined_city = pl.concat([df_sorted.head(5), df_sorted.tail(5)]).unique(subset=["Rank"]).sort("Rank")
            combined_city.write_csv(HEADS_TAILS_DIR / f"top5_bottom5_市町村_{metric}.tsv", separator='\t', include_bom=True)

        # 【都道府県】全域での集計と上下5位
        agg_exprs = []
        for c in cols_to_agg:
            if "最高" in metric:
                agg_exprs.append(pl.col(c).max())
            elif "最低" in metric:
                agg_exprs.append(pl.col(c).min())
            else:
                agg_exprs.append(pl.col(c).mean().round(1))

        df_metric_pref = (
            df_metric_city.group_by("Prefecture")
            .agg(agg_exprs)
            .with_columns([pl.lit("全域").alias("Municipality"), pl.lit("").alias("URL")])
        )
        
        df_pref_sorted = df_metric_pref.drop_nulls(subset=["年間"]).sort("年間", descending=not asc_sort).with_row_index("Rank", offset=1)
        if len(df_pref_sorted) > 0:
            combined_pref = pl.concat([df_pref_sorted.head(5), df_pref_sorted.tail(5)]).unique(subset=["Rank"]).sort("Rank")
            combined_pref.write_csv(HEADS_TAILS_DIR / f"top5_bottom5_都道府県_{metric}.tsv", separator='\t', include_bom=True)