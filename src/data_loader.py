"""
気象データロードモジュール。
Data loading module using Polars LazyFrame for Streamlit.
"""
import polars as pl
from pathlib import Path
from src.config import METRICS_DIR, HEADS_TAILS_DIR

def get_lazy_metric_data(metric: str) -> pl.LazyFrame:
    """特定の指標の全データをLazyFrameとして読み込みます。"""
    file_path = METRICS_DIR / f"{metric}.tsv"
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    return pl.scan_csv(file_path, separator='\t')

def get_lazy_heads_tails(mode: str, metric: str) -> pl.LazyFrame:
    """事前計算された上下5位のTSVをLazyFrameとして読み込みます。"""
    file_path = HEADS_TAILS_DIR / f"top5_bottom5_{mode}_{metric}.tsv"
    if not file_path.exists():
        return pl.LazyFrame()
    return pl.scan_csv(file_path, separator='\t')