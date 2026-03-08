"""
気象データロードモジュール。
Data loading module.

Imports the weather dataset using polars.LazyFrame.
"""

import polars as pl
import os

def get_lazy_data() -> pl.LazyFrame:
    """気象データをpolarsのLazyFrameとして読み込みます。
    Load the weather data as a polars LazyFrame.

    Returns:
        pl.LazyFrame: A LazyFrame containing weather data.
    """
    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "weather_data.csv")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}. Please run data_generator.py.")
    
    return pl.scan_csv(file_path)
