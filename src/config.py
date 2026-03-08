"""
設定値や定数を管理するモジュール。
Configuration and constants module.
"""
from pathlib import Path

# パス設定
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
METRICS_DIR = DATA_DIR / "metrics"
HEADS_TAILS_DIR = DATA_DIR / "heads_tails"
STRUCTURE_TSV_PATH = DATA_DIR / "stracture.tsv"

# 平年値（nml_*_ym.php）から取得する指標
METRICS_NORMALS = {
    "降水量(mm)": "平年降水量(mm)",
    "平均気温(℃)": "平年平均気温(℃)",
    "日最高気温(℃)": "平年日最高気温(℃)",
    "日最低気温(℃)": "平年日最低気温(℃)",
    "日照時間(時間)": "平年日照時間(時間)",
    "降雪量(cm)": "平年降雪量(cm)",  # 追加
}

# 歴代ランキング（rank_a.php）から取得する指標
# ※「最高気温」「最低気温」「降雪の深さ」など、表のヘッダーに含まれるキーワードで検索します
METRICS_RECORDS = {
    "最高気温": "歴代最高気温(℃)",      # 追加
    "最低気温": "歴代最低気温(℃)",      # 追加
    "降雪の深さ": "歴代最大降雪量(cm)",  # 追加
}

ALL_METRICS = list(METRICS_NORMALS.values()) + list(METRICS_RECORDS.values())