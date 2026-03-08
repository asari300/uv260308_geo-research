"""
processor.pyのテストモジュール
"""
import pytest
import polars as pl
from pathlib import Path
from src.processor import is_ascending, process_and_save_data

def test_is_ascending():
    """ソート順序ロジックのテスト"""
    assert is_ascending("歴代最低気温(℃)") is True
    assert is_ascending("平年平均気温(℃)") is False
    assert is_ascending("歴代最高気温(℃)") is False

def test_process_and_save_data(tmp_path: Path, monkeypatch):
    """データ処理からTSV出力（LF, UTF-8-BOM）の一連の動作テスト"""
    # configモジュールの保存先パスを一時ディレクトリ(tmp_path)に上書き
    monkeypatch.setattr("src.processor.STRUCTURE_TSV_PATH", tmp_path / "stracture.tsv")
    monkeypatch.setattr("src.processor.METRICS_DIR", tmp_path / "metrics")
    monkeypatch.setattr("src.processor.HEADS_TAILS_DIR", tmp_path / "heads_tails")
    monkeypatch.setattr("src.processor.ALL_METRICS", ["平年平均気温(℃)"])

    # ダミーデータの用意
    stations_data = pl.DataFrame({
        "Prefecture": ["東京都", "北海道"],
        "Municipality": ["東京", "札幌"],
        "prec_no": ["44", "14"],
        "block_no": ["47662", "47412"],
        "URL": ["http://jma/tokyo", "http://jma/sapporo"]
    })

    metrics_data = pl.DataFrame({
        "Prefecture": ["東京都", "北海道"],
        "Municipality": ["東京", "札幌"],
        "prec_no": ["44", "14"],
        "block_no": ["47662", "47412"],
        "URL": ["http://jma/tokyo", "http://jma/sapporo"],
        "Metric": ["平年平均気温(℃)", "平年平均気温(℃)"],
        "年間": [15.8, 8.9],
        "01": [5.0, -3.0], "02": [6.0, -2.0], "03": [8.0, 1.0], "04": [13.0, 7.0],
        "05": [18.0, 13.0], "06": [21.0, 16.0], "07": [25.0, 20.0], "08": [26.0, 22.0],
        "09": [23.0, 18.0], "10": [18.0, 11.0], "11": [12.0, 5.0], "12": [7.0, -1.0]
    })

    # 関数実行
    process_and_save_data(metrics_data, stations_data)

    # stracture.tsv の検証
    struct_file = tmp_path / "stracture.tsv"
    assert struct_file.exists()
    struct_df = pl.read_csv(struct_file, separator='\t')
    assert struct_df.shape == (2, 6) # 5メタデータカラム + 1指標カラム
    assert struct_df["平年平均気温(℃)"].to_list() == [True, True]

    # metrics 内のTSV検証
    metric_file = tmp_path / "metrics" / "平年平均気温(℃).tsv"
    assert metric_file.exists()
    # LF改行とBOMの確認(バイナリ読み込み)
    with open(metric_file, "rb") as f:
        content = f.read()
        assert content.startswith(b"\xef\xbb\xbf") # UTF-8 BOM
        assert b"\r\n" not in content             # CRLFを含まず、LFのみであることを検証

    # heads_tails の生成検証
    assert (tmp_path / "heads_tails" / "top5_bottom5_市町村_平年平均気温(℃).tsv").exists()
    assert (tmp_path / "heads_tails" / "top5_bottom5_都道府県_平年平均気温(℃).tsv").exists()