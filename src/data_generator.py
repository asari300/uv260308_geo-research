"""
気象庁地点のスクレイピングとデータ生成モジュール。
A module for scraping JMA station data and generating dataset from real table values.
"""
import polars as pl
import urllib.request
import re
from bs4 import BeautifulSoup
import time
import os
import concurrent.futures

# サポートするメトリクス
ALL_METRICS = [
    "平年降水量(mm)",
    "平年平均気温(℃)",
    "平年日最高気温(℃)",
    "平年日最低気温(℃)",
    "平年日照時間(時間)",
]

# JMAのテーブルヘッダ名とのマッピング
METRIC_MAPPING = {
    "平年降水量(mm)": "降水量(mm)",
    "平年平均気温(℃)": "平均気温(℃)",
    "平年日最高気温(℃)": "日最高気温(℃)",
    "平年日最低気温(℃)": "日最低気温(℃)",
    "平年日照時間(時間)": "日照時間(時間)",
}

def fetch_jma_stations() -> list[dict[str, str]]:
    """気象庁から都道府県と観測所のリストをスクレイピングして取得します。
    Fetches the list of prefectures and weather stations from JMA.

    Returns:
        list[dict[str, str]]: List of dictionary containing Prefecture, Municipality, and URL info.
    """
    base_url = "https://www.data.jma.go.jp/stats/etrn/select/prefecture00.php"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    req = urllib.request.Request(base_url, headers=headers)
    try:
        with urllib.request.urlopen(req) as res:
            html = res.read().decode("utf-8")
    except Exception as e:
        print("Failed to fetch prefecture list:", e)
        return []
        
    soup = BeautifulSoup(html, "html.parser")
    prefs = {}
    for area in soup.find_all("area"):
        href = area.get("href", "")
        alt = area.get("alt", "")
        m = re.search(r"prec_no=(\d+)", href)
        if m and alt and "地方" not in alt:
            prefs[m.group(1)] = alt
            
    stations = []
    seen = set()
    
    print(f"Fetching station lists for {len(prefs)} prefectures...")
    
    for prec_no, pref_name in prefs.items():
        pref_url = f"https://www.data.jma.go.jp/stats/etrn/select/prefecture.php?prec_no={prec_no}"
        req = urllib.request.Request(pref_url, headers=headers)
        try:
            with urllib.request.urlopen(req) as res:
                p_html = res.read().decode("utf-8")
        except Exception:
            continue
            
        p_soup = BeautifulSoup(p_html, "html.parser")
        
        for area in p_soup.find_all("area"):
            onmouseover = area.get("onmouseover", "")
            
            m_view = re.search(r"viewPoint\('([sa])',\s*'([^']*)',\s*'([^']*)'", onmouseover)
            if not m_view:
                continue
                
            typ_char = m_view.group(1)
            block_no = m_view.group(2)
            st_name = m_view.group(3).strip()
            
            st_type = "sfc" if typ_char == "s" else "amd"
            
            url = f"https://www.data.jma.go.jp/stats/etrn/view/nml_{st_type}_ym.php?prec_no={prec_no}&block_no={block_no}&year=&month=&day=&view="
            
            k = (prec_no, block_no)
            if k not in seen:
                seen.add(k)
                stations.append({
                    "Prefecture": pref_name,
                    "Municipality": st_name,
                    "URL": url,
                    "prec_no": prec_no,
                    "block_no": block_no,
                    "st_type": st_type
                })
                
        time.sleep(0.1) # Be gentle to JMA server
    
    return stations

def float_or_none(val: str) -> float | None:
    val = val.strip().replace("]", "").replace(")", "")
    if not val or val in ("///", "×", "#", "*"):
        return None
    try:
        return float(val)
    except ValueError:
        return None

def fetch_station_data(station: dict) -> list[dict]:
    """特定の観測所の「月ごとの平年値」をスクレイピングして取得します。
    Fetches actual monthly normal data from the station's JMA page.

    Args:
        station (dict): Station metadata.
        
    Returns:
        list[dict]: List of metrics rows for this station.
    """
    req = urllib.request.Request(station["URL"], headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            html = res.read().decode("utf-8")
    except Exception as e:
        print(f"Failed to fetch {station['Municipality']}: {e}")
        return []
        
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_="data2_s")
    if not tables:
        return []
        
    # The first data2_s table is normally the monthly normals
    monthly_table = tables[0]
    rows = monthly_table.find_all("tr")
    
    if len(rows) < 14: # Header(3) + 12 months + Annual
        return []
        
    # parse header indices (usually row 0 has the column names)
    headers = [th.text.strip().replace("\\r", "").replace("\\n", "") for th in rows[0].find_all(["th", "td"])]
    
    # map our metrics to the column indices
    col_idx_map = {}
    for my_metric, jma_name in METRIC_MAPPING.items():
        if jma_name in headers:
            col_idx_map[my_metric] = headers.index(jma_name)
    
    if not col_idx_map:
        return []
        
    # Parse data (rows 3 to 14 are Jan-Dec, row 15 is Annual)
    # Be careful: sometimes '資料年数' takes rows. Best is to search by the first td text ("1月" etc)
    monthly_data = {metric: [None]*12 for metric in col_idx_map.keys()}
    annual_data = {metric: None for metric in col_idx_map.keys()}
    
    for row in rows:
        tds = [td.text.strip() for td in row.find_all(["th", "td"])]
        if not tds: continue
        
        row_title = tds[0]
        match = re.match(r"^(\d{1,2})月$", row_title)
        
        if match:
            month_idx = int(match.group(1)) - 1
            for metric, c_idx in col_idx_map.items():
                if c_idx < len(tds):
                    monthly_data[metric][month_idx] = float_or_none(tds[c_idx])
        elif row_title == "年":
            for metric, c_idx in col_idx_map.items():
                if c_idx < len(tds):
                    annual_data[metric] = float_or_none(tds[c_idx])
                    
    results = []
    for metric in col_idx_map.keys():
        # filter out if all None
        if all(x is None for x in monthly_data[metric]):
            continue
            
        row_dict = {
            "Prefecture": station["Prefecture"],
            "Municipality": station["Municipality"],
            "URL": station["URL"],
            "Metric": metric,
            "年間": annual_data[metric]
        }
        for i in range(12):
            row_dict[f"{i+1:02d}"] = monthly_data[metric][i]
            
        results.append(row_dict)
        
    return results

def generate_data() -> None:
    """気象庁の地点データから実際の気象データをスクレイピング生成し、保存します。
    Generates and saves actual weather data for each scraped JMA station.
    """
    stations = fetch_jma_stations()
    if not stations:
        print("No stations found. Check your internet connection or logic.")
        return

    # Let's limit strictly for debugging if needed, but we will run all 47 prefectures.
    # We have around 900+ stations. It might take a few minutes.
    print(f"Scraping monthly data for {len(stations)} stations...")
    
    data = []
    
    # Use ThreadPoolExecutor for faster scraping
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_station_data, st): st for st in stations}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            if i > 0 and i % 50 == 0:
                print(f"Progress: {i}/{len(stations)}")
            try:
                metrics_rows = future.result()
                data.extend(metrics_rows)
            except Exception as e:
                print(f"Error fetching station data: {e}")
            
    if not data:
        print("No data could be generated.")
        return

    # Use polars to process and write
    df = pl.DataFrame(data)
    
    out_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
    os.makedirs(out_dir, exist_ok=True)
    
    filepath = os.path.join(out_dir, "weather_data.csv")
    df.write_csv(filepath)
    print(f"Dataset generated at {filepath}")

    # Pre-calculate Top 5 and Bottom 5 for each metric and mode, and save as TSV.
    print("Pre-calculating Top 5 and Bottom 5 TSVs...")
    
    def is_ascending(m: str) -> bool:
        return "最低" in m
        
    months = [f"{m:02d}" for m in range(1, 13)]
    cols_to_agg = ["年間"] + months
    
    lf = df.lazy()
    
    for metric in ALL_METRICS:
        df_metric_city = lf.filter(pl.col("Metric") == metric).collect()
        if df_metric_city.is_empty():
            continue
            
        asc_sort = is_ascending(metric)
        df_metric_city = df_metric_city.drop_nulls(subset=["年間"]).sort("年間", descending=not asc_sort)
        df_metric_city = df_metric_city.with_row_index("Rank", offset=1)
        
        top_5_city = df_metric_city.head(5)
        bot_5_city = df_metric_city.tail(5)
        combined_city = pl.concat([top_5_city, bot_5_city]).unique(subset=["Rank"]).sort("Rank")
        
        city_tsv_path = os.path.join(out_dir, f"top5_bottom5_市町村_{metric}.tsv")
        combined_city.write_csv(city_tsv_path, separator='\t')

        agg_exprs = []
        for c in cols_to_agg:
            if "最高" in metric:
                agg_exprs.append(pl.col(c).max())
            elif "最低" in metric:
                agg_exprs.append(pl.col(c).min())
            else:
                agg_exprs.append(pl.col(c).mean().round(1))
                
        df_metric_pref = (
            lf.filter(pl.col("Metric") == metric)
            .group_by("Prefecture")
            .agg(agg_exprs)
            .with_columns([
                pl.lit("全域").alias("Municipality"),
                pl.lit("").alias("URL")
            ])
            .collect()
        )
        
        df_metric_pref = df_metric_pref.drop_nulls(subset=["年間"]).sort("年間", descending=not asc_sort)
        df_metric_pref = df_metric_pref.with_row_index("Rank", offset=1)
        
        top_5_pref = df_metric_pref.head(5)
        bot_5_pref = df_metric_pref.tail(5)
        combined_pref = pl.concat([top_5_pref, bot_5_pref]).unique(subset=["Rank"]).sort("Rank")
        
        pref_tsv_path = os.path.join(out_dir, f"top5_bottom5_都道府県_{metric}.tsv")
        combined_pref.write_csv(pref_tsv_path, separator='\t')

    print("All TSVs pre-calculated successfully.")

if __name__ == "__main__":
    generate_data()
