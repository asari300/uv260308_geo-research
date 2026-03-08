"""
気象庁からのスクレイピングを担当するモジュール。
Scraping module for fetching JMA station data and values.
"""
import urllib.request
import re
import time
from bs4 import BeautifulSoup
from src.config import METRICS_NORMALS, METRICS_RECORDS

def fetch_jma_stations() -> list[dict[str, str]]:
    """都道府県と観測所のリストをスクレイピングして取得します。"""
    base_url = "https://www.data.jma.go.jp/stats/etrn/select/prefecture00.php"
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(base_url, headers=headers)
    
    with urllib.request.urlopen(req) as res:
        html = res.read().decode("utf-8")
        
    soup = BeautifulSoup(html, "html.parser")
    prefs = {m.group(1): a.get("alt") for a in soup.find_all("area") 
             if (m := re.search(r"prec_no=(\d+)", a.get("href", ""))) and "地方" not in a.get("alt", "")}
            
    stations = []
    seen = set()
    
    for prec_no, pref_name in prefs.items():
        pref_url = f"https://www.data.jma.go.jp/stats/etrn/select/prefecture.php?prec_no={prec_no}"
        req = urllib.request.Request(pref_url, headers=headers)
        try:
            with urllib.request.urlopen(req) as res:
                p_soup = BeautifulSoup(res.read().decode("utf-8"), "html.parser")
        except Exception:
            continue
            
        for area in p_soup.find_all("area"):
            onmouseover = area.get("onmouseover", "")
            m_view = re.search(r"viewPoint\('([sa])',\s*'([^']*)',\s*'([^']*)'", onmouseover)
            if not m_view:
                continue
                
            typ_char, block_no, st_name = m_view.groups()
            st_type = "sfc" if typ_char == "s" else "amd"
            k = (prec_no, block_no)
            
            if k not in seen:
                seen.add(k)
                stations.append({
                    "Prefecture": pref_name,
                    "Municipality": st_name.strip(),
                    "prec_no": prec_no,
                    "block_no": block_no,
                    "st_type": st_type
                })
        time.sleep(0.1)
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
    """観測所の平年値（nml_..._ym.php）と、月別の歴代記録（rank_a.php）を取得します。"""
    prec_no, block_no, st_type = station["prec_no"], station["block_no"], station["st_type"]
    base_nml_url = f"https://www.data.jma.go.jp/stats/etrn/view/nml_{st_type}_ym.php?prec_no={prec_no}&block_no={block_no}&year=&month=&day=&view="
    station["URL"] = base_nml_url
    
    results = _fetch_monthly_normals(base_nml_url, station)
    
    # 歴代ランキングデータの取得（1〜12月分および年間をスクレイピング）
    # ※ リクエスト数が多くなるため、運用時は対象を絞るかワーカー数を調整してください
    record_results = _fetch_monthly_records(prec_no, block_no, station)
    results.extend(record_results)
    
    return results

def _fetch_monthly_normals(url: str, station: dict) -> list[dict]:
    """平年値テーブルからデータを抽出します。"""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            soup = BeautifulSoup(res.read().decode("utf-8"), "html.parser")
    except Exception:
        return []

    tables = soup.find_all("table", class_="data2_s")
    if not tables:
        return []
        
    rows = tables[0].find_all("tr")
    headers = [th.text.strip().replace("\\r", "").replace("\\n", "") for th in rows[0].find_all(["th", "td"])]
    col_idx_map = {METRICS_NORMALS[k]: headers.index(k) for k in METRICS_NORMALS if k in headers}
    
    monthly_data = {m: [None]*12 for m in col_idx_map}
    annual_data = {m: None for m in col_idx_map}
    
    for row in rows[2:]:
        tds = [td.text.strip() for td in row.find_all(["th", "td"])]
        if not tds: continue
        
        row_title = tds[0]
        match = re.match(r"^(\d{1,2})月$", row_title)
        
        if match:
            m_idx = int(match.group(1)) - 1
            for metric, c_idx in col_idx_map.items():
                if c_idx < len(tds):
                    monthly_data[metric][m_idx] = float_or_none(tds[c_idx])
        elif row_title == "年":
            for metric, c_idx in col_idx_map.items():
                if c_idx < len(tds):
                    annual_data[metric] = float_or_none(tds[c_idx])
                    
    results = []
    for metric in col_idx_map:
        if all(x is None for x in monthly_data[metric]) and annual_data[metric] is None:
            continue
        row_dict = {
            "Prefecture": station["Prefecture"], "Municipality": station["Municipality"],
            "prec_no": station["prec_no"], "block_no": station["block_no"],
            "URL": station["URL"], "Metric": metric, "年間": annual_data[metric]
        }
        for i in range(12):
            row_dict[f"{i+1:02d}"] = monthly_data[metric][i]
        results.append(row_dict)
    return results

def _fetch_monthly_records(prec_no: str, block_no: str, station: dict) -> list[dict]:
    """歴代ランキング（最高・最低など）の1位の値を月別・年間で抽出します。"""
    # 月別（1〜12月）+ 年間（None）のパラメータ
    months_to_check = list(range(1, 13)) + [None]
    
    record_data = {m: {"年間": None, **{f"{i:02d}": None for i in range(1, 13)}} for m in METRICS_RECORDS.values()}
    
    for mth in months_to_check:
        mth_param = f"&month={mth}" if mth else "&month="
        url = f"https://www.data.jma.go.jp/stats/etrn/view/rank_a.php?prec_no={prec_no}&block_no={block_no}&year={mth_param}&day=&view=h0"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=5) as res:
                soup = BeautifulSoup(res.read().decode("utf-8"), "html.parser")
        except Exception:
            continue
            
        for table in soup.find_all("table", class_="data2_s"):
            headers = [th.text.strip() for th in table.find_all("th")]
            for key, metric_name in METRICS_RECORDS.items():
                if any(key in h for h in headers):
                    rows = table.find_all("tr")
                    for row in rows:
                        cols = row.find_all("td")
                        # 順位1位の行から値を取得（通常は td要素の2番目が「値」）
                        if len(cols) >= 2 and cols[0].text.strip() == "1":
                            val = float_or_none(cols[1].text)
                            if mth is None:
                                record_data[metric_name]["年間"] = val
                            else:
                                record_data[metric_name][f"{mth:02d}"] = val
                            break
        time.sleep(0.1) # サーバー負荷軽減

    results = []
    for metric, data_dict in record_data.items():
        if all(v is None for v in data_dict.values()):
            continue
        row_dict = {
            "Prefecture": station["Prefecture"], "Municipality": station["Municipality"],
            "prec_no": station["prec_no"], "block_no": station["block_no"],
            "URL": station["URL"], "Metric": metric
        }
        row_dict.update(data_dict)
        results.append(row_dict)
    return results