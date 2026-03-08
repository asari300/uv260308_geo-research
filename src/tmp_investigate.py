import urllib.request
from bs4 import BeautifulSoup
import sys

def fetch_and_print(url_name, url):
    print(f"\n=== {url_name} ===")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    res = urllib.request.urlopen(req)
    soup = BeautifulSoup(res.read(), "html.parser")
    tables = soup.find_all("table", class_="data2_s")
    if not tables:
         print("No data2_s table found.")
         return
    for i, table in enumerate(tables):
        rows = table.find_all("tr")
        print(f"Table {i} - rows: {len(rows)}")
        for j, row in enumerate(rows[:10]):
            cols = [c.text.strip().replace("\n", "").replace("\r", "") for c in row.find_all(["th", "td"])]
            if len(cols) > 2:
                print(f"  Row {j}: {cols[0]} -> {cols[1]}")
        print("...")

if __name__ == "__main__":
    fetch_and_print("Rank A (h0)", "https://www.data.jma.go.jp/stats/etrn/view/rank_a.php?prec_no=31&block_no=0166&year=&month=&day=&view=h0")
    fetch_and_print("Normals (AMD)", "https://www.data.jma.go.jp/stats/etrn/view/nml_amd_ym.php?prec_no=31&block_no=0166&year=&month=&day=&view=")
