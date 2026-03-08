"""
データ生成を実行するメインスクリプト。
Main entry point for scraping and generating datasets.
"""
import polars as pl
import concurrent.futures
from tqdm import tqdm
from src.scraper import fetch_jma_stations, fetch_station_data
from src.processor import process_and_save_data

def generate_data():
    stations = fetch_jma_stations()
    if not stations:
        print("No stations found.")
        return

    print(f"Scraping data for {len(stations)} stations...")
    data = []

    # 負荷を考慮し、ワーカー数は適宜調整してください
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_station_data, st): st for st in stations}
        
        # tqdmを使用してプログレスバーを表示
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(stations), desc="Scraping progress"):
            try:
                data.extend(future.result())
            except Exception as e:
                # エラー出力がプログレスバーを崩さないようにtqdm.writeを使用
                tqdm.write(f"Error fetching data: {e}")

    stations_df = pl.DataFrame(stations).select(["Prefecture", "Municipality", "prec_no", "block_no", "URL"])
    df = pl.DataFrame(data) if data else pl.DataFrame()

    print("Processing and saving TSV files...")
    process_and_save_data(df, stations_df)
    print("All tasks completed successfully.")

if __name__ == "__main__":
    generate_data()
