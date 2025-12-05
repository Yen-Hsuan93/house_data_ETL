import os
import pandas as pd
from pathlib import Path

class RawMerger:

    def __init__(
        self,
        raw_folder=r"C:\sideProject\house_rawdata",
        merged_path=r"C:\sideProject\main_house_rawdata\merged_rawdata.csv",
    ):
        self.raw_folder = Path(raw_folder)
        self.merged_path = Path(merged_path)
        self.merged_path.parent.mkdir(parents=True, exist_ok=True)

    def merge(self):
        # 取得目前所有下載的檔案
        csv_files = [file for file in os.listdir(self.raw_folder) if file.endswith(".csv")]
        if not csv_files:
            print("未找到任何新檔案")
            return str(self.merged_path)

        # 若 merged_rawdata.csv 已存在，讀進舊資料
        if self.merged_path.exists():
            old_df = pd.read_csv(self.merged_path, encoding="utf-8-sig", low_memory=False)
            old_sources = set(old_df["來源檔名"].unique()) if "來源檔名" in old_df.columns else set()
        else:
            old_df = pd.DataFrame()
            old_sources = set()

        # 篩出還沒合併過的新檔案
        new_files = [f for f in csv_files if f not in old_sources]
        if not new_files:
            print("沒有新檔案可合併，跳過 RawMerger")
            return str(self.merged_path)

        print(f"{len(new_files)} 個新檔案，併入 merged_rawdata.csv")

        # 開始合併新資料
        merged_df = old_df.copy()
        for csv_file in new_files:
            file_path = self.raw_folder / csv_file
            try:
                df = pd.read_csv(file_path, encoding="utf-8-sig", low_memory=False)

                # 移除英文標題列
                if df.iloc[0].astype(str).str.contains("transaction|total price|square meter", case=False).any():
                    print(f"移除英文標題列：{csv_file}")
                    df = df.iloc[1:]

                df["來源檔名"] = csv_file
                merged_df = pd.concat([merged_df, df], ignore_index=True)
                print(f"併入：{csv_file}")
            except Exception as e:
                print(f"讀取失敗 {csv_file}：{e}")

        merged_df.to_csv(self.merged_path, index=False, encoding="utf-8-sig")
        print(f"輸出合併檔：{self.merged_path}")
        print(f"總筆數：{len(merged_df):,}")

        return str(self.merged_path)

def main():
    raw_merger = RawMerger(
        raw_folder=r"C:\sideProject\house_rawdata",
        merged_path=r"C:\sideProject\main_house_rawdata\merged_rawdata.csv"
    )
    merged_path = raw_merger.merge()
    print("[TEST] merged_path = ", merged_path)

if __name__ == "__main__":
    main()
