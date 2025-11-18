import pandas as pd
from pathlib import Path

class MainDataLoader:

    def __init__(self, main_data_path: str, new_data_path: str, output_path: str):
        self.main_data_path = Path(main_data_path)
        self.new_data_path = Path(new_data_path)
        self.output_path = Path(output_path)

    def load(self):
        #合併清整後資料回主檔

        old_main = pd.read_csv(self.main_data_path, encoding="utf-8-sig")
        new_cleaned = pd.read_csv(self.new_data_path, encoding="utf-8-sig")

        merged = pd.concat([old_main, new_cleaned], ignore_index=True)
        if "編號" in merged.columns:
            merged.drop_duplicates(subset=["編號"], keep="last", inplace=True)
        else:
            print("找不到『編號』欄位")

        merged.to_csv(self.output_path, index=False, encoding="utf-8-sig")
        print(f" 已更新 main_data，筆數：{len(merged):,}")
        print(f" 輸出：{self.output_path}")
        return self.output_path
