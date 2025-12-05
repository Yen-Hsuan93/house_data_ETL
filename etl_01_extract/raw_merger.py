import os
import pandas as pd
from pathlib import Path

class RawMerger:
    def __init__(self, raw_folder, merged_path):
        self.raw_folder = Path(raw_folder)
        self.merged_path = Path(merged_path)
        self.merged_path.parent.mkdir(parents=True, exist_ok=True)

    def merge(self):
        old_df = pd.DataFrame()
        old_sources = set()
        
        if self.merged_path.exists():
            try:
                old_df = pd.read_csv(self.merged_path, encoding="utf-8-sig", low_memory=False)
                if "來源檔名" in old_df.columns:
                    old_sources = set(old_df["來源檔名"].unique())
            except:
                pass 

        new_files = [f for f in os.listdir(self.raw_folder) if f.endswith(".csv") and f not in old_sources]
        
        if not new_files:
            return str(self.merged_path)

        new_dfs = []
        for f in new_files:
            try:
                df = pd.read_csv(self.raw_folder / f, encoding="utf-8-sig", low_memory=False)
                if not df.empty:
                    first_row_str = df.iloc[0].astype(str).str.lower()
                    if first_row_str.str.contains("transaction|total price|square meter").any():
                        print(f"偵測到英文標題列，已移除：{f}")
                        df = df.iloc[1:]
                
                df["來源檔名"] = f
                new_dfs.append(df)
            except:
                pass 

        if new_dfs:
            final_df = pd.concat([old_df] + new_dfs, ignore_index=True)
            final_df.to_csv(self.merged_path, index=False, encoding="utf-8-sig")
            print(f"合併完成，新增 {len(new_files)} 個檔案，總筆數: {len(final_df)}")

        return str(self.merged_path)



def main():

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

    raw_folder_path = os.path.join(PROJECT_ROOT, "house_rawdata")
    merged_csv_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")

    raw_merger = RawMerger(
        raw_folder=raw_folder_path,  
        merged_path=merged_csv_path   
    )
    
    merged_path = raw_merger.merge()
    print("[TEST] merged_path = ", merged_path)

if __name__ == "__main__":
    main()