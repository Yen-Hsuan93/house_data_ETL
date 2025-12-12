import os
import pandas as pd
from pathlib import Path

class RawMerger:
    def __init__(self, raw_folder, merged_path):
        self.raw_folder = Path(raw_folder)
        self.merged_path = Path(merged_path)
        self.merged_path.parent.mkdir(parents=True, exist_ok=True)

    def merge(self):
        old_sources = set()
        file_exists = self.merged_path.exists()
        
        if file_exists:
            try:
                check_df = pd.read_csv(self.merged_path, usecols=["來源檔名"], encoding="utf-8-sig")
                old_sources = set(check_df["來源檔名"].unique())
            except ValueError:
                pass
            except Exception as e:
                print(f"讀取舊檔檢查時發生錯誤: {e}")

        new_files = [f for f in os.listdir(self.raw_folder) if f.endswith(".csv") and f not in old_sources]
        
        if not new_files:
            print("沒有偵測到新檔案，無需合併。")
            return str(self.merged_path)

        print(f"偵測到 {len(new_files)} 個新檔案，準備進行追加合併...")

        new_dfs = []
        for f in new_files:
            try:
                file_path = self.raw_folder / f
                df = pd.read_csv(file_path, encoding="utf-8-sig", low_memory=False)
                
                if not df.empty:
                    first_row_str = df.iloc[0].astype(str).str.lower()
                    if first_row_str.str.contains("transaction|total price|square meter").any():
                        print(f"偵測到英文標題列，已移除第一列：{f}")
                        df = df.iloc[1:]
                
                # 加上來源檔名標籤
                df["來源檔名"] = f
                new_dfs.append(df)
            except Exception as e:
                print(f"讀取檔案 {f} 失敗: {e}") 

        if new_dfs:

            combined_new_df = pd.concat(new_dfs, ignore_index=True)
            
            write_header = not file_exists
            
            combined_new_df.to_csv(
                self.merged_path, 
                mode='a', 
                index=False, 
                header=write_header, 
                encoding="utf-8-sig"
            )
            
            print(f"合併完成！本次新增 {len(new_files)} 個檔案，新增筆數: {len(combined_new_df)}")
        
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