import os
import pandas as pd
from pathlib import Path

class RawMerger:
    def __init__(self, raw_folder, merged_path, master_path=None):

        self.raw_folder = Path(raw_folder)
        self.merged_path = Path(merged_path)
        self.master_path = Path(master_path) if master_path else None
        

        self.merged_path.parent.mkdir(parents=True, exist_ok=True)

    def merge(self):

        old_sources = set()

        if self.merged_path.exists():
            try:
                check_df = pd.read_csv(self.merged_path, usecols=["來源檔名"], encoding="utf-8-sig")
                merged_sources = set(check_df["來源檔名"].unique())
                old_sources.update(merged_sources)
                print(f"[RawMerger] 中繼檔中已包含 {len(merged_sources)} 個檔案")
            except Exception as e:
                print(f"[RawMerger] 讀取中繼檔檢查時發生錯誤: {e}")


        if self.master_path and self.master_path.exists():
            try:

                master_df = pd.read_csv(self.master_path, usecols=["來源檔名"], encoding="utf-8-sig")
                master_sources = set(master_df["來源檔名"].unique())
                old_sources.update(master_sources)
                print(f"[RawMerger] 最終主檔中已記錄 {len(master_sources)} 個檔案 (將跳過不處理)")
            except ValueError:
                print("[RawMerger] 主檔存在但無 [來源檔名] 欄位，無法進行比對。")
            except Exception as e:
                print(f"[RawMerger] 讀取主檔檢查時發生錯誤: {e}")


        all_csv_files = [f for f in os.listdir(self.raw_folder) if f.endswith(".csv")]
        new_files = [f for f in all_csv_files if f not in old_sources]
        
        if not new_files:
            print("[RawMerger] 沒有偵測到需處理的新檔案。")
            return str(self.merged_path)

        print(f"[RawMerger] 偵測到 {len(new_files)} 個未處理檔案，準備 Append 到中繼檔...")


        new_dfs = []
        for f in new_files:
            try:
                file_path = self.raw_folder / f

                df = pd.read_csv(file_path, encoding="utf-8-sig", low_memory=False)
                
                if not df.empty:
                    # 移除重複英文標題
                    first_row_str = df.iloc[0].astype(str).str.lower()
                    if first_row_str.str.contains("transaction|total price|square meter").any():
                        df = df.iloc[1:]
                

                df["來源檔名"] = f
                new_dfs.append(df)
            except Exception as e:
                print(f"讀取檔案 {f} 失敗: {e}") 


        if new_dfs:
            combined_new_df = pd.concat(new_dfs, ignore_index=True)
            

            combined_new_df.to_csv(
                self.merged_path, 
                mode='a', 
                index=False, 
                header=not self.merged_path.exists(), 
                encoding="utf-8-sig"
            )
            
            print(f"[RawMerger] 合併完成！本次新增 {len(new_files)} 個檔案到 merged_rawdata.csv")
        
        return str(self.merged_path)