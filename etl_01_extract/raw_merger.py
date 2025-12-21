import os
import pandas as pd
from pathlib import Path

class RawMerger:
    def __init__(self, raw_folder, merged_path, master_path=None):
        """
        :param raw_folder: 原始 csv 資料夾
        :param merged_path: 中繼合併檔 (merged_rawdata.csv) 路徑
        :param master_path: 最終主檔 (cleaning_main_data2.csv) 路徑，用於檢查是否已清洗過
        """
        self.raw_folder = Path(raw_folder)
        self.merged_path = Path(merged_path)
        self.master_path = Path(master_path) if master_path else None
        
        # 確保中繼檔的資料夾存在
        self.merged_path.parent.mkdir(parents=True, exist_ok=True)

    def merge(self):
        # 1. 建立「已處理過」的檔案清單 (old_sources)
        old_sources = set()

        # [檢查 A]：檢查「中繼合併檔 (merged_rawdata.csv)」裡面有的
        # (避免重複合併到 rawdata)
        if self.merged_path.exists():
            try:
                check_df = pd.read_csv(self.merged_path, usecols=["來源檔名"], encoding="utf-8-sig")
                merged_sources = set(check_df["來源檔名"].unique())
                old_sources.update(merged_sources)
                print(f"[RawMerger] 中繼檔中已包含 {len(merged_sources)} 個檔案")
            except Exception as e:
                print(f"[RawMerger] 讀取中繼檔檢查時發生錯誤: {e}")

        # [檢查 B]：檢查「最終主檔 (cleaning_main_data2.csv)」裡面有的
        # (這是您要求的核心：如果主檔已經有，就完全不要理它)
        if self.master_path and self.master_path.exists():
            try:
                # 只讀取 '來源檔名' 欄位，速度快
                master_df = pd.read_csv(self.master_path, usecols=["來源檔名"], encoding="utf-8-sig")
                master_sources = set(master_df["來源檔名"].unique())
                old_sources.update(master_sources)
                print(f"[RawMerger] 最終主檔中已記錄 {len(master_sources)} 個檔案 (將跳過不處理)")
            except ValueError:
                print("[RawMerger] 主檔存在但無 [來源檔名] 欄位，無法進行比對。")
            except Exception as e:
                print(f"[RawMerger] 讀取主檔檢查時發生錯誤: {e}")

        # 2. 掃描資料夾，找出真正的新檔案
        # 邏輯：資料夾有的 - (中繼檔有的 + 主檔有的)
        all_csv_files = [f for f in os.listdir(self.raw_folder) if f.endswith(".csv")]
        new_files = [f for f in all_csv_files if f not in old_sources]
        
        if not new_files:
            print("[RawMerger] 沒有偵測到需處理的新檔案。")
            return str(self.merged_path)

        print(f"[RawMerger] 偵測到 {len(new_files)} 個未處理檔案，準備 Append 到中繼檔...")

        # 3. 讀取並合併新檔案
        new_dfs = []
        for f in new_files:
            try:
                file_path = self.raw_folder / f
                # low_memory=False 防止欄位型態警告
                df = pd.read_csv(file_path, encoding="utf-8-sig", low_memory=False)
                
                if not df.empty:
                    # 移除重複英文標題 (Open Data 常見問題)
                    first_row_str = df.iloc[0].astype(str).str.lower()
                    if first_row_str.str.contains("transaction|total price|square meter").any():
                        df = df.iloc[1:]
                
                # 關鍵：加上來源檔名，作為未來的識別證
                df["來源檔名"] = f
                new_dfs.append(df)
            except Exception as e:
                print(f"讀取檔案 {f} 失敗: {e}") 

        # 4. 寫入中繼檔 (merged_rawdata.csv)
        if new_dfs:
            combined_new_df = pd.concat(new_dfs, ignore_index=True)
            
            # mode='a' 代表 Append (追加)
            # header=not self.merged_path.exists() 代表：如果是新建立的檔案才寫標題
            combined_new_df.to_csv(
                self.merged_path, 
                mode='a', 
                index=False, 
                header=not self.merged_path.exists(), 
                encoding="utf-8-sig"
            )
            
            print(f"[RawMerger] 合併完成！本次新增 {len(new_files)} 個檔案到 merged_rawdata.csv")
        
        return str(self.merged_path)