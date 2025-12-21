import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from time import sleep
import pandas as pd
import numpy as np
from pathlib import Path

class LatLngUpdate:
    def __init__(self, df: pd.DataFrame, main_data_path=None, test_mode=True):
        
        #test_mode=True 僅爬前 50 筆
        #test_mode=False 正常模式
        
        self.df = df.copy()
        self.main_data_path = main_data_path
        self.test_mode = test_mode

        # 讀取主資料作為比對來源
        if main_data_path and Path(main_data_path).exists():
            self.main_data = pd.read_csv(main_data_path, encoding="utf-8-sig", low_memory=False)
            print(f"[LatLngUpdate] 已載入主資料檔：{len(self.main_data):,} 筆")
        else:
            self.main_data = pd.DataFrame()
            print("[LatLngUpdate] 未提供 main_data.csv 或檔案不存在，略過比對")

        # 標準化地址
        for df_ in [self.df, self.main_data]:
            if "土地位置建物門牌" in df_.columns:
                df_["土地位置建物門牌"] = (
                    df_["土地位置建物門牌"].astype(str).str.replace("台", "臺", regex=False).str.strip()
                )

        # 清空字串 → NaN
        for col in ["緯度", "經度"]:
            if col in self.df.columns:
                self.df[col].replace("", np.nan, inplace=True)

        my_options = webdriver.ChromeOptions()
        my_options.add_argument("--start-maximized")
        my_options.add_argument("--incognito")
        my_options.add_argument("--disable-popup-blocking")
        my_options.add_argument("--disable-notifications")
        my_options.add_argument("--lang=zh-TW")
        self.driver = webdriver.Chrome(options=my_options)

    def _extract_coordinates_from_url(self, url: str):
        try:
            parts = url.split("@")
            if len(parts) > 1:
                coords = parts[1].split(",")[:2]
                if len(coords) == 2:
                    return coords[0], coords[1]
        except Exception as e:
            print("擷取經緯度錯誤：", e)
        return None, None

    def visit(self):
        self.driver.get("https://www.google.com/maps")
        sleep(5)
        return self

    def update_lat_lng(self):
        print("[LatLngUpdate] 開始更新經緯度...")

        # 建立欄位
        for col in ["緯度", "經度"]:
            if col not in self.df.columns:
                self.df[col] = np.nan

        # 僅處理緯經度缺失者
        miss_mask = self.df["緯度"].isna() | self.df["經度"].isna()
        target_df = self.df.loc[miss_mask].copy()
        print(f" 共有 {len(target_df)} 筆需要補經緯度")

        #Step 1：向量化比對補值
        if not self.main_data.empty:
            ref = (
                self.main_data.dropna(subset=["緯度", "經度"])
                .drop_duplicates(subset=["土地位置建物門牌"], keep="last")
            )
            lat_map = dict(zip(ref["土地位置建物門牌"], ref["緯度"]))
            lng_map = dict(zip(ref["土地位置建物門牌"], ref["經度"]))

            before = self.df["緯度"].notna().sum()
            self.df["緯度"] = self.df["緯度"].fillna(self.df["土地位置建物門牌"].map(lat_map))
            self.df["經度"] = self.df["經度"].fillna(self.df["土地位置建物門牌"].map(lng_map))
            after = self.df["緯度"].notna().sum()
            print(f"向量化比對補上 {after - before:,} 筆經緯度")

        # Google Maps 轉經緯度
        still_missing = self.df[self.df["緯度"].isna() | self.df["經度"].isna()]
        print(f" 仍有 {len(still_missing)} 筆缺少經緯度")

        if len(still_missing) == 0:
            print("[LatLngUpdate] 無需爬蟲，自動關閉瀏覽器。")
            return self.quit()

        #  測試模式：只爬 50 筆
        if self.test_mode:
            print(" 測試：僅爬前 50 筆，其他資料將刪除")
            still_missing = still_missing.head(50)
        else:
            print(" 一般：對所有缺值資料執行爬蟲")

        for idx, row in still_missing.iterrows():
            addr = row.get("土地位置建物門牌", "")
            if not addr:
                continue
            try:
                txtInput = self.driver.find_element(
                    By.CSS_SELECTOR, "input.fontBodyMedium.searchboxinput.xiQnY"
                )
                txtInput.clear()
                sleep(0.7)
                txtInput.send_keys(addr)
                sleep(0.7)
                txtInput.send_keys(Keys.ENTER)
                sleep(1.5)

                url = self.driver.current_url
                lat, lng = self._extract_coordinates_from_url(url)
                if lat and lng:
                    self.df.loc[idx, ["緯度", "經度"]] = lat, lng
                    print(f" {addr} → 緯度:{lat}, 經度:{lng}")
                else:
                    print(f" {addr} → 無法取得經緯度")
            except Exception as e:
                print(f" 搜尋失敗：{addr}，錯誤：{e}")

        # 清理未爬資料
        if self.test_mode:
            print("刪除未爬取經緯度的其他資料")
            self.df = self.df.loc[still_missing.index].reset_index(drop=True)
            print(f" 留前 {len(self.df)} 筆測試資料")

        print("[LatLngUpdate] 經緯度更新完成。")
        return self.quit()

    def quit(self):
        try:
            self.driver.quit()
            print("[LatLngUpdate] 瀏覽器已關閉。")
        except Exception as e:
            print(f" 關閉瀏覽器時發生錯誤：{e}")
        return self


def main():
    MASTER_DATA_PATH = os.path.join(PROJECT_ROOT, "cleaning_house_rawdata", "cleaning_main_data.csv")
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    lat_lng_update = LatLngUpdate(df,main_data_path=MASTER_DATA_PATH)
    lat_lng_update = lat_lng_update.visit()
    lat_lng_update = lat_lng_update.update_lat_lng()
    lat_lng_update = lat_lng_update.quit()
    df = lat_lng_update.df

    cols = ["緯度","經度"]  
    print(lat_lng_update.df[cols].head(5))

    # for col in cols:
    #     print(f"\n【{col}】種類：")
    #     print(lat_lng_update.df[col].dropna().unique())

if __name__ == "__main__":
    main()
