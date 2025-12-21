import os
import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree

class MrtDistance:

    def __init__(self, df: pd.DataFrame, mrt_path: str = "./mrt_location.csv"):
        self.df = df
        self.mrt_path = mrt_path
        # 自動偵測編碼讀取捷運檔
        try:
            self.mrt_df = pd.read_csv(mrt_path, encoding="big5")
        except:
            self.mrt_df = pd.read_csv(mrt_path, encoding="utf-8")

    def calculate_distance_to_mrt(self): 
        """使用 Haversine BallTree 計算最近捷運距離"""
        print("[MrtDistance] 1. 開始計算捷運距離...")

        # 確認欄位存在 
        if "緯度" not in self.df.columns or "經度" not in self.df.columns:
            raise ValueError("缺少必要欄位：緯度 / 經度")

        # 轉換為數值型態
        self.df["緯度"] = pd.to_numeric(self.df["緯度"], errors="coerce")
        self.df["經度"] = pd.to_numeric(self.df["經度"], errors="coerce")
        
        # 建議：這邊若 dropna 會把沒座標的資料整筆刪掉，若確定資料乾淨則無妨
        self.df = self.df.dropna(subset=["緯度", "經度"])

        self.mrt_df["緯度"] = pd.to_numeric(self.mrt_df["緯度"], errors="coerce")
        self.mrt_df["經度"] = pd.to_numeric(self.mrt_df["經度"], errors="coerce")
        self.mrt_df = self.mrt_df.dropna(subset=["緯度", "經度"])

        # 將經緯度轉成弧度 
        house_coords_rad = np.deg2rad(self.df[["緯度", "經度"]].to_numpy())
        mrt_coords_rad = np.deg2rad(self.mrt_df[["緯度", "經度"]].to_numpy())

        # 建立 BallTree
        tree = BallTree(mrt_coords_rad, metric="haversine")

        # 查詢最近捷運站 (k=1)
        distances_rad, indices = tree.query(house_coords_rad, k=1)

        # 轉換為公里 (地球半徑 6371km)
        earth_radius_km = 6371
        distances_km = distances_rad[:, 0] * earth_radius_km

        # 加入欄位
        self.df["捷運距離(km)"] = distances_km.round(2)

        print("   - 距離計算完成。")
        return self
    
    def process_mrt_name_and_grade(self):
        """取得最近捷運站名稱並進行評分 (直接修改欄位)"""
        print("[MrtDistance] 2. 正在比對最近捷運站名稱與評分...")
        
        # 1. 檢查是否已計算距離
        if '捷運距離(km)' not in self.df.columns:
            print("尚未計算距離，正在自動補算...")
            self.calculate_distance_to_mrt()
        
        # 檢查座標欄位
        valid_rows = self.df.dropna(subset=["緯度", "經度"]).index
        if len(valid_rows) == 0:
            return self

        # ==========================================
        # 步驟 1: 找出「最近捷運站」的名稱
        # ==========================================
        house_rad = np.deg2rad(self.df.loc[valid_rows, ['緯度', '經度']].to_numpy())
        mrt_rad = np.deg2rad(self.mrt_df[['緯度', '經度']].to_numpy())

        tree = BallTree(mrt_rad, metric='haversine')
        _, indices = tree.query(house_rad, k=1)

        # 抓出站名
        mrt_name_col = "出入口名稱" 
        if mrt_name_col not in self.mrt_df.columns:
            mrt_name_col = self.mrt_df.columns[0]
            
        self.df.loc[valid_rows, '最近捷運站'] = self.mrt_df.iloc[indices[:, 0]][mrt_name_col].values

        # ==========================================
        # 步驟 2: 執行 5-0 分級 (維持原設定)
        # ==========================================
        # Bins 維持您原本設定 [0, 0.3, 0.6, 1.0, 1.5, 3.0, inf]
        bins = [0, 0.3, 0.6, 1.0, 1.5, 3.0, float('inf')]
        labels = [5, 4, 3, 2, 1, 0]

        self.df["捷運便利等級"] = pd.cut(self.df["捷運距離(km)"], bins=bins, labels=labels, include_lowest=True)
        self.df["捷運便利等級"] = self.df["捷運便利等級"].fillna(0).astype(int)

        # ==========================================
        # 步驟 3: 直接修改原本欄位
        # ==========================================
        # 只要等級是 0，原本抓到的捷運站名就改成 "尚無捷運區"
        self.df.loc[self.df["捷運便利等級"] == 0, "最近捷運站"] = "尚無捷運區"

        print("處理完成！已將無捷運區域標記更新。")
        return self


def main():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    
    # 這裡加入簡單的檢查，避免路徑錯誤報錯
    if not os.path.exists(input_path):
        print(f"找不到檔案: {input_path}")
        return

    df = pd.read_csv(input_path, encoding="utf-8-sig")

    # 請確認捷運檔路徑正確
    mrtDistance = MrtDistance(df, mrt_path=r"C:\sideProject\mrt_location.csv")
    mrtDistance = mrtDistance.calculate_distance_to_mrt()
    mrtDistance = mrtDistance.process_mrt_name_and_grade()
    
    # 顯示結果驗證
    cols = ["最近捷運站", "捷運便利等級", "捷運距離(km)"]  
    print(mrtDistance.df[cols].head(10))

if __name__ == "__main__":
    main()