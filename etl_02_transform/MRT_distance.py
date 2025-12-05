import os
import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree


class MrtDistance:

    def __init__(self, df: pd.DataFrame, mrt_path: str = "./mrt_location.csv"):
        self.df = df
        self.mrt_path = mrt_path
        self.mrt_df = pd.read_csv(mrt_path, encoding="big5")

    def calculate_distance_to_mrt(self): #使用 Haversine BallTree 計算最近捷運距離
        print("[MrtDistance] 開始計算捷運距離...")

        # 確認欄位存在 
        if "緯度" not in self.df.columns or "經度" not in self.df.columns:
            raise ValueError(" 缺少必要欄位：緯度 / 經度")

        #  轉換為數值型態，避免字串導致 np.deg2rad 錯誤 
        self.df["緯度"] = pd.to_numeric(self.df["緯度"], errors="coerce")
        self.df["經度"] = pd.to_numeric(self.df["經度"], errors="coerce")
        self.df = self.df.dropna(subset=["緯度", "經度"])

        self.mrt_df["緯度"] = pd.to_numeric(self.mrt_df["緯度"], errors="coerce")
        self.mrt_df["經度"] = pd.to_numeric(self.mrt_df["經度"], errors="coerce")
        self.mrt_df = self.mrt_df.dropna(subset=["緯度", "經度"])

        # 將經緯度轉成弧度 
        house_coords_rad = np.deg2rad(self.df[["緯度", "經度"]].to_numpy())
        mrt_coords_rad = np.deg2rad(self.mrt_df[["緯度", "經度"]].to_numpy())

        # 建立 BallTree（Haversine metric
        tree = BallTree(mrt_coords_rad, metric="haversine")

        # 查詢最近捷運站
        distances_rad, indices = tree.query(house_coords_rad, k=1)

        # 轉換為公里
        earth_radius_km = 6371
        distances_km = distances_rad[:, 0] * earth_radius_km

        # 加入欄位
        self.df["捷運距離(km)"] = distances_km.round(2)

        print("[MrtDistance] 捷運距離計算完成。")
        return self


def main():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    mrtDistance = MrtDistance(df, mrt_path=r"C:\sideProject\mrt_location.csv")
    mrtDistance = mrtDistance.calculate_distance_to_mrt()
    df = mrtDistance.df

    cols = ["捷運距離(km)"]  
    print(mrtDistance.df[cols].head(5))