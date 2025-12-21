import os
import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree

class MrtDistance:

    def __init__(self, df: pd.DataFrame, mrt_path: str = "./mrt_location.csv"):
        self.df = df
        self.mrt_path = mrt_path

        try:
            self.mrt_df = pd.read_csv(mrt_path, encoding="big5")
        except:
            self.mrt_df = pd.read_csv(mrt_path, encoding="utf-8")

    def calculate_distance_to_mrt(self): 

        print("[MrtDistance] 1. 開始計算捷運距離...")


        if "緯度" not in self.df.columns or "經度" not in self.df.columns:
            raise ValueError("缺少必要欄位：緯度 / 經度")


        self.df["緯度"] = pd.to_numeric(self.df["緯度"], errors="coerce")
        self.df["經度"] = pd.to_numeric(self.df["經度"], errors="coerce")
        

        self.df = self.df.dropna(subset=["緯度", "經度"])

        self.mrt_df["緯度"] = pd.to_numeric(self.mrt_df["緯度"], errors="coerce")
        self.mrt_df["經度"] = pd.to_numeric(self.mrt_df["經度"], errors="coerce")
        self.mrt_df = self.mrt_df.dropna(subset=["緯度", "經度"])


        house_coords_rad = np.deg2rad(self.df[["緯度", "經度"]].to_numpy())
        mrt_coords_rad = np.deg2rad(self.mrt_df[["緯度", "經度"]].to_numpy())


        tree = BallTree(mrt_coords_rad, metric="haversine")


        distances_rad, indices = tree.query(house_coords_rad, k=1)


        earth_radius_km = 6371
        distances_km = distances_rad[:, 0] * earth_radius_km


        self.df["捷運距離(km)"] = distances_km.round(2)

        print("   - 距離計算完成。")
        return self
    
    def process_mrt_name_and_grade(self):

        print("[MrtDistance] 2. 正在比對最近捷運站名稱與評分...")
        

        if '捷運距離(km)' not in self.df.columns:
            print("尚未計算距離，正在自動補算...")
            self.calculate_distance_to_mrt()
        
        # 檢查座標欄位
        valid_rows = self.df.dropna(subset=["緯度", "經度"]).index
        if len(valid_rows) == 0:
            return self


        house_rad = np.deg2rad(self.df.loc[valid_rows, ['緯度', '經度']].to_numpy())
        mrt_rad = np.deg2rad(self.mrt_df[['緯度', '經度']].to_numpy())

        tree = BallTree(mrt_rad, metric='haversine')
        _, indices = tree.query(house_rad, k=1)

        # 抓出站名
        mrt_name_col = "出入口名稱" 
        if mrt_name_col not in self.mrt_df.columns:
            mrt_name_col = self.mrt_df.columns[0]
            
        self.df.loc[valid_rows, '最近捷運站'] = self.mrt_df.iloc[indices[:, 0]][mrt_name_col].values


        bins = [0, 0.3, 0.6, 1.0, 1.5, 3.0, float('inf')]
        labels = [5, 4, 3, 2, 1, 0]

        self.df["捷運便利等級"] = pd.cut(self.df["捷運距離(km)"], bins=bins, labels=labels, include_lowest=True)
        self.df["捷運便利等級"] = self.df["捷運便利等級"].fillna(0).astype(int)


        self.df.loc[self.df["捷運便利等級"] == 0, "最近捷運站"] = "尚無捷運區"

        print("處理完成！已將無捷運區域標記更新。")
        return self


def main():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    

    if not os.path.exists(input_path):
        print(f"找不到檔案: {input_path}")
        return

    df = pd.read_csv(input_path, encoding="utf-8-sig")


    mrtDistance = MrtDistance(df, mrt_path=r"C:\sideProject\mrt_location.csv")
    mrtDistance = mrtDistance.calculate_distance_to_mrt()
    mrtDistance = mrtDistance.process_mrt_name_and_grade()
    

    cols = ["最近捷運站", "捷運便利等級", "捷運距離(km)"]  
    print(mrtDistance.df[cols].head(10))

if __name__ == "__main__":
    main()