import os
import re
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor


class ParkingProcessing:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def _to_numeric(self, col: str): #安全轉數值；若不存在則建空欄
        if col not in self.df.columns:
            self.df[col] = np.nan
        self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
        return self

    # 處理車位欄位
    def process_parking(self):
        print("[ParkingProcessing] 開始處理車位欄位...")

        # 數值欄位轉型
        for col in ["建物移轉總面積平方公尺", "車位移轉總面積平方公尺", "總價元", "車位總價元"]:
            self._to_numeric(col)

        # 建物總坪數
        self.df["建物總坪數"] = (
            self.df["建物移轉總面積平方公尺"] * 0.3025
        ).round(2)


        # 車位數從交易筆棟數中擷取
        def extract_parking_num(text):
            text = str(text) if pd.notna(text) else ""
            m = re.search(r"車位(\d+)", text)
            return int(m.group(1)) if m else 0

        if "交易筆棟數" in self.df.columns:
            self.df["車位數"] = (
                self.df["交易筆棟數"].apply(extract_parking_num).astype("Int64")
            )
        else:
            self.df["車位數"] = pd.Series(0, index=self.df.index, dtype="Int64")

        # 統一數值型態
        self.df["車位坪數"] = (
            self.df["車位移轉總面積平方公尺"] * 0.3025
        ).round(2)
        self.df["車位總價元"] = pd.to_numeric(self.df["車位總價元"], errors="coerce")

        # 若缺欄位則補
        if "車位類別" not in self.df.columns:
            self.df["車位類別"] = np.nan

        # 清理異常值
        self.df.loc[self.df["車位總價元"] == 0, "車位總價元"] = np.nan
        self.df.loc[self.df["車位坪數"] == 0, "車位坪數"] = np.nan

        print("[ParkingProcessing] 車位基本欄位處理完成。")
        return self

    # 車位類別補值
    def impute_parking_type(self):
        #依建物型態眾數補車位類別（排除無車位)
        if "建物型態" not in self.df.columns or "車位類別" not in self.df.columns:
            return self

        tmp = self.df.copy()
        tmp.loc[tmp["車位類別"] == "無車位", "車位類別"] = np.nan

        # 用 transform，直接在原 df 對應補值，不產生 index 錯位
        mode_by_type = tmp.groupby("建物型態")["車位類別"].transform(
            lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else np.nan
        )

        # 補上缺值
        mask_need = self.df["車位類別"].isna()
        self.df.loc[mask_need, "車位類別"] = mode_by_type[mask_need].values

        print("[ParkingProcessing] 車位類別補值完成。")
        return self


    def impute_parking_price_rf(self):#隨機森林補『車位總價元』
        for col in ["總價元", "建物總坪數", "車位數", "車位總價元"]:
            self._to_numeric(col)

        mask_has = self.df["車位類別"] != "無車位"
        mask_train = mask_has & self.df["車位總價元"].notna()
        mask_pred = mask_has & self.df["車位總價元"].isna()

        if mask_pred.sum() == 0 or mask_train.sum() < 10:
            return self

        feats = ["總價元", "建物總坪數", "車位數"]
        X_train = self.df.loc[mask_train, feats].copy()
        y_train = self.df.loc[mask_train, "車位總價元"].copy()
        X_test = self.df.loc[mask_pred, feats].copy()

        ok = (~X_train.isna().any(axis=1)) & (~y_train.isna())
        X_train, y_train = X_train[ok].copy(), y_train[ok].copy()

        if len(X_train) < 10:
            return self

        med = X_train.median(numeric_only=True)
        X_test = X_test.fillna(med)

        model = RandomForestRegressor(
            n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
        )
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        self.df.loc[X_test.index, "車位總價元"] = np.round(pred)
        print("[ParkingProcessing] 車位總價補值完成。")
        return self

     #車位每坪價 + 清理異常筆數
    def calculate_parking_price_per_ping(self):
        """計算車位每坪價格（萬元/坪）並移除異常筆數"""
        for col in ["車位總價元", "車位坪數"]:
            self._to_numeric(col)

        valid = (
            self.df["車位總價元"].notna()
            & self.df["車位坪數"].notna()
            & (self.df["車位坪數"] > 0)
        )
        self.df["車位每坪價格"] = np.where(
            valid,
            (self.df["車位總價元"] / self.df["車位坪數"] / 10000).round(2),
            np.nan,
        )
        print("[ParkingProcessing] 車位每坪價格計算完成。")

        #  統一：車位類別=無車位 → 車位數=0
        mask_no_parking = self.df["車位類別"] == "無車位"
        self.df.loc[mask_no_parking, "車位數"] = 0

        #移除「無車位但車位數≠0」異常筆數
        #無車位但車位數 ≠ 0 → 刪除
        invalid1 = self.df[
            (self.df["車位類別"] == "無車位") & (self.df["車位數"] != 0)
        ].index
        if len(invalid1) > 0:
            print(f"異常（無車位但車位數≠0）刪除 {len(invalid1)} 筆")
            self.df.drop(index=invalid1, inplace=True)

        #車位數 = 0 但類別 ≠ 無車位 → 刪除
        invalid2 = self.df[
            (self.df["車位數"] == 0) & (self.df["車位類別"] != "無車位")
        ].index
        if len(invalid2) > 0:
            print(f"異常（車位數=0但類別≠無車位）刪除 {len(invalid2)} 筆")
            self.df.drop(index=invalid2, inplace=True)

        return self
    

def main():

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    parking_Process = ParkingProcessing(df)
    parking_Process = parking_Process.process_parking()
    parking_Process = parking_Process.impute_parking_type()
    parking_Process = parking_Process.impute_parking_price_rf()
    parking_Process = parking_Process.calculate_parking_price_per_ping()
    df = parking_Process.df
    
    cols = ["車位數", "車位類別"]  
    # print(parking_Process.df[cols].head(5))

    for col in cols:
        print(f"\n【{col}】種類：")
        print(parking_Process.df[col].dropna().unique())

if __name__ == "__main__":
    main()