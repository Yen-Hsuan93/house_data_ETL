import os
import pandas as pd

class DateHouseAge:
    """處理:日期、屋齡、預售屋"""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def parse_dates(self):
        """將民國年月日拆成年/月/日（西元）"""
        def parse_date(date_str):
            try:
                s = str(date_str)
                if len(s) == 7 and s.isdigit():
                    y, m, d = int(s[:3]) + 1911, int(s[3:5]), int(s[5:7])
                elif len(s) == 6 and s.isdigit():
                    y, m, d = int(s[:2]) + 1911, int(s[2:4]), int(s[4:6])
                else:
                    return None, None, None
                if not (1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31):
                    return None, None, None
                return y, m, d
            except Exception:
                return None, None, None

        self.df[["交易年", "交易月", "交易日"]] = self.df["交易年月日"].astype(str).apply(
            lambda x: pd.Series(parse_date(x))
        )
        self.df[["建築年", "建築月", "建築日"]] = self.df["建築完成年月"].astype(str).apply(
            lambda x: pd.Series(parse_date(x))
        )
        return self

    def calculate_house_age(self):
        """屋齡與預售屋欄位"""
        # datetime 轉換
        self.df["交易日期"] = pd.to_datetime(
            dict(year=self.df["交易年"], month=self.df["交易月"], day=self.df["交易日"]),
            errors="coerce"
        )
        self.df["建築日期"] = pd.to_datetime(
            dict(year=self.df["建築年"], month=self.df["建築月"], day=self.df["建築日"]),
            errors="coerce"
        )

        # 屋齡（年）
        self.df["屋齡"] = (self.df["交易日期"] - self.df["建築日期"]).dt.days / 365
        self.df["屋齡"] = self.df["屋齡"].round(1)

        # 預售屋：-5 < 屋齡 < 0
        # 這裡需要調整邏輯，避免引用被註解掉的 -5 條件
        is_presale = (self.df["屋齡"] < 0)
        self.df["預售屋"] = is_presale.astype(int)

        # 負屋齡歸零
        self.df.loc[self.df["屋齡"] < 0, "屋齡"] = 0

        # 清理暫存欄
        self.df = self.df.drop(columns=["交易日期", "建築日期"])
        return self
    
    def drop_abnormal_houseage(self):
        # 移除缺屋齡
        self.df['屋齡'] = self.df['屋齡'].fillna(-10)
        # -5 > 屋齡 清除         
        self.df = self.df.loc[self.df["屋齡"] <= -5].copy() 

        return self



def main():

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    print(f"[Info] 讀取檔案路徑: {input_path}")
    
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    house_age = DateHouseAge(df)
    house_age = house_age.parse_dates()
    house_age = house_age.calculate_house_age()

    df = house_age.df
    
    cols = ["屋齡", "交易年", "交易月", "交易日"]  
    # print(house_age.df[cols].head(5))

    for col in cols:
        print(f"\n【{col}】種類：")
        print(house_age.df[col].dropna().unique())

if __name__ == "__main__":
    main()
