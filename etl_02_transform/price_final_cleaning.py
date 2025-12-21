import os
import pandas as pd
import numpy as np

class PriceFinalCleaning:
    #每坪價格與最終欄位清理
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
    def price_ping(self):
        num = self.df["總價元"] - self.df["車位總價元"]
        den = self.df["建物總坪數"] - self.df["車位坪數"]
        mask = (num > 0) & (den > 0)
        self.df.loc[mask, "每坪價格"] = (num[mask] / den[mask] / 10000).round(2)
        return self

    def drop_missing_core_fields(self):
        cols = ["總樓層數", "每坪價格", "移轉樓層總數"]
        self.df = self.df.dropna(subset=[col for col in cols if col in self.df.columns])
        return self
    
    

def main():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    price_clean = PriceFinalCleaning(df)
    price_clean = price_clean.price_ping()
    price_clean = price_clean.drop_missing_core_fields()
    df = price_clean.df

    cols = ["總價元", "建物每坪價格"]  
    print(price_clean.df[cols].head(5))

    for col in cols:
        print(f"\n【{col}】種類：")
        print(price_clean.df[col].dropna().unique())

if __name__ == "__main__":
    main()
