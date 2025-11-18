import pandas as pd

class FilterBasic:
    """基本篩選與欄位統一處理"""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def drop_duplicates_by_id(self):
        """刪除重複編號"""
        if "編號" in self.df.columns:
            before = len(self.df)
            self.df = self.df.drop_duplicates(subset=["編號"], keep="first")
            print(f"[FilterBasic] 移除重複筆數: {before - len(self.df)}")
        return self
    
    def unify_columns(self):

        # 轉換為數值
        num_cols = ["建物移轉總面積平方公尺", "車位移轉總面積平方公尺"]
        for col in num_cols:
            # 使用 .loc 明確指派
            self.df.loc[:, col] = pd.to_numeric(self.df[col], errors="coerce")

        # 例：合併相似欄位
        col1, col2 = "非都市土地使用分區", "非都市土地使用編定"
        if col1 in self.df.columns and col2 in self.df.columns:
            self.df.loc[:, col1] = self.df[col1].combine_first(self.df[col2])

            self.df = self.df.drop(columns=[col2])

        print("[FilterBasic] 統一車位面積欄位")
        return self


    # 過濾欄位
    def filter_out_transaction_targets(self, banned=("土地", "車位")):
        self.df = self.df[~self.df["交易標的"].isin(list(banned))]
        return self

    def keep_residential_usage(self, allowed=("住家用", "國民住宅", "集合住宅")):
        self.df = self.df[self.df["主要用途"].isin(list(allowed))]
        return self

    def keep_urban_zone_residential(self, allowed=("住",)):
        self.df = self.df[self.df["都市土地使用分區"].isin(list(allowed))]
        return self

    def filter_out_non_urban_zones(self, exclude=("一般農業區", "鄉村區", "山坡地保育區", "特定農業區")):
        col = "非都市土地使用分區"
        if col in self.df.columns:
            self.df = self.df[~self.df[col].isin(list(exclude))]
        return self

    def remove_notes_with_keywords(self, keywords=("親友", "親戚", "朋友", "員工", "特殊")):
        if "備註" in self.df.columns:
            pattern = "|".join(map(str, keywords))
            self.df = self.df[~self.df["備註"].astype(str).str.contains(pattern, na=False)]
        return self

    def add_city_from_source(self):
        mapping = {"F": "新北市", "H": "桃園市", "A": "臺北市"}
        if "來源檔名" in self.df.columns:
            self.df["標的縣市"] = self.df["來源檔名"].astype(str).str[0].map(mapping)
        return self
def main():

    input_path = r"C:\sideProject\main_house_rawdata\merged_rawdata.csv"
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    filter_basic = FilterBasic(df)
    filter_basic = filter_basic.drop_duplicates_by_id()
    filter_basic = filter_basic.unify_columns()
    filter_basic = filter_basic.filter_out_transaction_targets()
    filter_basic = filter_basic.keep_residential_usage()
    filter_basic = filter_basic.keep_urban_zone_residential()
    filter_basic = filter_basic.filter_out_non_urban_zones()
    filter_basic = filter_basic.remove_notes_with_keywords()
    filter_basic = filter_basic.add_city_from_source()

    df = filter_basic.df
    
    cols = ["交易標的", "主要用途", "都市土地使用分區", "來源檔名"]  
    # print(filter_basic.df[cols].head(5))

    for col in cols:
        print(f"\n【{col}】種類：")
        print(filter_basic.df[col].dropna().unique())

if __name__ == "__main__":
    main()