import os
import pandas as pd
import re # <-- 新增：需要導入 re 模組

class FilterBasic:
    """基本篩選與欄位統一處理"""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    # 移除特殊字元/亂碼的函式

    def remove_pua_chars_from_address(self, address_column: str = "土地位置建物門牌"):
        before_len = len(self.df)
        
        if address_column in self.df.columns:
            PUA_CHAR_PATTERN = r'[\uE000-\uF8FF]'
            rows_to_drop_mask = self.df[address_column].astype(str).str.contains(
                PUA_CHAR_PATTERN, 
                regex=True,
                na=False
            )

            # 刪除資料列
            self.df = self.df[~rows_to_drop_mask].copy()

            num_dropped = before_len - len(self.df)
            if num_dropped > 0:
                print(f"[FilterBasic]  地址清理: 刪除 {num_dropped} 筆包含 PUA 亂碼的資料。")
            else:
                print("[FilterBasic]  地址清理: 未發現需要刪除的 PUA 亂碼。")
        else:
            print(f"[FilterBasic]  地址欄位 '{address_column}' 不存在，跳過清理。")
            
        return self

    def drop_duplicates_by_id(self):
        """刪除重複編號"""
        if "編號" in self.df.columns:
            before = len(self.df)
            self.df = self.df.drop_duplicates(subset=["編號"], keep="first")
            print(f"[FilterBasic] 移除重複筆數: {before - len(self.df)}")
        return self
    
    def unify_columns(self):
        print("[FilterBasic] 正在統一欄位格式...")

        col_new = "車位移轉總面積平方公尺"
        col_old = "車位移轉總面積(平方公尺)"

        # 1. 先確保兩個欄位若存在，都轉為數字 (才能運算)
        for col in [col_new, col_old, "建物移轉總面積平方公尺"]:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

        # 2. 處理合併邏輯
        if col_old in self.df.columns:
            if col_new in self.df.columns:
                # 狀況 A: 兩個欄位都有 -> 合併資料 (用舊補新)
                self.df[col_new] = self.df[col_new].combine_first(self.df[col_old])
                self.df.drop(columns=[col_old], inplace=True)
                print(f"   -> 合併 [{col_old}] 至 [{col_new}]")
            else:
                # 狀況 B: 只有舊欄位 -> 直接改名
                self.df.rename(columns={col_old: col_new}, inplace=True)
                print(f"   -> 改名 [{col_old}] 為 [{col_new}]")

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
        
    def cleaning_house_type(self,target_types =('住宅大樓', '華廈', '公寓', '透天厝','套房') ):
        self.df['建物型態'] = self.df['建物型態'].astype(str).str.split('(').str[0]
        self.df = self.df[self.df['建物型態'].isin(target_types)].copy()
        return self

    def add_city_from_source(self):
        mapping = {"F": "新北市", "H": "桃園市", "A": "臺北市"}
        if "來源檔名" in self.df.columns:
            self.df["標的縣市"] = self.df["來源檔名"].astype(str).str[0].map(mapping)
        return self
        
def main():

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    
    # 建議先處理編碼問題，再讀取 CSV
    # 這是為了避免在讀取檔案時，特殊字元就變成無法修復的亂碼
    try:
        # 假設您的 CSV 是 UTF-8 編碼 (推薦)
        df = pd.read_csv(input_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        # 如果不是 UTF-8，則嘗試 CP950
        df = pd.read_csv(input_path, encoding="cp950")


    filter_basic = FilterBasic(df)
    filter_basic = filter_basic.drop_duplicates_by_id()
    filter_basic = filter_basic.unify_columns()
    
    # 🌟 在其他篩選之前先移除亂碼地址
    filter_basic = filter_basic.remove_pua_chars_from_address()
    
    filter_basic = filter_basic.filter_out_transaction_targets()
    filter_basic = filter_basic.keep_residential_usage()
    filter_basic = filter_basic.keep_urban_zone_residential()
    filter_basic = filter_basic.filter_out_non_urban_zones()
    filter_basic = filter_basic.remove_notes_with_keywords()
    filter_basic = filter_basic.add_city_from_source()

    df = filter_basic.df
    
    cols = ["交易標的", "主要用途", "都市土地使用分區", "來源檔名"] 
    
    for col in cols:
        print(f"\n【{col}】種類：")
        # 確保在印出時能處理 NaN 值，避免錯誤
        print(filter_basic.df[col].dropna().unique())

if __name__ == "__main__":
    main()