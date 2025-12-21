import os
import re
import numpy as np
import pandas as pd

class FloorProcessing:

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def total_floor(self): 
        cn = {'零':0,'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9}
        def _convert(value):
            try:
                s = str(value).strip().replace('層', '')
                if s.isdigit():
                    num = int(s)
                    return np.nan if num == 0 else num
                if s == '十':
                    return 10
                if s.startswith('十'):
                    return 10 + cn.get(s[1:], 0)
                if s.endswith('十'):
                    return cn.get(s[0], 0) * 10
                m = re.match(r'^([一二三四五六七八九])十([一二三四五六七八九])$', s)
                if m:
                    return cn[m.group(1)] * 10 + cn[m.group(2)]
                if s in cn:
                    num = cn[s]
                    return np.nan if num == 0 else num
            except:
                pass
            return np.nan

        if '總樓層數' in self.df.columns:
            self.df['總樓層數'] = self.df['總樓層數'].apply(_convert).astype('Int64')
        return self

    def count_transfer_floors(self):
        
        def _count(text): 
            if pd.isnull(text): return pd.NA
            t = str(text)
            if '全' in t: return 0
            basement = re.findall(r'地下([一二三四五六七八九十]{1,5})層', t)
            normal   = re.findall(r'(?<!地下)([一二三四五六七八九十]{1,5})層', t)
            total = len(basement) + len(normal)
            return total if total > 0 else pd.NA

        if '移轉層次' in self.df.columns: 
            self.df['移轉樓層總數'] = self.df['移轉層次'].apply(_count).astype('Int64')
        return self

    def extract_highest_floor(self):
        
        def cn2int(s: str) -> int: 
            cn = {'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10}
            if s.isdigit(): return int(s)
            if s in cn: return cn[s]
            if '十' in s:
                left, right = s.split('十') if '十' in s else (s, '')
                if left == '':
                    return 10 + (cn.get(right, 0) if right != '' else 0)
                else:
                    return cn.get(left, 0) * 10 + (cn.get(right, 0) if right != '' else 0)
            return 0

        def _highest(text):
            if pd.isnull(text): return pd.NA
            t = str(text)
            if '全' in t: return 0
            basement = [-cn2int(x) for x in re.findall(r'地下([一二三四五六七八九十0-9]+)層', t)]
            above    = [ cn2int(x) for x in re.findall(r'(?<!地下)([一二三四五六七八九十0-9]+)層', t)]
            all_lvls = basement + above
            return max(all_lvls) if all_lvls else pd.NA

        if '移轉層次' in self.df.columns:
            self.df['最高交易樓層'] = self.df['移轉層次'].apply(_highest)
            self.df = self.df[(self.df['最高交易樓層'].isna()) | (self.df['最高交易樓層'] >= 0)]
            self.df['最高交易樓層'] = self.df['最高交易樓層'].astype('Int64')
        return self


def main():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    floor_Process = FloorProcessing(df)
    floor_Process = floor_Process.total_floor()
    floor_Process = floor_Process.count_transfer_floors()
    floor_Process = floor_Process.extract_highest_floor()
    df = floor_Process.df

    cols = ["總樓層數", "移轉樓層總數", "最高交易樓層"]  
    # print(floor_Process.df[cols].head(5))

    for col in cols:
        print(f"\n【{col}】種類：")
        print(floor_Process.df[col].dropna().unique())

if __name__ == "__main__":
    main()