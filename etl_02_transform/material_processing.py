import os
import pandas as pd
import numpy as np

class MaterialProcessing:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def impute_main_material(self):
        if '主要建材' not in self.df.columns or '建物型態' not in self.df.columns:
            print(" 缺少欄位，跳過 impute_main_material")
            return self

        mode_by_type = self.df.groupby('建物型態')['主要建材'].apply(
            lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan
        )

        self.df['主要建材'] = self.df['主要建材'].fillna(self.df['建物型態'].map(mode_by_type))
        print("[MainMaterialProcessing] 完成主要建材補值")
        return self
    

def main():

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    material_process = MaterialProcessing(df)
    material_process = material_process.impute_main_material()
    df = material_process.df
    
    cols = ["主要建材"]  
    # print(material_process.df[cols].head(5))

    for col in cols:
        print(f"\n【{col}】種類：")
        print(material_process.df[col].dropna().unique())

if __name__ == "__main__":
    main()
