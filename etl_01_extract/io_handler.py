import pandas as pd
from typing import Optional
import os

class IOHandler:
    def __init__(self, 
                 input_path : Optional[str] = None,
                 output_path : Optional[str] = None,
                 encoding : str = "utf-8"
                 ):
        self.input_path = input_path
        self.output_path = output_path
        self.encoding = encoding
    def load(self):
        if not self.input_path:
            raise ValueError(f"{self.input_path}:路徑未提供")
        try:
            df = pd.read_csv(self.input_path, encoding =self.encoding, sep = ",")
            print("使用utf-8儲存")
            return df
        except UnicodeDecodeError:
            df = pd.read_csv(self.input_path, encoding = "big5", sep = "\t")
            print("使用big5儲存")
            return df
        
    def save(self,df):
        if not self.output_path:
            raise ValueError("未提供輸出路徑")
        df.to_csv(self.output_path,encoding = self.encoding, index = False, sep = ",")
        print(f"已utf-8儲存到{self.output_path}")
    


def main():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_csv_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    output_csv_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_cleaned.csv")

    io = IOHandler(
        input_path=input_csv_path,   
        output_path=output_csv_path  
    )
    
    df = io.load()
    io.save(df)

if __name__ == "__main__":
    main()